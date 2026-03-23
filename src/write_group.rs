// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Write group commit pipeline.
//!
//! Amortizes journal I/O by batching concurrent writes: one thread becomes
//! the "group leader", collects all pending writes from other threads,
//! performs a single journal write + fsync for the entire group.
//!
//! Based on the same principle as `RocksDB`'s write group / `InnoDB`'s
//! group commit. See upstream fjall-rs/fjall#96.

use crate::{
    batch::item::Item as BatchItem,
    journal::{writer::PersistMode, Journal},
    keyspace::InternalKeyspaceId,
    snapshot_tracker::SnapshotTracker,
};
use lsm_tree::{SeqNo, SharedSequenceNumberGenerator, UserKey, UserValue, ValueType};
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};

/// A write operation to be committed to the journal.
pub enum WriteOp {
    /// Single key-value write (insert, remove, `remove_weak`).
    Raw {
        keyspace_id: InternalKeyspaceId,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
    },
    /// Multi-item atomic batch.
    Batch { items: Vec<BatchItem> },
    /// Clear all data in a keyspace.
    Clear { keyspace_id: InternalKeyspaceId },
}

/// Outcome communicated from leader to follower via [`WriteSlot`].
enum WriteOutcome {
    /// Write succeeded — carries assigned seqno and the original op
    /// (returned so the caller can apply it to the memtable without cloning).
    Success { seqno: SeqNo, op: WriteOp },
    /// Write group failed — database is poisoned.
    Failed,
}

/// Per-writer slot for result delivery between leader and follower.
struct WriteSlot {
    state: Mutex<Option<WriteOutcome>>,
    condvar: Condvar,
}

impl WriteSlot {
    fn new() -> Self {
        Self {
            state: Mutex::new(None),
            condvar: Condvar::new(),
        }
    }

    /// Block until the leader delivers a result.
    fn wait(&self) -> crate::Result<(SeqNo, WriteOp)> {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut state = self.state.lock().expect("slot lock poisoned");
        while state.is_none() {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            {
                state = self.condvar.wait(state).expect("slot lock poisoned");
            }
        }
        match state.take() {
            Some(WriteOutcome::Success { seqno, op }) => Ok((seqno, op)),
            Some(WriteOutcome::Failed) | None => Err(crate::Error::Poisoned),
        }
    }

    fn complete(&self, outcome: WriteOutcome) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut state = self.state.lock().expect("slot lock poisoned");
        *state = Some(outcome);
        self.condvar.notify_one();
    }
}

/// A pending write in the group queue.
struct PendingWrite {
    op: WriteOp,
    persist_mode: Option<PersistMode>,
    slot: Arc<WriteSlot>,
}

/// Write group commit pipeline.
///
/// Instead of each writer acquiring the journal mutex, writing, and
/// fsyncing individually, writers submit their operations here. The
/// first thread to arrive becomes the "group leader" and:
///
/// 1. Drains all pending writes from the queue.
/// 2. Acquires the journal mutex **once**.
/// 3. Writes all operations to the journal sequentially.
/// 4. Calls fsync **once** (strongest mode in the group).
/// 5. Releases the journal mutex.
/// 6. Delivers (seqno, op) back to each follower.
///
/// Followers block on their [`WriteSlot`] until notified.
///
/// Result: N concurrent writers → 1 lock acquisition + 1 fsync
/// instead of N locks + N fsyncs.
pub struct WriteGroup {
    inner: Mutex<WriteGroupInner>,
}

struct WriteGroupInner {
    pending: Vec<PendingWrite>,
    leader_active: bool,
}

impl WriteGroup {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(WriteGroupInner {
                pending: Vec::new(),
                leader_active: false,
            }),
        }
    }

    /// Submit a write operation to the group commit pipeline.
    ///
    /// Returns `(seqno, op)` on success — the caller gets the original
    /// [`WriteOp`] back (zero-copy) so it can apply the data to the
    /// memtable without cloning keys/values.
    ///
    /// If this thread becomes the group leader, it writes all pending
    /// operations to the journal in a single batch with one fsync.
    /// Otherwise, the thread blocks until the current leader processes it.
    ///
    /// # Caller contract
    ///
    /// After a successful return, the caller **must** apply the write to the
    /// memtable and then call [`PendingWatermark::applied`] with the returned
    /// seqno. Failing to do so leaves the seqno permanently pending, blocking
    /// the MVCC visible watermark from advancing.
    pub(crate) fn submit(
        &self,
        op: WriteOp,
        persist_mode: Option<PersistMode>,
        journal: &Journal,
        seqno_gen: &SharedSequenceNumberGenerator,
        is_poisoned: &AtomicBool,
        watermark: &PendingWatermark,
    ) -> crate::Result<(SeqNo, WriteOp)> {
        let slot = Arc::new(WriteSlot::new());

        let am_leader = {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let mut inner = self.inner.lock().expect("write group lock poisoned");
            inner.pending.push(PendingWrite {
                op,
                persist_mode,
                slot: Arc::clone(&slot),
            });
            if inner.leader_active {
                false
            } else {
                inner.leader_active = true;
                true
            }
        };

        if am_leader {
            self.run_leader(journal, seqno_gen, is_poisoned, &slot, watermark)
        } else {
            slot.wait()
        }
    }

    /// Leader path: drain queue in a loop, write everything, notify followers.
    ///
    /// The leader holds the journal writer and keeps draining the queue until
    /// no more pending writes remain. This prevents a deadlock where followers
    /// push after the leader's initial drain but before it releases leadership.
    fn run_leader(
        &self,
        journal: &Journal,
        seqno_gen: &SharedSequenceNumberGenerator,
        is_poisoned: &AtomicBool,
        leader_slot: &Arc<WriteSlot>,
        watermark: &PendingWatermark,
    ) -> crate::Result<(SeqNo, WriteOp)> {
        // Acquire journal writer (serialization point with rotate/recovery)
        let mut journal_writer = journal.get_writer();

        // IMPORTANT: Check poisoned flag AFTER acquiring serialization (TOCTOU).
        // Acquire pairs with the Release store on the error paths below.
        if is_poisoned.load(Ordering::Acquire) {
            drop(journal_writer);
            self.drain_fail_and_release();
            return Err(crate::Error::Poisoned);
        }

        let mut leader_result = None;

        // Loop: keep draining the queue until empty. Followers that push
        // while we are processing get picked up in the next iteration,
        // avoiding the deadlock where writes sit in the queue with no leader.
        loop {
            let writes = {
                #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
                let mut inner = self.inner.lock().expect("write group lock poisoned");
                let pending = std::mem::take(&mut inner.pending);
                if pending.is_empty() {
                    // Queue is empty — release leadership and exit
                    inner.leader_active = false;
                    break;
                }
                pending
            };

            // Re-check poisoned on each iteration — another thread (e.g. flush
            // worker via PoisonDart) may have poisoned the DB since we last checked.
            if is_poisoned.load(Ordering::Acquire) {
                Self::fail_group(&writes);
                self.drain_fail_and_release();
                return leader_result.ok_or(crate::Error::Poisoned);
            }

            log::trace!("write group: leader processing {} writes", writes.len());

            // Determine strongest persist mode in this batch
            let group_persist =
                writes
                    .iter()
                    .filter_map(|w| w.persist_mode)
                    .max_by_key(|m| match m {
                        PersistMode::Buffer => 0,
                        PersistMode::SyncData => 1,
                        PersistMode::SyncAll => 2,
                    });

            // Write all operations to journal, allocating seqnos
            let mut seqnos = Vec::with_capacity(writes.len());
            let mut write_error = false;

            for write in &writes {
                let seqno = seqno_gen.next();
                let result = match &write.op {
                    WriteOp::Raw {
                        keyspace_id,
                        key,
                        value,
                        value_type,
                    } => journal_writer.write_raw(*keyspace_id, key, value, *value_type, seqno),
                    WriteOp::Batch { items } => journal_writer.write_batch(items, seqno),
                    WriteOp::Clear { keyspace_id } => {
                        journal_writer.write_clear(*keyspace_id, seqno)
                    }
                };

                if let Err(e) = result {
                    log::error!("journal write failed in group commit: {e:?}");
                    is_poisoned.store(true, Ordering::Release);
                    write_error = true;
                    break;
                }

                seqnos.push(seqno);
            }

            if write_error {
                drop(journal_writer);
                Self::fail_group(&writes);
                self.drain_fail_and_release();
                return leader_result.ok_or(crate::Error::Poisoned);
            }

            // Single persist for this batch
            if let Some(mode) = group_persist {
                if let Err(e) = journal_writer.persist(mode) {
                    log::error!(
                        "persist failed in group commit, FATAL hardware-related failure: {e:?}"
                    );
                    is_poisoned.store(true, Ordering::Release);
                    drop(journal_writer);
                    Self::fail_group(&writes);
                    self.drain_fail_and_release();
                    return leader_result.ok_or(crate::Error::Poisoned);
                }
            }

            // IMPORTANT: register seqnos as pending BEFORE waking followers.
            // This ensures the watermark can't advance past these seqnos
            // until all memtable applies are complete.
            // NOTE: this line is only reached if ALL journal writes + persist
            // succeeded — error paths above return before reaching here,
            // so no seqnos are left orphaned in the pending set on failure.
            watermark.register(&seqnos);

            // Deliver results: move ops back to callers for memtable apply
            for (write, seqno) in writes.into_iter().zip(seqnos) {
                if leader_result.is_none() && Arc::ptr_eq(&write.slot, leader_slot) {
                    leader_result = Some((seqno, write.op));
                } else {
                    write.slot.complete(WriteOutcome::Success {
                        seqno,
                        op: write.op,
                    });
                }
            }
        }

        drop(journal_writer);

        #[expect(clippy::expect_used, reason = "leader is always present in the group")]
        Ok(leader_result.expect("leader must be in the write group"))
    }

    /// Fail a specific batch of writes.
    fn fail_group(writes: &[PendingWrite]) {
        for write in writes {
            write.slot.complete(WriteOutcome::Failed);
        }
    }

    /// Drain any remaining pending writes, fail them, and release leadership.
    fn drain_fail_and_release(&self) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut inner = self.inner.lock().expect("write group lock poisoned");
        let remaining = std::mem::take(&mut inner.pending);
        inner.leader_active = false;
        drop(inner);

        for write in remaining {
            write.slot.complete(WriteOutcome::Failed);
        }
    }
}

/// Tracks pending write seqnos to prevent the MVCC visible watermark
/// from advancing past seqnos whose memtable applies are still in flight.
///
/// Without this, concurrent memtable-apply + publish after group commit
/// can advance the watermark past seqnos whose writes haven't been applied
/// to the memtable yet, breaking snapshot read consistency.
///
/// # How it works
///
/// 1. The group commit leader **registers** all seqnos in the group as pending
///    (while holding the journal mutex, before any memtable apply).
/// 2. Each caller **applies** its write to the memtable, then calls [`applied`].
///    On failure, the caller calls [`aborted`] instead.
/// 3. `applied` removes the seqno from the pending set and advances the
///    watermark to `min(pending) - 1`. `aborted` removes from pending
///    without advancing — the watermark will advance past the aborted
///    seqno only when a later `applied()` finds no lower pending seqnos.
pub struct PendingWatermark {
    inner: Mutex<WatermarkInner>,
    snapshot_tracker: SnapshotTracker,
}

struct WatermarkInner {
    pending: BTreeSet<SeqNo>,
    max_registered: SeqNo,
}

impl PendingWatermark {
    pub fn new(snapshot_tracker: SnapshotTracker) -> Self {
        Self {
            inner: Mutex::new(WatermarkInner {
                pending: BTreeSet::new(),
                max_registered: 0,
            }),
            snapshot_tracker,
        }
    }

    /// Register a batch of seqnos as pending (not yet applied to memtable).
    ///
    /// Must be called BEFORE the seqnos become eligible for watermark advancement
    /// (i.e., while the journal mutex is still held by the leader).
    pub(crate) fn register(&self, seqnos: &[SeqNo]) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut inner = self.inner.lock().expect("watermark lock poisoned");
        for &seqno in seqnos {
            inner.pending.insert(seqno);
            inner.max_registered = inner.max_registered.max(seqno);
        }
    }

    /// Remove a seqno from pending WITHOUT advancing the watermark.
    ///
    /// Use when the memtable apply failed — the journal entry exists but
    /// the data is not in the memtable. This unblocks the watermark for
    /// other seqnos without falsely claiming this seqno is visible.
    ///
    /// # Design choice: remove vs keep in pending
    ///
    /// Keeping the seqno in pending would permanently freeze the watermark,
    /// blocking ALL subsequent MVCC visibility — a worse outcome than the
    /// transient inconsistency from removing it. The aborted seqno's data
    /// will be replayed from the journal on next recovery, restoring
    /// consistency. This matches the pre-group-commit error behavior where
    /// `tree.clear()` failures propagated without poisoning.
    pub(crate) fn aborted(&self, seqno: SeqNo) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut inner = self.inner.lock().expect("watermark lock poisoned");
        inner.pending.remove(&seqno);
        // Do NOT publish — the data for this seqno is not in the memtable.
        // The watermark will advance past this seqno only when a later
        // seqno's applied() finds pending empty or min > this seqno.
    }

    /// Mark a seqno as applied (memtable write complete) and advance the
    /// visible watermark as far as safely possible.
    pub(crate) fn applied(&self, seqno: SeqNo) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut inner = self.inner.lock().expect("watermark lock poisoned");
        let removed = inner.pending.remove(&seqno);
        if !removed {
            log::error!(
                "PendingWatermark::applied called with unknown or already-applied seqno: {seqno}"
            );
            return;
        }

        // The safe watermark is one below the lowest still-pending seqno.
        // If nothing is pending, all registered writes are applied.
        let safe_seqno = match inner.pending.iter().next() {
            Some(&min_pending) => {
                if min_pending == 0 {
                    return; // seqno 0 still pending, can't advance at all
                }
                min_pending - 1
            }
            None => inner.max_registered,
        };

        drop(inner);

        // publish uses fetch_max internally, so lower values are no-ops
        self.snapshot_tracker.publish(safe_seqno);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{journal::Journal, snapshot_tracker::SnapshotTracker};
    use lsm_tree::{SequenceNumberCounter, SharedSequenceNumberGenerator};
    use std::sync::Arc;

    fn make_seqno() -> SharedSequenceNumberGenerator {
        Arc::new(SequenceNumberCounter::default())
    }

    fn make_watermark(seqno: &SharedSequenceNumberGenerator) -> PendingWatermark {
        PendingWatermark::new(SnapshotTracker::new(Arc::clone(seqno)))
    }

    #[test]
    fn single_writer_roundtrip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let journal_path = dir.path().join("0.jnl");
        let journal = Arc::new(Journal::create_new(&journal_path)?);
        let seqno: SharedSequenceNumberGenerator = make_seqno();
        let is_poisoned = AtomicBool::new(false);
        let wg = WriteGroup::new();
        let wm = make_watermark(&seqno);

        let (seq, op) = wg.submit(
            WriteOp::Raw {
                keyspace_id: 0,
                key: b"hello".to_vec().into(),
                value: b"world".to_vec().into(),
                value_type: ValueType::Value,
            },
            Some(PersistMode::Buffer),
            &journal,
            &seqno,
            &is_poisoned,
            &wm,
        )?;

        assert_eq!(seq, 0);
        match op {
            WriteOp::Raw { key, value, .. } => {
                assert_eq!(&*key, b"hello");
                assert_eq!(&*value, b"world");
            }
            _ => panic!("expected Raw op"),
        }
        wm.applied(seq);
        Ok(())
    }

    #[test]
    fn concurrent_writers_all_succeed() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let journal_path = dir.path().join("0.jnl");
        let journal = Arc::new(Journal::create_new(&journal_path)?);
        let seqno: SharedSequenceNumberGenerator = make_seqno();
        let is_poisoned = Arc::new(AtomicBool::new(false));
        let wg = Arc::new(WriteGroup::new());
        let wm = Arc::new(make_watermark(&seqno));

        let num_writers = 8;
        let barrier = Arc::new(std::sync::Barrier::new(num_writers));
        let mut handles = Vec::new();

        for i in 0..num_writers {
            let journal = Arc::clone(&journal);
            let seqno = Arc::clone(&seqno);
            let is_poisoned = Arc::clone(&is_poisoned);
            let wg = Arc::clone(&wg);
            let wm = Arc::clone(&wm);
            let barrier = Arc::clone(&barrier);

            handles.push(std::thread::spawn(move || {
                barrier.wait();

                let key = format!("key-{i}");
                let val = format!("val-{i}");

                #[expect(clippy::expect_used, reason = "test: can't use ? in spawned thread")]
                let (seq, op) = wg
                    .submit(
                        WriteOp::Raw {
                            keyspace_id: 0,
                            key: key.as_bytes().to_vec().into(),
                            value: val.as_bytes().to_vec().into(),
                            value_type: ValueType::Value,
                        },
                        Some(PersistMode::Buffer),
                        &journal,
                        &seqno,
                        &is_poisoned,
                        &wm,
                    )
                    .expect("submit must not fail");

                match op {
                    WriteOp::Raw {
                        key: k, value: v, ..
                    } => {
                        assert_eq!(&*k, key.as_bytes());
                        assert_eq!(&*v, val.as_bytes());
                    }
                    _ => panic!("expected Raw op"),
                }

                wm.applied(seq);
                seq
            }));
        }

        #[expect(clippy::expect_used, reason = "test: joining thread requires expect")]
        let mut seqnos: Vec<SeqNo> = handles
            .into_iter()
            .map(|h| h.join().expect("thread must not panic"))
            .collect();
        seqnos.sort_unstable();
        seqnos.dedup();
        // All seqnos should be unique
        assert_eq!(seqnos.len(), num_writers);
        Ok(())
    }

    #[test]
    fn poisoned_database_rejects_writes() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let journal_path = dir.path().join("0.jnl");
        let journal = Arc::new(Journal::create_new(&journal_path)?);
        let seqno: SharedSequenceNumberGenerator = make_seqno();
        let is_poisoned = AtomicBool::new(true);
        let wg = WriteGroup::new();
        let wm = make_watermark(&seqno);

        let result = wg.submit(
            WriteOp::Raw {
                keyspace_id: 0,
                key: b"k".to_vec().into(),
                value: b"v".to_vec().into(),
                value_type: ValueType::Value,
            },
            Some(PersistMode::Buffer),
            &journal,
            &seqno,
            &is_poisoned,
            &wm,
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn batch_op_roundtrip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let journal_path = dir.path().join("0.jnl");
        let journal = Arc::new(Journal::create_new(&journal_path)?);
        let seqno: SharedSequenceNumberGenerator = make_seqno();
        let is_poisoned = AtomicBool::new(false);
        let wg = WriteGroup::new();
        let wm = make_watermark(&seqno);

        let db_dir = tempfile::tempdir()?;
        let db = crate::Database::builder(db_dir.path()).open()?;
        let ks = db.keyspace("test", Default::default)?;

        let items = vec![
            crate::batch::item::Item::new(
                ks.clone(),
                UserKey::from(b"k1".to_vec()),
                UserValue::from(b"v1".to_vec()),
                ValueType::Value,
            ),
            crate::batch::item::Item::new(
                ks,
                UserKey::from(b"k2".to_vec()),
                UserValue::from(b"v2".to_vec()),
                ValueType::Value,
            ),
        ];

        let (seq, op) = wg.submit(
            WriteOp::Batch { items },
            Some(PersistMode::Buffer),
            &journal,
            &seqno,
            &is_poisoned,
            &wm,
        )?;

        assert_eq!(seq, 0);
        match op {
            WriteOp::Batch { items } => assert_eq!(items.len(), 2),
            _ => panic!("expected Batch op"),
        }
        wm.applied(seq);
        Ok(())
    }

    #[test]
    fn clear_op_roundtrip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let journal_path = dir.path().join("0.jnl");
        let journal = Arc::new(Journal::create_new(&journal_path)?);
        let seqno: SharedSequenceNumberGenerator = make_seqno();
        let is_poisoned = AtomicBool::new(false);
        let wg = WriteGroup::new();
        let wm = make_watermark(&seqno);

        let (seq, op) = wg.submit(
            WriteOp::Clear { keyspace_id: 42 },
            Some(PersistMode::Buffer),
            &journal,
            &seqno,
            &is_poisoned,
            &wm,
        )?;

        assert_eq!(seq, 0);
        match op {
            WriteOp::Clear { keyspace_id } => assert_eq!(keyspace_id, 42),
            _ => panic!("expected Clear op"),
        }
        wm.applied(seq);
        Ok(())
    }

    #[test]
    fn watermark_advances_only_through_consecutive_seqnos() {
        let visible = make_seqno();
        let wm = PendingWatermark::new(SnapshotTracker::new(Arc::clone(&visible)));

        // Register seqnos 0, 1, 2 as pending
        wm.register(&[0, 1, 2]);

        // Apply out of order: seqno 2 first
        wm.applied(2);
        // Watermark should NOT advance past 0 (still pending)
        assert_eq!(visible.get(), 0);

        // Apply seqno 0
        wm.applied(0);
        // Now 0 is done, but 1 is still pending → safe_seqno = 0, stored = 0+1 = 1
        assert_eq!(visible.get(), 1); // publish(0) → fetch_max(0+1) = 1

        // Apply seqno 1 — all done
        wm.applied(1);
        // Now all applied, watermark = max(2+1) = 3
        assert_eq!(visible.get(), 3); // publish(2) → fetch_max(2+1) = 3
    }

    #[test]
    fn watermark_seqno_zero_pending_blocks_advance() {
        let visible = make_seqno();
        let wm = PendingWatermark::new(SnapshotTracker::new(Arc::clone(&visible)));

        wm.register(&[0, 1]);

        // Apply seqno 1 first — seqno 0 still pending
        wm.applied(1);
        // Watermark must not advance while seqno 0 is pending
        assert_eq!(visible.get(), 0);

        // Now apply seqno 0
        wm.applied(0);
        // Both done, watermark advances
        assert_eq!(visible.get(), 2); // publish(1) → fetch_max(1+1) = 2
    }

    #[test]
    fn watermark_aborted_unblocks_without_advancing() {
        let visible = make_seqno();
        let wm = PendingWatermark::new(SnapshotTracker::new(Arc::clone(&visible)));

        wm.register(&[0, 1, 2]);

        // Apply seqno 0
        wm.applied(0);
        // Seqno 1 still pending → watermark stays at 1
        assert_eq!(visible.get(), 1);

        // Abort seqno 1 (memtable apply failed) — removes from pending
        // without advancing watermark
        wm.aborted(1);
        // Seqno 1 removed, but aborted() doesn't publish → watermark still 1
        assert_eq!(visible.get(), 1);

        // Apply seqno 2 — all pending cleared, watermark advances to max_registered
        wm.applied(2);
        assert_eq!(visible.get(), 3); // publish(2) → fetch_max(2+1) = 3
    }
}
