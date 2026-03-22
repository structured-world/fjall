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

        // IMPORTANT: Check poisoned flag AFTER acquiring serialization (TOCTOU)
        if is_poisoned.load(Ordering::Relaxed) {
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

/// Tracks pending write seqnos and ensures the MVCC visible watermark
/// only advances through consecutively-applied seqnos.
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
/// 3. `applied` removes the seqno from the pending set and advances the
///    watermark to `min(pending) - 1` — i.e., all seqnos below the lowest
///    still-pending one are guaranteed visible.
pub struct PendingWatermark {
    inner: Mutex<WatermarkInner>,
    snapshot_tracker: SnapshotTracker,
}

struct WatermarkInner {
    pending: BTreeSet<SeqNo>,
}

impl PendingWatermark {
    pub fn new(snapshot_tracker: SnapshotTracker) -> Self {
        Self {
            inner: Mutex::new(WatermarkInner {
                pending: BTreeSet::new(),
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
        }
    }

    /// Mark a seqno as applied (memtable write complete) and advance the
    /// visible watermark as far as safely possible.
    pub(crate) fn applied(&self, seqno: SeqNo) {
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let mut inner = self.inner.lock().expect("watermark lock poisoned");
        inner.pending.remove(&seqno);

        // The safe watermark is one below the lowest still-pending seqno.
        // If nothing is pending, all writes up to this seqno are applied.
        let safe_seqno = match inner.pending.iter().next() {
            Some(&min_pending) => {
                if min_pending == 0 {
                    return; // seqno 0 still pending, can't advance at all
                }
                min_pending - 1
            }
            None => seqno,
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
}
