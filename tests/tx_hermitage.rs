// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Hermitage-style transaction anomaly test suite for OptimisticTxDatabase (SSI).
//!
//! Tests adapted from the Hermitage project methodology to verify that fjall's
//! SSI implementation correctly prevents all serialization anomalies.
//! See: <https://ithare.com/testing-database-transaction-isolation/>
//!
//! All reads that participate in conflict detection use `*_for_update()` methods.
//! Snapshot reads (`Readable::get`) are non-serializable and intentionally skip
//! conflict tracking — see `read_only_snapshot_no_conflict` for that case.

use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use tempfile::TempDir;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct TestEnv {
    db: OptimisticTxDatabase,
    ks: OptimisticTxKeyspace,
    // Prevent early cleanup — dropping TempDir deletes the directory on disk.
    _tmpdir: TempDir,
}

fn setup() -> Result<TestEnv> {
    let tmpdir = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;
    // keyspace() takes impl FnOnce() — passing the function item directly is correct
    let ks = db.keyspace("test", KeyspaceCreateOptions::default)?;
    Ok(TestEnv {
        db,
        ks,
        _tmpdir: tmpdir,
    })
}

/// Two transactions both read the same key, then write a new value based on
/// what they read. Under SSI, the second commit must detect the conflict.
///
/// T1: read X → T2: read X → T1: write X, commit → T2: write X, commit → Conflict
#[test]
fn lost_update_prevented_by_ssi() -> Result {
    let env = setup()?;

    // Seed initial value
    env.ks.insert("balance", 100u64.to_be_bytes())?;

    // T1: read balance (for_update → tracked for conflict detection)
    let mut tx1 = env.db.write_tx()?;
    let val1 = tx1.get_for_update(env.ks.inner(), "balance")?.unwrap();
    let balance1 = u64::from_be_bytes(val1.as_ref().try_into()?);

    // T2: read same balance
    let mut tx2 = env.db.write_tx()?;
    let val2 = tx2.get_for_update(env.ks.inner(), "balance")?.unwrap();
    let balance2 = u64::from_be_bytes(val2.as_ref().try_into()?);

    assert_eq!(balance1, 100);
    assert_eq!(balance2, 100);

    // T1: write updated value, commit
    tx1.insert(env.ks.inner(), "balance", (balance1 + 10).to_be_bytes());
    tx1.commit()??;

    // T2: write updated value based on stale read, commit → must conflict
    tx2.insert(env.ks.inner(), "balance", (balance2 + 20).to_be_bytes());
    let result = tx2.commit()?;
    assert!(result.is_err(), "lost update must be prevented by SSI");
    Ok(())
}

/// Classic write skew: T1 reads A and writes B; T2 reads B and writes A.
/// Under SSI, at least one must abort because the combined execution is
/// not equivalent to any serial order.
///
/// Example: two doctors on-call. Each checks the other is on-call, then
/// removes themselves. Without SSI, both succeed and nobody is on-call.
#[test]
fn write_skew_detected() -> Result {
    let env = setup()?;

    // Both doctors on-call
    env.ks.insert("alice_oncall", b"true")?;
    env.ks.insert("bob_oncall", b"true")?;

    // T1: check Bob is on-call (for_update), then remove Alice
    let mut tx1 = env.db.write_tx()?;
    let bob = tx1.get_for_update(env.ks.inner(), "bob_oncall")?.unwrap();
    assert_eq!(bob.as_ref(), b"true");
    tx1.insert(env.ks.inner(), "alice_oncall", b"false");

    // T2: check Alice is on-call (for_update), then remove Bob
    let mut tx2 = env.db.write_tx()?;
    let alice = tx2.get_for_update(env.ks.inner(), "alice_oncall")?.unwrap();
    assert_eq!(alice.as_ref(), b"true");
    tx2.insert(env.ks.inner(), "bob_oncall", b"false");

    // Under SSI, this write skew pattern must not allow both commits to succeed.
    // The API does not guarantee which transaction aborts, only that at least one does.
    let r1 = tx1.commit()?;
    let r2 = tx2.commit()?;
    assert!(
        r1.is_err() || r2.is_err(),
        "at least one transaction must conflict to prevent write skew"
    );
    Ok(())
}

/// T1 scans a range, T2 inserts a new key within that range and commits.
/// T1 then writes and commits — must conflict because the range read is
/// invalidated by T2's insert (phantom prevention).
#[test]
fn phantom_read_in_range_prevented() -> Result {
    let env = setup()?;

    // Seed keys in range [a..z]
    env.ks.insert("item_a", b"1")?;
    env.ks.insert("item_m", b"2")?;
    env.ks.insert("item_z", b"3")?;

    // T1: scan range to count items (for_update → range tracked).
    // Iter::Item = Guard (not Result), so .count() is safe — no errors to propagate.
    let mut tx1 = env.db.write_tx()?;
    let count: usize = tx1
        .range_for_update(env.ks.inner(), "item_a"..="item_z")
        .count();
    assert_eq!(count, 3);

    // T2: insert a phantom into that range and commit
    let mut tx2 = env.db.write_tx()?;
    tx2.insert(env.ks.inner(), "item_f", b"phantom");
    tx2.commit()??;

    // T1: write something (to make it a read-write tx) and commit.
    // Fjall's conflict manager tracks the exact range from range_for_update
    // and detects overlap with T2's insert — this always produces a conflict.
    tx1.insert(env.ks.inner(), "summary", b"count_was_3");
    let result = tx1.commit()?;
    assert!(
        result.is_err(),
        "phantom insert into scanned range must cause conflict"
    );
    Ok(())
}

/// Same as range phantom but using prefix scan.
#[test]
fn phantom_read_in_prefix_prevented() -> Result {
    let env = setup()?;

    // Seed keys with prefix
    env.ks.insert("user:1", b"alice")?;
    env.ks.insert("user:2", b"bob")?;

    // T1: prefix scan (for_update → prefix range tracked).
    // Iter::Item = Guard (not Result), so .count() is safe.
    let mut tx1 = env.db.write_tx()?;
    let count: usize = tx1.prefix_for_update(env.ks.inner(), "user:").count();
    assert_eq!(count, 2);

    // T2: insert new key with same prefix and commit
    let mut tx2 = env.db.write_tx()?;
    tx2.insert(env.ks.inner(), "user:3", b"charlie");
    tx2.commit()??;

    // T1: write and commit → must conflict.
    // Fjall's conflict manager expands prefix_for_update into a concrete range
    // and detects overlap with T2's insert — this always produces a conflict.
    tx1.insert(env.ks.inner(), "user_count", b"2");
    let result = tx1.commit()?;
    assert!(
        result.is_err(),
        "phantom insert into prefix-scanned range must cause conflict"
    );
    Ok(())
}

/// A snapshot read (non-serializable) should not cause conflicts even if
/// concurrent writers modify the same keys, because snapshot reads are
/// intentionally excluded from conflict tracking.
#[test]
fn read_only_snapshot_no_conflict() -> Result {
    let env = setup()?;

    env.ks.insert("key", b"original")?;

    // T1: snapshot read (Readable::get — NOT tracked for conflicts)
    let tx1 = env.db.write_tx()?;
    let val = tx1.get(env.ks.inner(), "key")?.unwrap();
    assert_eq!(val.as_ref(), b"original");

    // T2: write to same key and commit
    let mut tx2 = env.db.write_tx()?;
    tx2.insert(env.ks.inner(), "key", b"modified");
    tx2.commit()??;

    // T1: commit (no writes, snapshot read only) → should succeed
    let result = tx1.commit()?;
    assert!(
        result.is_ok(),
        "snapshot read-only transaction should not conflict"
    );
    Ok(())
}

/// Two transactions both use update_fetch on the same key. The second must
/// conflict because update_fetch marks both a read and a write on the key.
fn increment(prev: Option<&fjall::UserValue>) -> Option<fjall::UserValue> {
    let val = prev
        .map(|v| u64::from_be_bytes(v.as_ref().try_into().expect("8 bytes")))
        .unwrap_or(0);
    Some((val + 1).to_be_bytes().into())
}

#[test]
fn concurrent_update_fetch_conflict() -> Result {
    let env = setup()?;

    env.ks.insert("counter", 0u64.to_be_bytes())?;

    // T1: increment counter via update_fetch (implicitly for_update)
    let mut tx1 = env.db.write_tx()?;
    tx1.update_fetch(env.ks.inner(), "counter", increment)?;

    // T2: increment same counter via update_fetch
    let mut tx2 = env.db.write_tx()?;
    tx2.update_fetch(env.ks.inner(), "counter", increment)?;

    // T1 commits first
    tx1.commit()??;

    // T2 must conflict (read-write on same key)
    let result = tx2.commit()?;
    assert!(
        result.is_err(),
        "concurrent update_fetch on same key must conflict"
    );
    Ok(())
}

/// Two transactions write to completely disjoint keys without reading each
/// other's keys. Both should commit successfully — no conflict.
#[test]
fn disjoint_key_writes_no_conflict() -> Result {
    let env = setup()?;

    // T1: write key A only
    let mut tx1 = env.db.write_tx()?;
    tx1.insert(env.ks.inner(), "key_a", b"value_a");

    // T2: write key B only
    let mut tx2 = env.db.write_tx()?;
    tx2.insert(env.ks.inner(), "key_b", b"value_b");

    // Both should commit without conflict
    tx1.commit()??;
    tx2.commit()??;

    // Verify both writes are visible
    let a = env.ks.get("key_a")?.unwrap();
    let b = env.ks.get("key_b")?.unwrap();
    assert_eq!(a.as_ref(), b"value_a");
    assert_eq!(b.as_ref(), b"value_b");
    Ok(())
}

/// T1 reads from keyspace A (for_update), T2 writes to the same key in
/// keyspace A and commits. T1 then writes to keyspace B and tries to
/// commit — must conflict because its read set (keyspace A) was invalidated.
#[test]
fn cross_keyspace_conflict() -> Result {
    let env = setup()?;
    let ks_b = env.db.keyspace("ks_b", KeyspaceCreateOptions::default)?;

    env.ks.insert("shared_key", b"original")?;

    // T1: read from keyspace A (for_update → tracked)
    let mut tx1 = env.db.write_tx()?;
    tx1.get_for_update(env.ks.inner(), "shared_key")?
        .expect("shared_key should exist for T1 read dependency");

    // T2: write to same key in keyspace A and commit
    let mut tx2 = env.db.write_tx()?;
    tx2.insert(env.ks.inner(), "shared_key", b"modified");
    tx2.commit()??;

    // T1: write to keyspace B and commit → must conflict (read in ks_a invalidated)
    tx1.insert(ks_b.inner(), "unrelated", b"data");
    let result = tx1.commit()?;
    assert!(
        result.is_err(),
        "cross-keyspace transaction must conflict when read set is invalidated"
    );
    Ok(())
}

/// Open a long-running transaction (T1), then commit 1,000 short transactions.
/// T1 must still get correct conflict detection — oracle GC must not discard
/// conflict information needed for T1's window.
#[test]
fn long_running_tx_gc_interaction() -> Result {
    let env = setup()?;

    env.ks.insert("watched_key", b"initial")?;

    // T1: start a long-running transaction, read the watched key (for_update)
    let mut tx1 = env.db.write_tx()?;
    let val = tx1.get_for_update(env.ks.inner(), "watched_key")?.unwrap();
    assert_eq!(val.as_ref(), b"initial");

    // 1,000 iterations is the minimum to exercise oracle GC cleanup logic.
    // Measured at ~200ms total on disk-backed storage — acceptable for CI.
    for i in 0u64..1_000 {
        let mut tx = env.db.write_tx()?;
        tx.insert(env.ks.inner(), "watched_key", i.to_be_bytes());
        tx.commit()??;
    }

    // T1: write and commit — must conflict because watched_key was modified
    tx1.insert(env.ks.inner(), "watched_key", b"stale_update");
    let result = tx1.commit()?;
    assert!(
        result.is_err(),
        "long-running transaction must detect conflict despite oracle GC"
    );
    Ok(())
}

/// A rolled-back transaction must not leak its writes to other transactions.
#[test]
fn rollback_does_not_leak_writes() -> Result {
    let env = setup()?;

    env.ks.insert("key", b"original")?;

    // T1: write new value but roll back.
    // rollback(self) consumes the transaction — tx1 is dropped here.
    let mut tx1 = env.db.write_tx()?;
    tx1.insert(env.ks.inner(), "key", b"rolled_back_value");
    tx1.rollback();

    // T2: should see the original value, not the rolled-back one
    let tx2 = env.db.write_tx()?;
    let val = tx2.get(env.ks.inner(), "key")?.unwrap();
    assert_eq!(
        val.as_ref(),
        b"original",
        "rolled-back writes must not be visible to other transactions"
    );
    tx2.commit()??;

    // Direct read should also see original
    let val = env.ks.get("key")?.unwrap();
    assert_eq!(val.as_ref(), b"original");
    Ok(())
}
