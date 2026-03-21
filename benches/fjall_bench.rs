// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fjall::{Database, KeyspaceCreateOptions, OptimisticTxDatabase};

const BATCH_SIZE: usize = 1_000;

// Each benchmark has distinct setup (Database vs OptimisticTxDatabase, different
// keyspace counts, seeding patterns). Extracting a shared helper would require
// generics or trait objects without reducing meaningful duplication.

fn keyspace_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("keyspace_write");

    for value_size in [64_usize, 1_024, 65_536] {
        group.throughput(Throughput::Bytes((value_size * BATCH_SIZE) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
            &value_size,
            |b, &size| {
                let value = vec![0xABu8; size];

                b.iter_batched(
                    || {
                        let tmpdir = tempfile::tempdir().unwrap();
                        let db = Database::builder(tmpdir.path()).open().unwrap();
                        let ks = db
                            .keyspace("bench", KeyspaceCreateOptions::default)
                            .unwrap();
                        (tmpdir, db, ks)
                    },
                    |(_tmpdir, _db, ks)| {
                        for i in 0..BATCH_SIZE as u64 {
                            ks.insert(i.to_be_bytes(), &value).unwrap();
                        }
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn partition_switch(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_switch");

    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("10_keyspaces", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().unwrap();
                let db = Database::builder(tmpdir.path()).open().unwrap();
                let keyspaces: Vec<_> = (0..10)
                    .map(|i| {
                        db.keyspace(&format!("ks_{i}"), KeyspaceCreateOptions::default)
                            .unwrap()
                    })
                    .collect();
                (tmpdir, db, keyspaces)
            },
            |(_tmpdir, _db, keyspaces)| {
                for i in 0..BATCH_SIZE {
                    let ks = &keyspaces[i % keyspaces.len()];
                    ks.insert((i as u64).to_be_bytes(), b"value").unwrap();
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn tx_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_commit");

    // Measures single-threaded OCC commit overhead (oracle lock, conflict check,
    // journal write). Multi-threaded contention benchmarks belong in tx_conflict_rate.
    for tx_count in [100_usize, 500, 1_000] {
        group.throughput(Throughput::Elements(tx_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(tx_count),
            &tx_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let tmpdir = tempfile::tempdir().unwrap();
                        let db = OptimisticTxDatabase::builder(tmpdir.path()).open().unwrap();
                        let ks = db
                            .keyspace("bench", KeyspaceCreateOptions::default)
                            .unwrap();
                        (tmpdir, db, ks)
                    },
                    |(_tmpdir, db, ks)| {
                        for i in 0..count {
                            let mut tx = db.write_tx().unwrap();
                            tx.insert(ks.inner(), (i as u64).to_be_bytes(), b"value");
                            tx.commit().unwrap().unwrap();
                        }
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn tx_conflict_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_conflict_rate");

    // Measure conflict rate with overlapping transactions on a hot key.
    // Each iteration runs 100 conflict-test pairs (2 transactions each = 200 commits).
    group.throughput(Throughput::Elements(200));

    group.bench_function("hot_key", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().unwrap();
                let db = OptimisticTxDatabase::builder(tmpdir.path()).open().unwrap();
                let ks = db
                    .keyspace("bench", KeyspaceCreateOptions::default)
                    .unwrap();
                // Seed key 0
                ks.insert(0u64.to_be_bytes(), b"init").unwrap();
                (tmpdir, db, ks)
            },
            |(_tmpdir, db, ks)| {
                let mut conflicts = 0u64;
                for _ in 0..100 {
                    // Create two overlapping transactions on the same hot key
                    let mut tx1 = db.write_tx().unwrap();
                    let mut tx2 = db.write_tx().unwrap();

                    // Both read (for_update) and write key 0.
                    // debug_assert: verify seeded key exists without skewing bench measurements.
                    let v1 = tx1.get_for_update(ks.inner(), 0u64.to_be_bytes()).unwrap();
                    debug_assert!(v1.is_some(), "seeded key 0 must be present for tx1");
                    tx1.insert(ks.inner(), 0u64.to_be_bytes(), b"tx1");

                    let v2 = tx2.get_for_update(ks.inner(), 0u64.to_be_bytes()).unwrap();
                    debug_assert!(v2.is_some(), "seeded key 0 must be present for tx2");
                    tx2.insert(ks.inner(), 0u64.to_be_bytes(), b"tx2");

                    // First commit must succeed; a conflict here is a bug
                    match tx1.commit() {
                        Ok(Ok(())) => {}
                        Ok(Err(c)) => panic!("unexpected conflict on tx1: {c:?}"),
                        Err(e) => panic!("unexpected error on tx1: {e}"),
                    }

                    // Second commit must conflict (read set invalidated by tx1)
                    match tx2.commit() {
                        Ok(Ok(())) => panic!("tx2 committed without conflict — SSI violation"),
                        Ok(Err(_conflict)) => conflicts += 1,
                        Err(e) => panic!("unexpected error on tx2: {e}"),
                    }
                }
                criterion::black_box(conflicts)
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn recovery_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery_time");
    group.sample_size(10); // Recovery is slow, fewer samples

    for entry_count in [1_000_usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(entry_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(entry_count),
            &entry_count,
            |b, &count| {
                // One-time setup: populate database, then measure reopen time only
                let tmpdir = tempfile::tempdir().unwrap();
                {
                    let db = Database::builder(tmpdir.path()).open().unwrap();
                    let ks = db
                        .keyspace("bench", KeyspaceCreateOptions::default)
                        .unwrap();
                    for i in 0..count as u64 {
                        ks.insert(i.to_be_bytes(), b"value_data_for_recovery")
                            .unwrap();
                    }
                    // Drop flushes memtables to segments; journal cleanup follows
                }
                let path = tmpdir.path().to_path_buf();

                // Repeated reopens of the same path are intentional: measures warm
                // recovery time (OS page cache populated). Cold recovery would require
                // a fresh tmpdir per iteration, defeating the one-time population setup.
                b.iter(|| {
                    let db = Database::builder(&path).open().unwrap();
                    let ks = db
                        .keyspace("bench", KeyspaceCreateOptions::default)
                        .unwrap();
                    // Consume the read result to prevent dead-code elimination
                    let value = ks.get(0u64.to_be_bytes()).unwrap();
                    debug_assert!(value.is_some());
                    criterion::black_box(value);
                });
            },
        );
    }

    group.finish();
}

// TODO: concurrent_merge benchmark deferred — merge operators are at lsm-tree level,
// not exposed through fjall's public API. See structured-world/lsm-tree#42.

criterion_group!(
    benches,
    keyspace_write,
    partition_switch,
    tx_commit,
    tx_conflict_rate,
    recovery_time,
);
criterion_main!(benches);
