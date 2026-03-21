// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fjall::{Database, KeyspaceCreateOptions, OptimisticTxDatabase, Readable};

const BATCH_SIZE: usize = 1_000;

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
                        for i in 0..BATCH_SIZE {
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
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
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
                            tx.insert(ks.inner(), i.to_be_bytes(), b"value");
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
    // Each iteration opens two transactions that both read+write the same key,
    // then commits them in order — the second should conflict under SSI.
    group.throughput(Throughput::Elements(100));

    group.bench_function("hot_key_range", |b| {
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

                    // Both read and write key 0
                    tx1.get(ks.inner(), 0u64.to_be_bytes()).unwrap();
                    tx1.insert(ks.inner(), 0u64.to_be_bytes(), b"tx1");

                    tx2.get(ks.inner(), 0u64.to_be_bytes()).unwrap();
                    tx2.insert(ks.inner(), 0u64.to_be_bytes(), b"tx2");

                    // First commit succeeds
                    match tx1.commit() {
                        Ok(Ok(())) => {}
                        Ok(Err(_conflict)) => conflicts += 1,
                        Err(e) => panic!("unexpected error on tx1: {e}"),
                    }

                    // Second commit should conflict (read set invalidated by tx1)
                    match tx2.commit() {
                        Ok(Ok(())) => {}
                        Ok(Err(_conflict)) => conflicts += 1,
                        Err(e) => panic!("unexpected error on tx2: {e}"),
                    }
                }
                conflicts
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
                b.iter_batched(
                    || {
                        // Setup: populate database then drop it. Measures reopen time
                        // which includes journal replay for any unflushed entries.
                        let tmpdir = tempfile::tempdir().unwrap();
                        {
                            let db = Database::builder(tmpdir.path()).open().unwrap();
                            let ks = db
                                .keyspace("bench", KeyspaceCreateOptions::default)
                                .unwrap();
                            for i in 0..count {
                                ks.insert(i.to_be_bytes(), b"value_data_for_recovery")
                                    .unwrap();
                            }
                            // Drop flushes memtables to segments; journal cleanup follows
                        }
                        tmpdir
                    },
                    |tmpdir| {
                        // Measure: reopen (includes journal recovery)
                        let db = Database::builder(tmpdir.path()).open().unwrap();
                        let ks = db
                            .keyspace("bench", KeyspaceCreateOptions::default)
                            .unwrap();
                        // Verify at least one key to prevent dead-code elimination
                        assert!(ks.get(0usize.to_be_bytes()).unwrap().is_some());
                    },
                    criterion::BatchSize::PerIteration,
                );
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
