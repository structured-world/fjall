use crate::config::BenchConfig;
use crate::db::{make_sequential_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace, PersistMode};
use std::time::Instant;

/// Measures database cold-start reopen time after clean shutdown.
/// Write N entries → persist → close → measure reopen latency.
/// This benchmarks metadata/manifest loading, not crash-recovery WAL replay.
pub struct Recovery;

impl Workload for Recovery {
    fn run(
        &self,
        _db: &Database,
        _keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        if config.disable_wal {
            eprintln!("recovery: skipped (--disable_wal has no journal to measure reopen against)");
            // Return without touching reporter — main.rs will see 0 ops and
            // skip emitting a github JSON entry for this workload.
            return Ok(());
        }

        // Own tmpdir required: recovery needs to close and reopen the database,
        // which is incompatible with the shared db/keyspace from run_single().
        let tmpdir = tempfile::tempdir()?;
        let path = tmpdir.path().to_path_buf();

        // Phase 1: populate database.
        {
            let db = Database::builder(&path)
                .cache_size(config.cache_size)
                .open()?;
            let compression_policy =
                fjall::config::CompressionPolicy::all(config.compression_type.to_fjall());
            let ks = db.keyspace("bench", || {
                fjall::KeyspaceCreateOptions::default()
                    .data_block_compression_policy(compression_policy)
            })?;

            for i in 0..config.num {
                let key = make_sequential_key(i, config.key_size);
                let value = make_value(config.value_size);
                ks.insert(key, value)?;
            }

            db.persist(PersistMode::SyncAll)?;

            eprintln!(
                "Populated {} entries ({} bytes/entry), closing database...",
                config.num,
                config.entry_size(),
            );
        }
        // Database dropped — closed cleanly after SyncAll.
        // This measures cold-start open time (metadata + manifest loading),
        // not crash-recovery WAL replay. Clean shutdown flushes all memtables.

        // Phase 2: measure reopen time (10 iterations for stability).
        let iterations = 10u64;

        reporter.start();

        for _ in 0..iterations {
            let t = Instant::now();
            let db = Database::builder(&path)
                .cache_size(config.cache_size)
                .open()?;
            reporter.record_duration(t.elapsed());
            // Close between iterations.
            drop(db);
        }

        reporter.stop();

        let data_size_mb = (config.num as f64 * config.entry_size() as f64) / (1024.0 * 1024.0);
        eprintln!("Recovery: {iterations} iterations over {data_size_mb:.1} MB dataset",);

        Ok(())
    }
}
