use crate::config::BenchConfig;
use crate::db::{make_sequential_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace, OptimisticTxDatabase};
use std::time::Instant;

/// OCC transaction benchmark: read-modify-write cycle with commit.
/// Measures OCC commit overhead + conflict detection.
pub struct TxnCommit;

impl Workload for TxnCommit {
    fn run(
        &self,
        _db: &Database,
        _keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        // Create a dedicated OCC database — the shared Database doesn't support
        // optimistic transactions.
        let tmpdir = tempfile::tempdir()?;
        let mut builder =
            OptimisticTxDatabase::builder(tmpdir.path()).cache_size(config.cache_size);

        if config.disable_wal {
            builder = builder.journal_mode(fjall::JournalMode::Noop);
        }

        let tx_db = builder.open()?;

        let compression_policy =
            fjall::config::CompressionPolicy::all(config.compression_type.to_fjall());
        let ks = tx_db.keyspace("bench", || {
            fjall::KeyspaceCreateOptions::default()
                .data_block_compression_policy(compression_policy)
        })?;

        // Prefill some keys so transactions do read-modify-write.
        let prefill = config.num.min(1000);
        for i in 0..prefill {
            let key = make_sequential_key(i, config.key_size);
            let value = make_value(config.value_size);
            ks.insert(key, value)?;
        }

        let mut committed = 0u64;
        let mut conflicts = 0u64;

        // Single-threaded: measures OCC commit overhead without contention.
        // Conflict rate will be 0% — use --threads > 1 for contention testing
        // (not yet implemented; tracked as future work).
        reporter.start();

        for i in 0..config.num {
            let key = make_sequential_key(i % prefill, config.key_size);
            let value = make_value(config.value_size);

            let t = Instant::now();
            let mut tx = tx_db.write_tx()?;
            // Read-modify-write: get_for_update marks key for conflict detection.
            let _ = tx.get_for_update(&ks, &key)?;
            tx.insert(&ks, key, value);
            match tx.commit()? {
                Ok(()) => committed += 1,
                Err(_conflict) => conflicts += 1,
            }
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();

        eprintln!(
            "Committed: {committed}, Conflicts: {conflicts} ({:.1}% conflict rate)",
            if config.num > 0 {
                conflicts as f64 / config.num as f64 * 100.0
            } else {
                0.0
            },
        );

        Ok(())
    }
}
