use crate::config::BenchConfig;
use crate::db::{make_sequential_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use std::time::Instant;

/// Measures write throughput across multiple keyspaces.
/// Exercises keyspace partition switching overhead.
pub struct MultiKeyspace;

impl Workload for MultiKeyspace {
    fn run(
        &self,
        _db: &Database,
        _keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        if config.keyspaces == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multi_keyspace: --keyspaces must be > 0",
            )
            .into());
        }

        // Own tmpdir: needs multiple keyspaces on a fresh database, incompatible
        // with the single shared keyspace from run_single().
        let tmpdir = tempfile::tempdir()?;

        let mut builder = Database::builder(tmpdir.path()).cache_size(config.cache_size);

        if config.disable_wal {
            builder = builder.journal_mode(fjall::JournalMode::Noop);
        }

        let db = builder.open()?;

        let num_keyspaces = config.keyspaces;
        let compression_policy =
            fjall::config::CompressionPolicy::all(config.compression_type.to_fjall());
        let keyspaces: Vec<Keyspace> = (0..num_keyspaces)
            .map(|i| {
                db.keyspace(&format!("ks_{i}"), || {
                    fjall::KeyspaceCreateOptions::default()
                        .data_block_compression_policy(compression_policy.clone())
                })
            })
            .collect::<fjall::Result<Vec<_>>>()?;

        reporter.start();

        let mut ks_idx: usize = 0;
        for i in 0..config.num {
            let ks = &keyspaces[ks_idx];
            ks_idx += 1;
            if ks_idx == num_keyspaces {
                ks_idx = 0;
            }
            let key = make_sequential_key(i, config.key_size);
            let value = make_value(config.value_size);

            let t = Instant::now();
            ks.insert(key, value)?;
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();

        eprintln!(
            "Wrote {} entries across {} keyspaces",
            config.num, num_keyspaces,
        );

        Ok(())
    }
}
