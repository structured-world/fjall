use crate::config::BenchConfig;
use crate::db::{make_random_key, make_sequential_key, make_value, prefill_sequential};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use rand::Rng;
use std::sync::Barrier;
use std::time::Instant;

pub struct ReadWhileWriting;

impl Workload for ReadWhileWriting {
    fn run(
        &self,
        _db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        prefill_sequential(keyspace, config)?;

        // RocksDB convention: --threads=N gives N readers + 1 background writer.
        // Minimum 1 reader + 1 writer = 2 threads.
        let reader_count = config.threads.max(1);
        let max_readers = usize::try_from(config.num.max(1)).unwrap_or(usize::MAX);
        let reader_count = reader_count.min(max_readers);
        let total_threads = reader_count + 1;

        let base_ops = config.num / reader_count as u64;
        let remainder = config.num % reader_count as u64;
        let barrier = Barrier::new(total_threads);

        reporter.start();

        let scope_result: fjall::Result<()> = std::thread::scope(|s| {
            let reader_handles: Vec<_> = (0..reader_count)
                .map(|i| {
                    let my_ops = base_ops + if (i as u64) < remainder { 1 } else { 0 };
                    let barrier = &barrier;
                    s.spawn(move || -> fjall::Result<Reporter> {
                        let mut local_reporter = Reporter::new();
                        let mut rng = rand::rng();
                        barrier.wait();

                        for _ in 0..my_ops {
                            let idx: u64 = rng.random_range(0..config.num);
                            let key = make_sequential_key(idx, config.key_size);

                            let t = Instant::now();
                            keyspace.get(&key)?;
                            local_reporter.record_duration(t.elapsed());
                        }

                        Ok(local_reporter)
                    })
                })
                .collect();

            // Background writer thread.
            let writer_handle = s.spawn(|| {
                barrier.wait();

                for _ in 0..config.num {
                    let key = make_random_key(config.key_size);
                    let value = make_value(config.value_size);
                    // Ignore write errors in background writer to avoid
                    // blocking reader measurements.
                    let _ = keyspace.insert(key, value);
                }
            });

            // Only reader ops are counted — this is a read throughput benchmark
            // with concurrent write pressure, matching RocksDB db_bench semantics.
            for handle in reader_handles {
                #[expect(clippy::expect_used, reason = "reader panic is unrecoverable")]
                let local_reporter = handle.join().expect("reader thread panicked")?;
                reporter.merge(&local_reporter);
            }

            reporter.stop();

            #[expect(clippy::expect_used, reason = "writer panic is unrecoverable")]
            writer_handle.join().expect("writer thread panicked");

            Ok(())
        });

        scope_result?;

        Ok(())
    }
}
