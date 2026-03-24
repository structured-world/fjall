use crate::config::BenchConfig;
use crate::db::{fill_sequential_key, make_random_key, make_value, prefill_sequential};
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
        let requested = config.threads.max(1);
        let max_readers = usize::try_from(config.num.max(1)).unwrap_or(usize::MAX);
        // Cap so reader_count + 1 (writer) cannot overflow usize.
        let reader_count = requested.min(max_readers).min(usize::MAX - 1);
        if reader_count < requested {
            eprintln!(
                "readwhilewriting: capped readers from {} to {} (--num too small for requested threads)",
                requested, reader_count,
            );
        }
        let total_threads = reader_count + 1;

        let base_ops = config.num / reader_count as u64;
        let remainder = config.num % reader_count as u64;
        let barrier = Barrier::new(total_threads);

        // Timer starts before spawn — spawn overhead is negligible (<1ms)
        // compared to benchmark duration. Matches RocksDB db_bench and lsm-tree.
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

                        let mut key_buf = vec![0u8; config.key_size];
                        for _ in 0..my_ops {
                            let idx: u64 = rng.random_range(0..config.num);
                            fill_sequential_key(&mut key_buf, idx);

                            let t = Instant::now();
                            keyspace.get(&key_buf)?;
                            local_reporter.record_duration(t.elapsed());
                        }

                        Ok(local_reporter)
                    })
                })
                .collect();

            // Background writer thread — errors propagate to fail the workload.
            let writer_handle = s.spawn(|| -> fjall::Result<()> {
                barrier.wait();

                for _ in 0..config.num {
                    let key = make_random_key(config.key_size);
                    let value = make_value(config.value_size);
                    keyspace.insert(key, value)?;
                }
                Ok(())
            });

            // Only reader ops are counted — this is a read throughput benchmark
            // with concurrent write pressure, matching RocksDB db_bench semantics.
            for handle in reader_handles {
                #[expect(clippy::expect_used, reason = "reader panic is unrecoverable")]
                let local_reporter = handle.join().expect("reader thread panicked")?;
                reporter.merge(&local_reporter);
            }

            #[expect(clippy::expect_used, reason = "writer panic is unrecoverable")]
            writer_handle.join().expect("writer thread panicked")?;

            // Intentionally stop after writer join — elapsed must cover the full
            // concurrent read+write window. Stopping after readers but before writer
            // would undercount elapsed when the writer is slower, inflating ops/sec.
            reporter.stop();

            Ok(())
        });

        scope_result?;

        Ok(())
    }
}
