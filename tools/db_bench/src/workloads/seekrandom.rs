use crate::config::BenchConfig;
use crate::db::{make_sequential_key, prefill_sequential};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use rand::Rng;
use std::time::Instant;

pub struct SeekRandom;

impl Workload for SeekRandom {
    fn run(
        &self,
        _db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        prefill_sequential(keyspace, config)?;

        let mut rng = rand::rng();

        reporter.start();

        for _ in 0..config.num {
            let idx: u64 = rng.random_range(0..config.num);
            // Allocation is outside the timed region. range() takes ownership
            // of the key, so fill_sequential_key can't be used here.
            let key = make_sequential_key(idx, config.key_size);

            let t = Instant::now();
            let mut iter = keyspace.range(key..);
            if let Some(guard) = iter.next() {
                let _value = guard.value()?;
            }
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();
        Ok(())
    }
}
