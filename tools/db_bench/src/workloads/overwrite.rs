use crate::config::BenchConfig;
use crate::db::{make_sequential_key, make_value, prefill_sequential};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use rand::Rng;
use std::time::Instant;

pub struct Overwrite;

impl Workload for Overwrite {
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
            // Allocation outside timed region. insert() takes Into<UserKey>,
            // so we can't reuse a buffer without cloning anyway.
            let key = make_sequential_key(idx, config.key_size);
            let value = make_value(config.value_size);

            let t = Instant::now();
            keyspace.insert(key, value)?;
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();
        Ok(())
    }
}
