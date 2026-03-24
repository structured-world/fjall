use crate::config::BenchConfig;
use crate::db::{make_random_key, make_value};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use std::time::Instant;

pub struct FillRandom;

impl Workload for FillRandom {
    fn run(
        &self,
        _db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        reporter.start();

        for _ in 0..config.num {
            let key = make_random_key(config.key_size);
            let value = make_value(config.value_size);

            let t = Instant::now();
            keyspace.insert(key, value)?;
            reporter.record_duration(t.elapsed());
        }

        reporter.stop();
        Ok(())
    }
}
