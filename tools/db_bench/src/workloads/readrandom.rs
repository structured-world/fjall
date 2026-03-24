use crate::config::BenchConfig;
use crate::db::{fill_sequential_key, prefill_sequential};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use rand::Rng;
use std::time::Instant;

pub struct ReadRandom;

impl Workload for ReadRandom {
    fn run(
        &self,
        _db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        prefill_sequential(keyspace, config)?;

        let mut rng = rand::rng();
        let mut found = 0u64;
        let mut key_buf = vec![0u8; config.key_size];

        reporter.start();

        for _ in 0..config.num {
            let idx: u64 = rng.random_range(0..config.num);
            fill_sequential_key(&mut key_buf, idx);

            let t = Instant::now();
            let result = keyspace.get(&key_buf)?;
            reporter.record_duration(t.elapsed());

            if result.is_some() {
                found += 1;
            }
        }

        reporter.stop();

        if config.num > 0 {
            let hit_rate = found as f64 / config.num as f64 * 100.0;
            eprintln!("Hit rate: {found}/{} ({hit_rate:.1}%)", config.num);
        }

        Ok(())
    }
}
