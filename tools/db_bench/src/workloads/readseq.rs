use crate::config::BenchConfig;
use crate::db::prefill_sequential;
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fjall::{Database, Keyspace};
use std::time::Instant;

pub struct ReadSeq;

impl Workload for ReadSeq {
    fn run(
        &self,
        _db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()> {
        prefill_sequential(keyspace, config)?;

        let mut count = 0u64;

        reporter.start();

        for guard in keyspace.iter() {
            let t = Instant::now();
            let _value = guard.value()?;
            reporter.record_duration(t.elapsed());

            count += 1;
            if count >= config.num {
                break;
            }
        }

        reporter.stop();

        eprintln!("Scanned {count} entries");

        Ok(())
    }
}
