pub mod fillrandom;
pub mod fillseq;
pub mod multi_keyspace;
pub mod overwrite;
pub mod readrandom;
pub mod readseq;
pub mod readwhilewriting;
pub mod recovery;
pub mod seekrandom;
pub mod txn_commit;

use crate::config::BenchConfig;
use crate::reporter::Reporter;
use fjall::{Database, Keyspace};

/// All benchmark workloads implement this trait.
pub trait Workload {
    /// Run the benchmark, recording latencies into the reporter.
    fn run(
        &self,
        db: &Database,
        keyspace: &Keyspace,
        config: &BenchConfig,
        reporter: &mut Reporter,
    ) -> fjall::Result<()>;
}

/// Single source of truth for workload name → type mapping.
macro_rules! define_workloads {
    ( $( $name:expr => $ty:path ),+ $(,)? ) => {
        /// Create a workload by name.
        pub fn create_workload(name: &str) -> Option<Box<dyn Workload>> {
            match name {
                $( $name => Some(Box::new($ty)), )+
                _ => None,
            }
        }

        /// List all available benchmark names.
        pub fn available_benchmarks() -> &'static [&'static str] {
            &[ $( $name, )+ ]
        }
    };
}

define_workloads! {
    "fillseq" => fillseq::FillSeq,
    "fillrandom" => fillrandom::FillRandom,
    "readseq" => readseq::ReadSeq,
    "readrandom" => readrandom::ReadRandom,
    "overwrite" => overwrite::Overwrite,
    "seekrandom" => seekrandom::SeekRandom,
    "readwhilewriting" => readwhilewriting::ReadWhileWriting,
    "txn_commit" => txn_commit::TxnCommit,
    "recovery" => recovery::Recovery,
    "multi_keyspace" => multi_keyspace::MultiKeyspace,
}
