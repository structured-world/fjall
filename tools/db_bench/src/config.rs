use clap::ValueEnum;
use fjall::{CompressionType, JournalMode};
use std::path::Path;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Lz4 => f.write_str("lz4"),
            Self::Zstd => f.write_str("zstd"),
        }
    }
}

impl Compression {
    pub fn to_fjall(self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            Self::Lz4 => CompressionType::Lz4,
            Self::Zstd => CompressionType::Zstd(3),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub num: u64,
    pub key_size: usize,
    pub value_size: usize,
    pub threads: usize,
    pub cache_mb: u64,
    pub compression: Compression,
    pub disable_wal: bool,
    pub sync: bool,
    pub keyspaces: usize,
}

impl BenchConfig {
    /// Bytes per key-value pair (for throughput calculation).
    pub fn entry_size(&self) -> usize {
        self.key_size + self.value_size
    }
}

/// Create a fjall Database + Keyspace at the given path using the benchmark configuration.
pub fn create_db(
    path: &Path,
    config: &BenchConfig,
) -> fjall::Result<(fjall::Database, fjall::Keyspace)> {
    let cache_bytes = config.cache_mb * 1024 * 1024;

    let compression_policy = fjall::config::CompressionPolicy::all(config.compression.to_fjall());

    let mut builder = fjall::Database::builder(path).cache_size(cache_bytes);

    if config.disable_wal {
        builder = builder.journal_mode(JournalMode::Noop);
    }

    let db = builder.open()?;

    let keyspace = db.keyspace("default", || {
        fjall::KeyspaceCreateOptions::default().data_block_compression_policy(compression_policy)
    })?;

    Ok((db, keyspace))
}
