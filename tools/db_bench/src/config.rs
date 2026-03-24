use clap::ValueEnum;
use fjall::{CompressionType, JournalMode};
use std::path::Path;

// db_bench Cargo.toml enables both lz4 and zstd features unconditionally —
// all variants are always available. No cfg-gating needed for a standalone tool.
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
    pub cache_size: u64,
    pub compression_type: Compression,
    pub disable_wal: bool,
    #[expect(dead_code, reason = "reserved for PersistMode::SyncAll wiring")]
    pub sync: bool,
    pub keyspaces: usize,
}

impl BenchConfig {
    /// Bytes per key-value pair (for throughput calculation).
    pub fn entry_size(&self) -> usize {
        self.key_size.saturating_add(self.value_size)
    }
}

/// Create a fjall Database + Keyspace at the given path using the benchmark configuration.
pub fn create_db(
    path: &Path,
    config: &BenchConfig,
) -> fjall::Result<(fjall::Database, fjall::Keyspace)> {
    let compression_policy =
        fjall::config::CompressionPolicy::all(config.compression_type.to_fjall());

    let mut builder = fjall::Database::builder(path).cache_size(config.cache_size);

    // JournalMode::Noop disables fjall's WAL I/O while keeping the full fjall
    // stack (keyspace routing, write group, etc.). This isolates WAL overhead
    // specifically. For truly raw lsm-tree numbers, use lsm-tree's own db_bench.
    if config.disable_wal {
        builder = builder.journal_mode(JournalMode::Noop);
    }

    let db = builder.open()?;

    let keyspace = db.keyspace("default", || {
        fjall::KeyspaceCreateOptions::default().data_block_compression_policy(compression_policy)
    })?;

    Ok((db, keyspace))
}
