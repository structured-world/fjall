mod config;
mod db;
mod reporter;
mod workloads;

use crate::config::{BenchConfig, Compression};
use crate::reporter::{JsonConfig, Reporter};
use crate::workloads::{available_benchmarks, create_workload};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "db_bench",
    about = "fjall benchmark suite (RocksDB db_bench compatible output)",
    // RocksDB uses snake_case flags (--key_size, --value_size, --cache_size, etc.)
    rename_all = "snake_case"
)]
struct Cli {
    /// Comma-separated list of benchmarks to run. Use "all" to run every workload.
    #[arg(long, default_value = "all", value_parser = parse_benchmarks)]
    benchmarks: String,

    /// Number of operations.
    #[arg(long, default_value = "200000")]
    num: u64,

    /// Key size in bytes.
    #[arg(long, default_value = "16")]
    key_size: usize,

    /// Value size in bytes.
    #[arg(long, default_value = "100")]
    value_size: usize,

    /// Number of concurrent threads (for readwhilewriting: N readers + 1 writer).
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Block cache size in bytes (RocksDB compatible). Default: 64MB.
    #[arg(long, default_value = "67108864")]
    cache_size: u64,

    /// Compression type: none, lz4, zstd (RocksDB compatible flag name).
    #[arg(long, default_value = "none")]
    compression_type: Compression,

    /// Bypass fjall journal (WAL). Default: false (WAL enabled, matching RocksDB default).
    #[arg(long)]
    disable_wal: bool,

    /// [NOT YET WIRED] fsync per write (matches RocksDB --sync flag).
    /// Parsed for CLI compatibility but currently has no effect.
    #[arg(long)]
    sync: bool,

    /// Number of keyspaces for multi_keyspace workload.
    #[arg(long, default_value = "10")]
    keyspaces: usize,

    /// Output results as JSON (single workload only).
    #[arg(long)]
    json: bool,

    /// Output results in github-action-benchmark format (customBiggerIsBetter).
    #[arg(long)]
    github_json: bool,

    /// Print latency histogram percentiles after each benchmark.
    #[arg(long)]
    histogram: bool,

    /// Database directory path. If not set, a temporary directory is used.
    /// When set, the directory is reused across benchmarks without reset
    /// (similar to RocksDB's --use_existing_db behavior).
    #[arg(long)]
    db: Option<PathBuf>,
}

fn parse_benchmarks(s: &str) -> Result<String, String> {
    if s == "all" {
        return Ok(s.to_string());
    }
    let available = available_benchmarks();
    for name in s.split(',') {
        let name = name.trim();
        if !available.contains(&name) {
            return Err(format!(
                "unknown benchmark '{}'. Available: all, {}",
                name,
                available.join(", ")
            ));
        }
    }
    Ok(s.to_string())
}

fn main() {
    let cli = Cli::parse();

    let bench_config = BenchConfig {
        num: cli.num,
        key_size: cli.key_size,
        value_size: cli.value_size,
        threads: cli.threads,
        cache_size: cli.cache_size,
        compression_type: cli.compression_type,
        disable_wal: cli.disable_wal,
        sync: cli.sync,
        keyspaces: cli.keyspaces,
    };

    if bench_config.num == 0 {
        eprintln!("Error: --num must be > 0");
        std::process::exit(1);
    }

    if bench_config.key_size == 0 {
        eprintln!("Error: --key_size must be > 0");
        std::process::exit(1);
    }

    if cli.threads == 0 {
        eprintln!("Error: --threads must be >= 1");
        std::process::exit(1);
    }

    if cli.json && cli.github_json {
        eprintln!("Error: --json and --github_json are mutually exclusive");
        std::process::exit(1);
    }

    if cli.json && (cli.benchmarks.contains(',') || cli.benchmarks == "all") {
        eprintln!("Error: --json does not support multiple benchmarks; use --github_json");
        std::process::exit(1);
    }

    // Warn if key space is smaller than num ops.
    if bench_config.key_size < 8 {
        let max_keys = 1u64 << (bench_config.key_size * 8);
        if bench_config.num > max_keys {
            eprintln!(
                "Warning: --key_size {} supports only {} distinct keys, \
                 but --num {} was requested. Keys will repeat (overwrites).",
                bench_config.key_size, max_keys, bench_config.num,
            );
        }
    }

    // Print RocksDB-style header.
    let wal_status = if cli.disable_wal { "OFF" } else { "ON" };
    let sync_status = if cli.sync { "ON" } else { "OFF" };
    eprintln!(
        "Keys:       {} bytes each\n\
         Values:     {} bytes each\n\
         Entries:    {}\n\
         RawSize:    {:.1} MB (estimated)\n\
         Compression: {}\n\
         WAL:        {}\n\
         Sync:       {}\n\
         Threads:    {}\n\
         ------------------------------------------------",
        cli.key_size,
        cli.value_size,
        cli.num,
        cli.num as f64 * (cli.key_size as f64 + cli.value_size as f64) / (1024.0 * 1024.0),
        cli.compression_type,
        wal_status,
        sync_status,
        cli.threads,
    );

    let benchmarks: Vec<&str> = if cli.benchmarks == "all" {
        available_benchmarks().to_vec()
    } else {
        cli.benchmarks.split(',').map(|s| s.trim()).collect()
    };

    let mut github_entries: Vec<serde_json::Value> = Vec::new();
    let mut failures = 0u32;

    for benchmark_name in &benchmarks {
        if let Err(e) = run_single(benchmark_name, &bench_config, &cli, &mut github_entries) {
            eprintln!("Error: {benchmark_name} failed: {e}");
            failures += 1;
        }
    }

    if cli.github_json {
        let array = serde_json::Value::Array(github_entries);
        match serde_json::to_string_pretty(&array) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error: failed to serialize GitHub JSON: {e}");
                failures += 1;
            }
        }
    }

    if failures > 0 {
        eprintln!("{failures} benchmark(s) failed");
        std::process::exit(1);
    }
}

fn run_single(
    benchmark_name: &str,
    bench_config: &BenchConfig,
    cli: &Cli,
    github_entries: &mut Vec<serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _tmpdir;
    let db_path = match &cli.db {
        Some(p) => p.clone(),
        None => {
            _tmpdir = tempfile::tempdir()?;
            _tmpdir.path().to_path_buf()
        }
    };

    let wal_status = if cli.disable_wal { "OFF" } else { "ON" };
    let sync_status = if cli.sync { "ON" } else { "OFF" };
    eprintln!("=== db_bench: {benchmark_name} ===");
    eprintln!(
        "num={} key_size={} value_size={} threads={} cache_size={} wal={} sync={}",
        cli.num, cli.key_size, cli.value_size, cli.threads, cli.cache_size, wal_status, sync_status,
    );

    let (db, keyspace) = config::create_db(&db_path, bench_config)?;
    let mut reporter = Reporter::new();

    let workload = create_workload(benchmark_name)
        .ok_or_else(|| format!("unknown benchmark '{benchmark_name}'"))?;

    workload.run(&db, &keyspace, bench_config, &mut reporter)?;

    // Recovery measures reopen latency, not data throughput — suppress MB/s.
    let entry_size = if benchmark_name == "recovery" {
        0
    } else {
        bench_config.entry_size()
    };

    if cli.github_json {
        let s = reporter.summary(entry_size);
        // Skip emitting a datapoint for workloads that were skipped (0 ops).
        if s.ops == 0 {
            return Ok(());
        }
        let unit = if benchmark_name == "recovery" {
            "reopens/sec"
        } else {
            "ops/sec"
        };
        github_entries.push(serde_json::json!({
            "name": benchmark_name,
            "value": s.ops_per_sec,
            "unit": unit,
            "extra": format!(
                "P50: {:.1}us | P99: {:.1}us | P99.9: {:.1}us\nthreads: {} | elapsed: {:.2}s | num: {} | wal: {}",
                s.p50, s.p99, s.p999, cli.threads, s.secs, cli.num, wal_status,
            ),
        }));
    } else if cli.json {
        let json_config = JsonConfig {
            num: cli.num,
            key_size: cli.key_size,
            value_size: cli.value_size,
            entry_size,
            threads: cli.threads,
            compression: cli.compression_type.to_string(),
            wal: !cli.disable_wal,
        };
        println!("{}", reporter.to_json(benchmark_name, &json_config));
    } else {
        let s = reporter.summary(entry_size);
        if s.ops == 0 {
            eprintln!(
                "workload '{benchmark_name}' recorded 0 operations — skipping throughput summary"
            );
        } else {
            reporter.print_human(benchmark_name, entry_size);
            if cli.histogram {
                reporter.print_histogram();
            }
        }
    }

    Ok(())
}
