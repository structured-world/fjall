use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, Instant};

/// Derived metrics from a benchmark run.
pub struct Summary {
    pub secs: f64,
    pub ops: u64,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
    pub p50: f64,
    pub p99: f64,
    pub p999: f64,
    pub p9999: f64,
}

/// Collects per-operation latencies and computes summary statistics.
pub struct Reporter {
    histogram: Histogram<u64>,
    start: Option<Instant>,
    elapsed: Duration,
    ops_counted: u64,
}

impl Reporter {
    pub fn new() -> Self {
        Self {
            // Record up to 10 seconds (10_000_000_000 ns) with 3 significant digits.
            #[expect(clippy::expect_used, reason = "constant histogram params")]
            histogram: Histogram::new_with_max(10_000_000_000, 3)
                .expect("failed to create histogram"),
            start: None,
            elapsed: Duration::ZERO,
            ops_counted: 0,
        }
    }

    /// Start the measurement timer, resetting all prior state.
    pub fn start(&mut self) {
        self.histogram.reset();
        self.elapsed = Duration::ZERO;
        self.ops_counted = 0;
        self.start = Some(Instant::now());
    }

    /// Record a single operation's latency in nanoseconds.
    #[expect(
        clippy::expect_used,
        reason = "Histogram::record can only fail for out-of-range values, which we clamp"
    )]
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        let clamped = nanos.min(self.histogram.high());
        self.histogram
            .record(clamped)
            .expect("failed to record latency in histogram");
        self.ops_counted += 1;
    }

    /// Record a [`Duration`] as nanoseconds, saturating at u64::MAX.
    #[inline]
    pub fn record_duration(&mut self, d: Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.record(nanos);
    }

    /// Stop the measurement timer.
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.elapsed = start.elapsed();
        }
    }

    /// Merge another reporter's histogram into this one.
    #[expect(
        clippy::expect_used,
        reason = "Histogram::add can only fail with incompatible configurations"
    )]
    pub fn merge(&mut self, other: &Reporter) {
        self.histogram
            .add(&other.histogram)
            .expect("failed to merge histograms: incompatible configurations");
        self.ops_counted += other.ops_counted;
    }

    /// Compute derived metrics from raw histogram + elapsed time.
    pub fn summary(&self, entry_size: usize) -> Summary {
        let secs = self.elapsed.as_secs_f64();
        let ops = self.ops_counted;
        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        let mb_per_sec = ops_per_sec * entry_size as f64 / (1024.0 * 1024.0);
        Summary {
            secs,
            ops,
            ops_per_sec,
            mb_per_sec,
            p50: self.percentile_us(50.0),
            p99: self.percentile_us(99.0),
            p999: self.percentile_us(99.9),
            p9999: self.percentile_us(99.99),
        }
    }

    /// Print human-readable results in RocksDB db_bench format.
    pub fn print_human(&self, benchmark: &str, entry_size: usize) {
        let s = self.summary(entry_size);
        let micros_per_op = if s.ops > 0 {
            s.secs * 1_000_000.0 / s.ops as f64
        } else {
            0.0
        };
        println!(
            "{benchmark:<20} : {micros_per_op:>11.3} micros/op {ops_sec:>10.0} ops/sec {secs:.3} seconds {ops} operations;{mb}",
            ops_sec = s.ops_per_sec,
            secs = s.secs,
            ops = s.ops,
            mb = if entry_size > 0 {
                format!(" {:.1} MB/s", s.mb_per_sec)
            } else {
                String::new()
            },
        );
    }

    /// Print latency histogram percentiles.
    pub fn print_histogram(&self) {
        let s = self.summary(0);
        println!(
            "  Percentiles: P50: {:.1}us  P99: {:.1}us  P99.9: {:.1}us  P99.99: {:.1}us",
            s.p50, s.p99, s.p999, s.p9999,
        );
    }

    /// Produce JSON output.
    pub fn to_json(&self, benchmark: &str, config: &JsonConfig) -> String {
        let s = self.summary(config.entry_size);

        let report = JsonReport {
            benchmark: benchmark.to_string(),
            config: config.clone(),
            elapsed_secs: s.secs,
            ops_total: s.ops,
            ops_per_sec: s.ops_per_sec,
            mb_per_sec: s.mb_per_sec,
            latency_us: LatencyUs {
                p50: s.p50,
                p99: s.p99,
                p999: s.p999,
                p9999: s.p9999,
            },
        };

        #[expect(clippy::expect_used, reason = "fixed struct serialization")]
        serde_json::to_string_pretty(&report).expect("failed to serialize JSON")
    }

    fn percentile_us(&self, p: f64) -> f64 {
        self.histogram.value_at_percentile(p) as f64 / 1000.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonConfig {
    pub num: u64,
    pub key_size: usize,
    pub value_size: usize,
    pub entry_size: usize,
    pub threads: usize,
    pub compression: String,
    pub wal: bool,
}

#[derive(Serialize)]
struct JsonReport {
    benchmark: String,
    config: JsonConfig,
    elapsed_secs: f64,
    ops_total: u64,
    ops_per_sec: f64,
    mb_per_sec: f64,
    latency_us: LatencyUs,
}

#[derive(Serialize)]
struct LatencyUs {
    p50: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
}
