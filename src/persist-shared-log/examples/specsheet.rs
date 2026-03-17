// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configurable workload simulator for the consensus service.
//!
//! Spawns `num_shards × writers_per_shard` client tasks, each bound to one
//! shard, doing the full operation mix (CAS/scan/head/truncate) at the
//! configured rate. Reports latency percentiles, throughput, and flush
//! efficiency. See `examples/README.md` for full documentation.
//!
//! ```text
//! cargo run --release -p mz-persist-shared-log --example specsheet -- \
//!     --workload examples/scenarios/small-smoke.yaml
//!
//! cargo run --release -p mz-persist-shared-log --example specsheet -- \
//!     --workload examples/scenarios/production-10k.yaml --transport grpc
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use mz_ore::cast::{CastFrom, CastLossy};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::{
    ProtoCasProposal, ProtoLogProposal, ProtoTruncateProposal, proto_log_proposal,
};
use mz_persist_client::ShardId;
use mz_persist_shared_log::LatencyProfile;
use mz_persist_shared_log::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_shared_log::persist_log::acceptor::PersistAcceptor;
use mz_persist_shared_log::persist_log::learner::{PersistLearner, PersistLearnerConfig};
use mz_persist_shared_log::traits::Acceptor as _;
use mz_persist_shared_log::traits::AcceptorConfig;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Simulated blob latency profile. Each variant is a named preset with baked-in
/// latency parameters. Injected via LatencyBlob around the in-memory persist
/// blob to model real storage backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum BlobLatency {
    /// Instant operations. Isolates CPU cost.
    Zero,
    /// Every flush takes exactly 5ms.
    #[serde(rename = "fixed-5ms")]
    #[value(name = "fixed-5ms")]
    Fixed5ms,
    /// Every flush takes exactly 10ms.
    #[serde(rename = "fixed-10ms")]
    #[value(name = "fixed-10ms")]
    Fixed10ms,
    /// Models S3 Express One Zone.
    #[serde(rename = "s3-express")]
    #[value(name = "s3-express")]
    S3Express,
    /// Models S3 Standard.
    #[serde(rename = "s3-standard")]
    #[value(name = "s3-standard")]
    S3Standard,
}

impl BlobLatency {
    fn to_latency_profile(self) -> LatencyProfile {
        match self {
            BlobLatency::Zero => LatencyProfile::Zero,
            BlobLatency::Fixed5ms => LatencyProfile::Fixed(Duration::from_millis(5)),
            BlobLatency::Fixed10ms => LatencyProfile::Fixed(Duration::from_millis(10)),
            BlobLatency::S3Express => LatencyProfile::P50P99 {
                p50: Duration::from_millis(5),
                p99: Duration::from_millis(50),
            },
            BlobLatency::S3Standard => LatencyProfile::P50P99 {
                p50: Duration::from_millis(20),
                p99: Duration::from_millis(500),
            },
        }
    }
}

impl std::fmt::Display for BlobLatency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobLatency::Zero => write!(f, "zero"),
            BlobLatency::Fixed5ms => write!(f, "fixed-5ms"),
            BlobLatency::Fixed10ms => write!(f, "fixed-10ms"),
            BlobLatency::S3Express => write!(f, "s3-express"),
            BlobLatency::S3Standard => write!(f, "s3-standard"),
        }
    }
}

/// CLI arguments. `Option<T>` fields overlay YAML/default values when set.
#[derive(Parser)]
#[command(name = "specsheet", about = "Consensus service workload simulator")]
struct Cli {
    /// Path to a YAML workload file.
    #[arg(long)]
    workload: Option<String>,

    #[arg(long)]
    num_shards: Option<u64>,
    #[arg(long)]
    writers_per_shard: Option<u64>,
    #[arg(long)]
    value_size: Option<usize>,
    #[arg(long)]
    max_entries: Option<u64>,

    #[arg(long)]
    cas_pct: Option<u64>,
    #[arg(long)]
    scan_pct: Option<u64>,
    #[arg(long)]
    head_pct: Option<u64>,
    #[arg(long)]
    truncate_pct: Option<u64>,

    #[arg(long)]
    scan_limit: Option<u64>,
    #[arg(long)]
    truncate_to: Option<u64>,

    #[arg(long)]
    write_rate_per_second: Option<f64>,

    #[arg(long)]
    queue_depth: Option<usize>,

    /// Blob latency profile (simulated storage latency).
    #[arg(long, value_enum)]
    blob_latency: Option<BlobLatency>,

    #[arg(long)]
    duration: Option<u64>,
    #[arg(long)]
    warmup: Option<u64>,

    #[arg(long)]
    grpc_connections: Option<u64>,

    /// Emit JSON output instead of human-readable text.
    #[arg(long)]
    json: bool,

    /// Open-loop mode: clients fire as fast as possible with no inter-op sleep.
    /// Use to find the saturation point (max throughput before latency degrades).
    #[arg(long)]
    open_loop: bool,
}

/// Workload configuration. Deserializable directly from YAML with defaults
/// for every field, so a YAML file only needs to specify what it overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct WorkloadConfig {
    /// Derived from the YAML filename, not deserialized.
    #[serde(skip)]
    name: String,
    num_shards: u64,
    writers_per_shard: u64,
    value_size: usize,
    max_entries: u64,
    cas_pct: u64,
    scan_pct: u64,
    head_pct: u64,
    truncate_pct: u64,
    scan_limit: u64,
    truncate_to: u64,
    write_rate_per_second: f64,
    queue_depth: usize,
    grpc_connections: u64,
    #[serde(alias = "wal_latency")]
    blob_latency: BlobLatency,
    duration: u64,
    warmup: u64,
    // Not in YAML — CLI-only flags.
    #[serde(skip)]
    json: bool,
    #[serde(skip)]
    open_loop: bool,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            name: "default".to_string(),
            num_shards: 1000,
            writers_per_shard: 2,
            value_size: 64,
            max_entries: 1000,
            cas_pct: 90,
            scan_pct: 8,
            head_pct: 1,
            truncate_pct: 1,
            scan_limit: 10,
            truncate_to: 500,
            write_rate_per_second: 1.0,
            queue_depth: 4096,
            grpc_connections: 1,
            blob_latency: BlobLatency::Zero,
            duration: 60,
            warmup: 10,
            json: false,
            open_loop: false,
        }
    }
}

/// Apply CLI overrides to a config. Only fields explicitly set by the user
/// (Some values) overwrite the YAML/default values.
macro_rules! apply_overrides {
    ($cfg:expr, $cli:expr, $($field:ident),* $(,)?) => {
        $(if let Some(v) = $cli.$field { $cfg.$field = v; })*
    };
}

impl WorkloadConfig {
    fn from_cli(cli: Cli) -> Self {
        // Start from YAML if provided, otherwise defaults.
        let mut cfg = match &cli.workload {
            Some(path) => {
                let contents = std::fs::read_to_string(path)
                    .unwrap_or_else(|e| panic!("failed to read workload file {}: {}", path, e));
                let mut cfg: WorkloadConfig = serde_yaml::from_str(&contents)
                    .unwrap_or_else(|e| panic!("failed to parse workload file {}: {}", path, e));
                cfg.name = std::path::Path::new(path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                cfg
            }
            None => WorkloadConfig::default(),
        };

        // CLI args override YAML/default values.
        apply_overrides!(
            cfg,
            cli,
            num_shards,
            writers_per_shard,
            value_size,
            max_entries,
            cas_pct,
            scan_pct,
            head_pct,
            truncate_pct,
            scan_limit,
            truncate_to,
            write_rate_per_second,
            queue_depth,
            blob_latency,
            duration,
            warmup,
            grpc_connections,
        );
        cfg.json = cli.json;
        cfg.open_loop = cli.open_loop;

        cfg
    }

    fn validate(&self) {
        let total = self.cas_pct + self.scan_pct + self.head_pct + self.truncate_pct;
        assert_eq!(total, 100, "operation mix must sum to 100, got {}", total);
        assert!(self.num_shards > 0, "num_shards must be > 0");
        assert!(self.writers_per_shard > 0, "writers_per_shard must be > 0");
        if !self.open_loop {
            assert!(
                self.write_rate_per_second > 0.0,
                "write_rate_per_second must be > 0"
            );
        }
        assert!(self.duration > 0, "duration must be > 0");
        assert!(
            self.truncate_to < self.max_entries,
            "truncate_to ({}) must be < max_entries ({})",
            self.truncate_to,
            self.max_entries,
        );
    }

    fn latency_profile(&self) -> LatencyProfile {
        self.blob_latency.to_latency_profile()
    }
}

// ---------------------------------------------------------------------------
// Transport abstraction (direct handles vs gRPC)
// ---------------------------------------------------------------------------

/// Transport layer for talking to the consensus service. Each task gets its
/// own clone — the gRPC clients and handles are cheap to clone.
///
/// In the two-tier architecture, writes (CAS, truncate) are two-step:
/// append to the acceptor, then await the result from the learner.
/// Reads (head, scan) go directly to the learner.
#[derive(Clone)]
enum Transport {
    /// Direct handles to the acceptor and learner (no serialization overhead).
    Direct {
        acceptor: mz_persist_shared_log::persist_log::acceptor::PersistAcceptorHandle,
        learner: mz_persist_shared_log::persist_log::learner::PersistLearnerHandle,
    },
}

enum CasResult {
    Committed,
    Rejected,
    Shutdown,
}

impl Transport {
    async fn cas(
        &mut self,
        key: &str,
        expected: Option<u64>,
        seqno: u64,
        data: Vec<u8>,
    ) -> CasResult {
        match self {
            Transport::Direct { acceptor, learner } => {
                let proposal = ProtoLogProposal {
                    op: Some(proto_log_proposal::Op::Cas(ProtoCasProposal {
                        key: key.to_string(),
                        expected,
                        new_seqno: seqno,
                        data,
                    })),
                };
                let receipt = match acceptor.append(proposal).await {
                    Ok(r) => r,
                    Err(_) => return CasResult::Shutdown,
                };
                match learner
                    .await_cas_result(receipt.batch_number, receipt.position)
                    .await
                {
                    Ok(resp) if resp.committed => CasResult::Committed,
                    Ok(_) => CasResult::Rejected,
                    Err(_) => CasResult::Shutdown,
                }
            }
        }
    }

    async fn head(&mut self, key: &str) -> Option<(u64, usize)> {
        match self {
            Transport::Direct { learner, .. } => {
                let resp = learner.head(key.to_string()).await.ok()?;
                resp.data.map(|d| (d.seqno, d.data.len()))
            }
        }
    }

    async fn scan(&mut self, key: &str, from: u64, limit: u64) -> bool {
        match self {
            Transport::Direct { learner, .. } => {
                learner.scan(key.to_string(), from, limit).await.is_ok()
            }
        }
    }

    async fn truncate(&mut self, key: &str, seqno: u64) -> bool {
        match self {
            Transport::Direct { acceptor, learner } => {
                let proposal = ProtoLogProposal {
                    op: Some(proto_log_proposal::Op::Truncate(ProtoTruncateProposal {
                        key: key.to_string(),
                        seqno,
                    })),
                };
                let receipt = match acceptor.append(proposal).await {
                    Ok(r) => r,
                    Err(_) => return false,
                };
                learner
                    .await_truncate_result(receipt.batch_number, receipt.position)
                    .await
                    .is_ok()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Latency recording
// ---------------------------------------------------------------------------

struct LatencyRecorder {
    samples: Vec<Duration>,
    warmup_end: Instant,
}

impl LatencyRecorder {
    fn new(warmup_end: Instant) -> Self {
        LatencyRecorder {
            samples: Vec::new(),
            warmup_end,
        }
    }

    fn record(&mut self, start: Instant) {
        let now = Instant::now();
        if now >= self.warmup_end {
            self.samples.push(now.duration_since(start));
        }
    }
}

#[derive(Default, Clone)]
struct PercentileStats {
    count: u64,
    p50: Duration,
    p90: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

/// Extract approximate percentile stats from a prometheus Histogram.
///
/// Uses bucket boundaries as approximations — values are upper bounds of the
/// bucket the percentile falls into.
fn histogram_stats(h: &prometheus::Histogram) -> PercentileStats {
    use prometheus::core::Metric as _;
    let m = h.metric();
    let hist = m.get_histogram();
    let total = hist.get_sample_count();
    if total == 0 {
        return PercentileStats::default();
    }
    let buckets = hist.get_bucket();
    let percentile = |pct: f64| -> Duration {
        #[allow(clippy::as_conversions)]
        let target = ((pct / 100.0) * f64::cast_lossy(total)).ceil() as u64;
        for b in buckets {
            if b.cumulative_count() >= target {
                let bound = b.upper_bound();
                if bound.is_finite() {
                    return Duration::from_secs_f64(bound);
                }
                break;
            }
        }
        // Fallback: last finite bucket or mean.
        for b in buckets.iter().rev() {
            if b.upper_bound().is_finite() {
                return Duration::from_secs_f64(b.upper_bound());
            }
        }
        Duration::from_secs_f64(hist.get_sample_sum() / f64::cast_lossy(total))
    };
    PercentileStats {
        count: total,
        p50: percentile(50.0),
        p90: percentile(90.0),
        p95: percentile(95.0),
        p99: percentile(99.0),
        max: percentile(100.0),
    }
}

fn compute_percentiles(samples: &mut Vec<Duration>) -> PercentileStats {
    if samples.is_empty() {
        return PercentileStats::default();
    }
    samples.sort();
    let len = samples.len();
    PercentileStats {
        count: u64::cast_from(len),
        p50: samples[len * 50 / 100],
        p90: samples[len * 90 / 100],
        p95: samples[len * 95 / 100],
        p99: samples[len.saturating_sub(1).min(len * 99 / 100)],
        max: samples[len - 1],
    }
}

// ---------------------------------------------------------------------------
// Client task — each client is bound to one shard and does a mix of ops
// ---------------------------------------------------------------------------

struct ClientResult {
    cas_committed: LatencyRecorder,
    cas_rejected: LatencyRecorder,
    head: LatencyRecorder,
    scan: LatencyRecorder,
    truncate: LatencyRecorder,
    /// Total ops during measurement period (after warmup).
    measured_ops: u64,
    /// How long this client was active during the measurement period.
    measured_duration: Duration,
}

struct ClientConfig {
    shard_key: String,
    value_size: usize,
    op_rate: f64,
    cas_pct: u64,
    scan_pct: u64,
    head_pct: u64,
    scan_limit: u64,
    truncate_to: u64,
    seed: u64,
    open_loop: bool,
}

async fn client_task(
    mut transport: Transport,
    cfg: ClientConfig,
    stop: Arc<AtomicBool>,
    warmup_end: Instant,
) -> ClientResult {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let data_template: Vec<u8> = (0..cfg.value_size).map(|_| rng.r#gen()).collect();
    let sleep_dur = Duration::from_secs_f64(1.0 / cfg.op_rate);

    // Jitter start time to avoid phase-locking all clients.
    // Each client delays by a random fraction of the sleep interval.
    let jitter = Duration::from_secs_f64(rng.r#gen::<f64>() * sleep_dur.as_secs_f64());
    tokio::time::sleep(jitter).await;

    // CAS state for this shard.
    let mut expected_seqno: Option<u64> = None;
    let mut next_seqno: u64 = 1;

    let mut committed_rec = LatencyRecorder::new(warmup_end);
    let mut rejected_rec = LatencyRecorder::new(warmup_end);
    let mut head_rec = LatencyRecorder::new(warmup_end);
    let mut scan_rec = LatencyRecorder::new(warmup_end);
    let mut truncate_rec = LatencyRecorder::new(warmup_end);
    let mut measured_ops: u64 = 0;
    let mut measure_start: Option<Instant> = None;

    // Cumulative thresholds for operation selection.
    let cas_thresh = cfg.cas_pct;
    let scan_thresh = cas_thresh + cfg.scan_pct;
    let head_thresh = scan_thresh + cfg.head_pct;
    // Anything above head_thresh is truncate.

    while !stop.load(Ordering::Relaxed) {
        let roll: u64 = rng.gen_range(0..100);
        let start = Instant::now();

        if roll < cas_thresh {
            // CAS
            match transport
                .cas(
                    &cfg.shard_key,
                    expected_seqno,
                    next_seqno,
                    data_template.clone(),
                )
                .await
            {
                CasResult::Committed => {
                    committed_rec.record(start);
                    expected_seqno = Some(next_seqno);
                    next_seqno += 1;
                }
                CasResult::Rejected => {
                    rejected_rec.record(start);
                    // Re-sync from head and retry immediately.
                    if let Some((head_seqno, _)) = transport.head(&cfg.shard_key).await {
                        expected_seqno = Some(head_seqno);
                        next_seqno = head_seqno + 1;
                    }
                    continue;
                }
                CasResult::Shutdown => break,
            }
        } else if roll < scan_thresh {
            // Scan
            let _ = transport.scan(&cfg.shard_key, 0, cfg.scan_limit).await;
            scan_rec.record(start);
        } else if roll < head_thresh {
            // Head
            let _ = transport.head(&cfg.shard_key).await;
            head_rec.record(start);
        } else {
            // Truncate — only if shard has enough entries.
            if let Some((head_seqno, _)) = transport.head(&cfg.shard_key).await {
                if head_seqno > cfg.truncate_to {
                    let truncate_seqno = head_seqno.saturating_sub(cfg.truncate_to) + 1;
                    // Record only the truncate call, not the head lookup.
                    let truncate_start = Instant::now();
                    let _ = transport.truncate(&cfg.shard_key, truncate_seqno).await;
                    truncate_rec.record(truncate_start);
                }
            }
        }

        if Instant::now() >= warmup_end {
            if measure_start.is_none() {
                measure_start = Some(Instant::now());
            }
            measured_ops += 1;
        }

        // Sleep for remaining interval so we target start-to-start timing,
        // not end-to-start. If the op took longer than the interval, skip sleep.
        // In open-loop mode, skip sleep entirely — fire as fast as possible.
        if !cfg.open_loop {
            let elapsed = start.elapsed();
            if let Some(remaining) = sleep_dur.checked_sub(elapsed) {
                tokio::time::sleep(remaining).await;
            }
        }
    }

    let measured_duration = measure_start
        .map(|s| Instant::now().duration_since(s))
        .unwrap_or_default();

    ClientResult {
        cas_committed: committed_rec,
        cas_rejected: rejected_rec,
        head: head_rec,
        scan: scan_rec,
        truncate: truncate_rec,
        measured_ops,
        measured_duration,
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!(
            "{},{:03},{:03}",
            n / 1_000_000,
            (n / 1_000) % 1_000,
            n % 1_000
        )
    } else if n >= 1_000 {
        format!("{},{:03}", n / 1_000, n % 1_000)
    } else {
        format!("{}", n)
    }
}

fn format_bytes(b: i64) -> String {
    let b = f64::cast_lossy(b);
    if b >= 1_073_741_824.0 {
        format!("{:.1} GiB", b / 1_073_741_824.0)
    } else if b >= 1_048_576.0 {
        format!("{:.1} MiB", b / 1_048_576.0)
    } else if b >= 1024.0 {
        format!("{:.1} KiB", b / 1024.0)
    } else {
        format!("{:.0} B", b)
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

struct OpStats {
    name: String,
    target_per_sec: f64,
    stats: PercentileStats,
}

struct ClientDistribution {
    num_clients: u64,
    target_ops_per_sec: f64,
    min_ops_per_sec: f64,
    p50_ops_per_sec: f64,
    p90_ops_per_sec: f64,
    max_ops_per_sec: f64,
}

fn print_report(
    cfg: &WorkloadConfig,
    ops: &[OpStats],
    cas_rejected: &PercentileStats,
    client_dist: &ClientDistribution,
    acceptor_metrics: &AcceptorMetrics,
    learner_metrics: &LearnerMetrics,
    elapsed: Duration,
) {
    let secs = elapsed.as_secs_f64().max(0.001);
    let flush_count = acceptor_metrics.flush_count.get();
    let total_ops: u64 = ops.iter().map(|o| o.stats.count).sum();
    let log_bytes = acceptor_metrics.object_store_log_write_bytes.get();

    // --- Overview ---
    println!();
    println!("=== Consensus Service Spec Sheet ===");
    let target_writes_per_sec = f64::cast_lossy(cfg.num_shards)
        * f64::cast_lossy(cfg.writers_per_shard)
        * cfg.write_rate_per_second;
    println!(
        "Scenario: {} ({} shards, {} writers/shard, {} values, {} target writes/s)",
        cfg.name,
        format_count(cfg.num_shards),
        cfg.writers_per_shard,
        format_bytes(i64::try_from(cfg.value_size).expect("value_size fits in i64")),
        format_count(u64::cast_lossy(target_writes_per_sec)),
    );
    println!("Blob latency: {}", cfg.blob_latency);
    println!(
        "Duration: {}s ({}s warmup)",
        cfg.duration, cfg.warmup,
    );
    println!(
        "Op mix: {}% CAS / {}% scan / {}% head / {}% truncate",
        cfg.cas_pct, cfg.scan_pct, cfg.head_pct, cfg.truncate_pct,
    );

    // --- Latencies ---
    println!();
    println!("--- Latencies (ms) ---");
    println!(
        "  {:<16} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "Operation", "count", "p50", "p90", "p95", "p99", "max"
    );
    for op in ops {
        if op.stats.count == 0 {
            continue;
        }
        println!(
            "  {:<16} {:>10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
            op.name,
            format_count(op.stats.count),
            ms(op.stats.p50),
            ms(op.stats.p90),
            ms(op.stats.p95),
            ms(op.stats.p99),
            ms(op.stats.max),
        );
    }
    if cas_rejected.count > 0 {
        println!(
            "  {:<16} {:>10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
            "CAS rejected",
            format_count(cas_rejected.count),
            ms(cas_rejected.p50),
            ms(cas_rejected.p90),
            ms(cas_rejected.p95),
            ms(cas_rejected.p99),
            ms(cas_rejected.max),
        );
    }

    // --- Throughput ---
    let ops_per_sec = f64::cast_lossy(total_ops) / secs;
    let flushes_per_sec = f64::cast_lossy(flush_count) / secs;
    let flush_interval_actual = if flushes_per_sec > 0.0 {
        1000.0 / flushes_per_sec
    } else {
        0.0
    };
    let ops_per_flush = if flush_count > 0 {
        f64::cast_lossy(total_ops) / f64::cast_lossy(flush_count)
    } else {
        0.0
    };
    let bytes_per_flush = if flush_count > 0 {
        f64::cast_lossy(log_bytes) / f64::cast_lossy(flush_count)
    } else {
        0.0
    };

    let cas_committed_per_sec = ops
        .iter()
        .find(|o| o.name == "CAS committed")
        .map(|o| f64::cast_lossy(o.stats.count) / secs)
        .unwrap_or(0.0);
    let cas_rejected_per_sec = f64::cast_lossy(cas_rejected.count) / secs;
    let cas_total_per_sec = cas_committed_per_sec + cas_rejected_per_sec;

    // Total actual includes rejections (real load on service).
    let total_actual_per_sec = ops_per_sec + cas_rejected_per_sec;

    println!();
    println!("--- Throughput ---");
    println!("  {:<20} {:>8}", "ops/s", "");
    println!(
        "    {:<18} {:>8}",
        "CAS",
        format_count(u64::cast_lossy(cas_total_per_sec))
    );
    println!(
        "      {:<16} {:>8}",
        "committed",
        format_count(u64::cast_lossy(cas_committed_per_sec))
    );
    if cas_rejected.count > 0 {
        println!(
            "      {:<16} {:>8}",
            "rejected",
            format_count(u64::cast_lossy(cas_rejected_per_sec))
        );
    }
    for op in ops {
        if op.name == "CAS committed" {
            continue;
        }
        if op.stats.count == 0 && op.target_per_sec < 0.5 {
            continue;
        }
        let actual = u64::cast_lossy(f64::cast_lossy(op.stats.count) / secs);
        println!("    {:<18} {:>8}", op.name, format_count(actual));
    }
    let total_est = u64::cast_lossy(ops.iter().map(|o| o.target_per_sec).sum::<f64>());
    println!(
        "    {:<18} {:>8}",
        "Total (service)",
        format_count(u64::cast_lossy(total_actual_per_sec))
    );
    println!(
        "    {:<18} {:>8}  (est. {})",
        "Total (client)",
        format_count(u64::cast_lossy(ops_per_sec)),
        format_count(total_est)
    );
    println!();
    println!(
        "  {:<18} {:>8.1}  (every {:.1}ms)",
        "Flushes/sec", flushes_per_sec, flush_interval_actual
    );
    println!("  {:<18} {:>8.1}", "Ops/flush", ops_per_flush);
    println!(
        "  {:<18} {:>8}",
        "Bytes/flush",
        format_bytes(i64::cast_lossy(bytes_per_flush))
    );

    // --- Clients ---
    println!();
    println!(
        "--- Clients ({}) ---",
        format_count(client_dist.num_clients)
    );
    println!(
        "  {:<18} {:>8}  {:>8}  {:>8}  {:>8}",
        "ops/s per client", "min", "p50", "p90", "max"
    );
    println!(
        "  {:<18} {:>8.1}  {:>8.1}  {:>8.1}  {:>8.1}",
        "Actual",
        client_dist.min_ops_per_sec,
        client_dist.p50_ops_per_sec,
        client_dist.p90_ops_per_sec,
        client_dist.max_ops_per_sec,
    );
    println!(
        "  {:<18} {:>8.1}",
        "Estimated", client_dist.target_ops_per_sec,
    );

    // --- Server-Side Latencies ---
    // --- Server-Side Latencies (Acceptor) ---
    let acceptor_histograms: Vec<(&str, PercentileStats)> = vec![
        (
            "Queue delay",
            histogram_stats(&acceptor_metrics.proposal_queue_seconds),
        ),
        (
            "Log write",
            histogram_stats(&acceptor_metrics.object_store_log_write_latency_seconds),
        ),
        (
            "Flush (total)",
            histogram_stats(&acceptor_metrics.flush_latency_seconds),
        ),
    ];
    let has_acceptor_data = acceptor_histograms.iter().any(|(_, s)| s.count > 0);
    if has_acceptor_data {
        println!();
        println!("--- Acceptor Latencies (ms, includes warmup) ---");
        println!(
            "  {:<18} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Metric", "count", "p50", "p90", "p95", "p99", "max"
        );
        for (name, stats) in &acceptor_histograms {
            if stats.count == 0 {
                continue;
            }
            println!(
                "  {:<18} {:>10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
                name,
                format_count(stats.count),
                ms(stats.p50),
                ms(stats.p90),
                ms(stats.p95),
                ms(stats.p99),
                ms(stats.max),
            );
        }
    }

    // --- Server-Side Latencies (Learner) ---
    let learner_histograms: Vec<(&str, PercentileStats)> = vec![
        (
            "Cmd queue delay",
            histogram_stats(&learner_metrics.cmd_queue_seconds),
        ),
        (
            "Batch apply",
            histogram_stats(&learner_metrics.batch_materialize_latency_seconds),
        ),
        ("Head", histogram_stats(&learner_metrics.head_seconds)),
        ("Scan", histogram_stats(&learner_metrics.scan_seconds)),
        (
            "CAS result",
            histogram_stats(&learner_metrics.cas_result_seconds),
        ),
        (
            "Truncate result",
            histogram_stats(&learner_metrics.truncate_result_seconds),
        ),
    ];
    let has_learner_data = learner_histograms.iter().any(|(_, s)| s.count > 0);
    if has_learner_data {
        println!();
        println!("--- Learner Latencies (ms, includes warmup) ---");
        println!(
            "  {:<18} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Metric", "count", "p50", "p90", "p95", "p99", "max"
        );
        for (name, stats) in &learner_histograms {
            if stats.count == 0 {
                continue;
            }
            println!(
                "  {:<18} {:>10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
                name,
                format_count(stats.count),
                ms(stats.p50),
                ms(stats.p90),
                ms(stats.p95),
                ms(stats.p99),
                ms(stats.max),
            );
        }
    }

    // --- Service State ---
    println!();
    println!("--- Service State (includes warmup) ---");
    println!(
        "  Shards:       {:>10}",
        format_count(
            u64::try_from(learner_metrics.active_shards.get()).expect("non-negative gauge")
        )
    );
    println!(
        "  Entries:      {:>10}",
        format_count(
            u64::try_from(learner_metrics.total_entries.get()).expect("non-negative gauge")
        )
    );
    println!(
        "  Memory:       {:>10}",
        format_bytes(learner_metrics.approx_bytes.get())
    );
    println!();
}

/// Round to 2 decimal places for clean JSON output.
fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn print_json(
    cfg: &WorkloadConfig,
    ops: &[OpStats],
    cas_rejected: &PercentileStats,
    client_dist: &ClientDistribution,
    acceptor_metrics: &AcceptorMetrics,
    learner_metrics: &LearnerMetrics,
    elapsed: Duration,
) {
    let secs = elapsed.as_secs_f64().max(0.001);
    let total_ops: u64 = ops.iter().map(|o| o.stats.count).sum();
    let flush_count = acceptor_metrics.flush_count.get();
    let log_bytes = acceptor_metrics.object_store_log_write_bytes.get();

    // Only include operations that actually occurred.
    let mut latency_map: serde_json::Map<String, serde_json::Value> = ops
        .iter()
        .filter(|op| op.stats.count > 0)
        .map(|op| {
            (
                op.name.clone(),
                serde_json::json!({
                    "count": op.stats.count,
                    "p50_ms": round2(ms(op.stats.p50)),
                    "p90_ms": round2(ms(op.stats.p90)),
                    "p95_ms": round2(ms(op.stats.p95)),
                    "p99_ms": round2(ms(op.stats.p99)),
                    "max_ms": round2(ms(op.stats.max)),
                }),
            )
        })
        .collect();
    if cas_rejected.count > 0 {
        latency_map.insert(
            "CAS rejected".into(),
            serde_json::json!({
                "count": cas_rejected.count,
                "p50_ms": round2(ms(cas_rejected.p50)),
                "p90_ms": round2(ms(cas_rejected.p90)),
                "p95_ms": round2(ms(cas_rejected.p95)),
                "p99_ms": round2(ms(cas_rejected.p99)),
                "max_ms": round2(ms(cas_rejected.max)),
            }),
        );
    }
    let latencies: serde_json::Value = latency_map.into();

    let ops_per_flush = if flush_count > 0 {
        round2(f64::cast_lossy(total_ops) / f64::cast_lossy(flush_count))
    } else {
        0.0
    };
    let bytes_per_flush = if flush_count > 0 {
        u64::cast_lossy((f64::cast_lossy(log_bytes) / f64::cast_lossy(flush_count)).round())
    } else {
        0
    };
    let flushes_per_sec = f64::cast_lossy(flush_count) / secs;
    let flush_interval_ms = if flushes_per_sec > 0.0 {
        round2(1000.0 / flushes_per_sec)
    } else {
        0.0
    };

    // Per-operation throughput breakdown with target vs actual.
    let ops_breakdown: serde_json::Value = ops
        .iter()
        .filter(|op| op.stats.count > 0 || op.target_per_sec >= 0.5)
        .map(|op| {
            (
                op.name.clone(),
                serde_json::json!({
                    "target": u64::cast_lossy(op.target_per_sec.round()),
                    "actual": u64::cast_lossy((f64::cast_lossy(op.stats.count) / secs).round()),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into();

    // Server-side histogram stats.
    let server_hist = |h: &prometheus::Histogram| -> serde_json::Value {
        let s = histogram_stats(h);
        if s.count == 0 {
            return serde_json::Value::Null;
        }
        serde_json::json!({
            "count": s.count,
            "p50_ms": round2(ms(s.p50)),
            "p90_ms": round2(ms(s.p90)),
            "p95_ms": round2(ms(s.p95)),
            "p99_ms": round2(ms(s.p99)),
            "max_ms": round2(ms(s.max)),
        })
    };

    let result = serde_json::json!({
        "scenario": cfg.name,
        "config": {
            "num_shards": cfg.num_shards,
            "writers_per_shard": cfg.writers_per_shard,
            "value_size": cfg.value_size,
            "duration": cfg.duration,
            "warmup": cfg.warmup,
            "blob_latency": cfg.blob_latency.to_string(),
        },
        "latencies": latencies,
        "acceptor_latencies": {
            "queue_delay": server_hist(&acceptor_metrics.proposal_queue_seconds),
            "log_write": server_hist(&acceptor_metrics.object_store_log_write_latency_seconds),
            "flush_total": server_hist(&acceptor_metrics.flush_latency_seconds),
        },
        "learner_latencies": {
            "cmd_queue_delay": server_hist(&learner_metrics.cmd_queue_seconds),
            "batch_apply": server_hist(&learner_metrics.batch_materialize_latency_seconds),
            "head": server_hist(&learner_metrics.head_seconds),
            "scan": server_hist(&learner_metrics.scan_seconds),
            "cas_result": server_hist(&learner_metrics.cas_result_seconds),
            "truncate_result": server_hist(&learner_metrics.truncate_result_seconds),
        },
        "clients": {
            "num_clients": client_dist.num_clients,
            "target_ops_per_sec": round2(client_dist.target_ops_per_sec),
            "distribution_ops_per_sec": {
                "min": round2(client_dist.min_ops_per_sec),
                "p50": round2(client_dist.p50_ops_per_sec),
                "p90": round2(client_dist.p90_ops_per_sec),
                "max": round2(client_dist.max_ops_per_sec),
            },
        },
        "service_state": {
            "active_shards": learner_metrics.active_shards.get(),
            "total_entries": learner_metrics.total_entries.get(),
            "approx_bytes": learner_metrics.approx_bytes.get(),
        },
        "throughput": {
            "ops_per_sec": u64::cast_lossy((f64::cast_lossy(total_ops) / secs).round()),
            "ops_per_sec_by_type": ops_breakdown,
            "cas_rejections_per_sec": u64::cast_lossy((f64::cast_lossy(cas_rejected.count) / secs).round()),
            "flushes_per_sec": round2(flushes_per_sec),
            "flush_interval_ms": flush_interval_ms,
            "ops_per_flush": ops_per_flush,
            "bytes_per_flush": bytes_per_flush,
        },
    });

    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let json = cli.json;
    let cfg = WorkloadConfig::from_cli(cli);
    cfg.validate();

    let target_writes_per_sec = f64::cast_lossy(cfg.num_shards)
        * f64::cast_lossy(cfg.writers_per_shard)
        * cfg.write_rate_per_second;
    if !json {
        eprintln!(
            "specsheet: {} shards, {} writers/shard, {} values, {} target writes/s, {}s run + {}s warmup",
            cfg.num_shards,
            cfg.writers_per_shard,
            format_bytes(i64::try_from(cfg.value_size).expect("value_size fits in i64")),
            format_count(u64::cast_lossy(target_writes_per_sec)),
            cfg.duration,
            cfg.warmup,
        );
    }

    // --- Metrics ---
    let registry = MetricsRegistry::new();
    let acceptor_metrics = AcceptorMetrics::register(&registry);
    let learner_metrics = LearnerMetrics::register(&registry);
    let acceptor_metrics_for_report = acceptor_metrics.clone();
    let learner_metrics_for_report = learner_metrics.clone();

    let queue_depth = cfg
        .queue_depth
        .max(usize::cast_from(cfg.num_shards * cfg.writers_per_shard) + 1024);
    let num_clients = cfg.num_shards * cfg.writers_per_shard;

    // --- Backend setup ---
    use mz_persist_shared_log::persist_log::client::{self, PersistClientConfig};

    let persist_client_config = PersistClientConfig {
        latency_profile: cfg.latency_profile(),
    };
    let persist_client = client::new_persist_client(persist_client_config);
    let shard_id = ShardId::new();

    let acceptor_config = AcceptorConfig { queue_depth };
    let (acceptor_handle, _acceptor_task) =
        PersistAcceptor::spawn(acceptor_config, &persist_client, shard_id, acceptor_metrics.clone()).await;

    let learner_config = PersistLearnerConfig {
        queue_depth,
        ..Default::default()
    };
    let (learner_handle, _learner_task) =
        PersistLearner::spawn(learner_config, &persist_client, shard_id, learner_metrics.clone()).await;

    let transports = vec![Transport::Direct {
        acceptor: acceptor_handle.clone(),
        learner: learner_handle.clone(),
    }];

    drop(acceptor_handle);
    drop(learner_handle);

    // --- Timing ---
    let warmup_dur = Duration::from_secs(cfg.warmup);
    let total_dur = warmup_dur + Duration::from_secs(cfg.duration);
    let warmup_end = Instant::now() + warmup_dur;
    let stop = Arc::new(AtomicBool::new(false));

    // --- Spawn client tasks ---
    // Each client is bound to one shard and does the full op mix at op_rate.
    // op_rate = write_rate_per_second / (cas_pct / 100) so that the CAS
    // portion of ops hits the target write rate.
    let op_rate = if cfg.cas_pct > 0 {
        cfg.write_rate_per_second / (f64::cast_lossy(cfg.cas_pct) / 100.0)
    } else {
        cfg.write_rate_per_second
    };

    let mut client_handles = Vec::new();
    for (seed, (shard_idx, _writer_idx)) in (0..cfg.num_shards)
        .flat_map(|s| (0..cfg.writers_per_shard).map(move |w| (s, w)))
        .enumerate()
    {
        // Round-robin clients across the transport pool (gRPC connection pool).
        let t = transports[seed % transports.len()].clone();
        let stop = Arc::clone(&stop);
        let client_cfg = ClientConfig {
            shard_key: format!("shard-{:06}", shard_idx),
            value_size: cfg.value_size,
            op_rate,
            cas_pct: cfg.cas_pct,
            scan_pct: cfg.scan_pct,
            head_pct: cfg.head_pct,
            scan_limit: cfg.scan_limit,
            truncate_to: cfg.truncate_to,
            seed: u64::cast_from(seed),
            open_loop: cfg.open_loop,
        };
        client_handles.push(mz_ore::task::spawn(|| "specsheet-client", async move {
            client_task(t, client_cfg, stop, warmup_end).await
        }));
    }

    // Drop transports so the service shuts down when client tasks finish
    // (they hold the remaining clones). Backend-specific handles were already
    // dropped inside the match arms above.
    drop(transports);

    // --- Wait for duration ---
    tokio::time::sleep(total_dur).await;
    stop.store(true, Ordering::Relaxed);

    // --- Collect results ---
    let mut all_committed = Vec::new();
    let mut all_rejected = Vec::new();
    let mut head_samples = Vec::new();
    let mut scan_samples = Vec::new();
    let mut truncate_samples = Vec::new();
    let mut per_client_rates: Vec<f64> = Vec::new();
    for h in client_handles {
        let r = h.await;
        all_committed.extend(r.cas_committed.samples);
        all_rejected.extend(r.cas_rejected.samples);
        head_samples.extend(r.head.samples);
        scan_samples.extend(r.scan.samples);
        truncate_samples.extend(r.truncate.samples);
        // Per-client ops/sec for distribution.
        let client_secs = r.measured_duration.as_secs_f64();
        if client_secs > 0.0 {
            per_client_rates.push(f64::cast_lossy(r.measured_ops) / client_secs);
        }
    }

    // Wait for backend actors to finish.
    let _ = _learner_task.await;
    let _ = _acceptor_task.await;

    let measurement_elapsed = Duration::from_secs(cfg.duration);

    // --- Per-client distribution ---
    per_client_rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let client_dist = if per_client_rates.is_empty() {
        ClientDistribution {
            num_clients,
            target_ops_per_sec: op_rate,
            min_ops_per_sec: 0.0,
            p50_ops_per_sec: 0.0,
            p90_ops_per_sec: 0.0,
            max_ops_per_sec: 0.0,
        }
    } else {
        let len = per_client_rates.len();
        ClientDistribution {
            num_clients,
            target_ops_per_sec: op_rate,
            min_ops_per_sec: per_client_rates[0],
            p50_ops_per_sec: per_client_rates[len * 50 / 100],
            p90_ops_per_sec: per_client_rates[len * 90 / 100],
            max_ops_per_sec: per_client_rates[len - 1],
        }
    };

    // Target ops/sec per operation type (aggregate across all clients).
    let total_target_ops = f64::cast_lossy(num_clients) * op_rate;

    let cas_rejected_stats = compute_percentiles(&mut all_rejected);

    let op_stats = vec![
        OpStats {
            name: "CAS committed".into(),
            target_per_sec: total_target_ops * f64::cast_lossy(cfg.cas_pct) / 100.0,
            stats: compute_percentiles(&mut all_committed),
        },
        OpStats {
            name: "Head".into(),
            target_per_sec: total_target_ops * f64::cast_lossy(cfg.head_pct) / 100.0,
            stats: compute_percentiles(&mut head_samples),
        },
        OpStats {
            name: "Scan".into(),
            target_per_sec: total_target_ops * f64::cast_lossy(cfg.scan_pct) / 100.0,
            stats: compute_percentiles(&mut scan_samples),
        },
        OpStats {
            name: "Truncate".into(),
            target_per_sec: total_target_ops * f64::cast_lossy(cfg.truncate_pct) / 100.0,
            stats: compute_percentiles(&mut truncate_samples),
        },
    ];

    if cfg.json {
        print_json(
            &cfg,
            &op_stats,
            &cas_rejected_stats,
            &client_dist,
            &acceptor_metrics_for_report,
            &learner_metrics_for_report,
            measurement_elapsed,
        );
    } else {
        print_report(
            &cfg,
            &op_stats,
            &cas_rejected_stats,
            &client_dist,
            &acceptor_metrics_for_report,
            &learner_metrics_for_report,
            measurement_elapsed,
        );
    }
}
