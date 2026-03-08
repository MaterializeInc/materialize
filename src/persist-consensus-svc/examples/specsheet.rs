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
//! cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
//!     --workload examples/scenarios/small-smoke.yaml
//!
//! cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
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
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Server;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::consensus_service_client::ConsensusServiceClient;
use mz_persist::generated::consensus_service::consensus_service_server::ConsensusServiceServer;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoScanRequest, ProtoTruncateRequest,
    ProtoVersionedData,
};
use mz_persist_consensus_svc::actor::{Actor, ActorCommand, ActorConfig};
use mz_persist_consensus_svc::metrics::ConsensusMetrics;
use mz_persist_consensus_svc::service::ConsensusGrpcService;
use mz_persist_consensus_svc::wal::{LatencyProfile, LatencyWalWriter};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Simulated WAL latency profile. Each variant is a named preset with baked-in
/// latency parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum WalLatency {
    /// Instant flushes. Isolates actor CPU cost.
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

impl WalLatency {
    fn to_latency_profile(self) -> LatencyProfile {
        match self {
            WalLatency::Zero => LatencyProfile::Zero,
            WalLatency::Fixed5ms => LatencyProfile::Fixed(Duration::from_millis(5)),
            WalLatency::Fixed10ms => LatencyProfile::Fixed(Duration::from_millis(10)),
            WalLatency::S3Express => LatencyProfile::P50P99 {
                p50: Duration::from_millis(5),
                p99: Duration::from_millis(50),
            },
            WalLatency::S3Standard => LatencyProfile::P50P99 {
                p50: Duration::from_millis(20),
                p99: Duration::from_millis(500),
            },
        }
    }
}

impl std::fmt::Display for WalLatency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalLatency::Zero => write!(f, "zero"),
            WalLatency::Fixed5ms => write!(f, "fixed-5ms"),
            WalLatency::Fixed10ms => write!(f, "fixed-10ms"),
            WalLatency::S3Express => write!(f, "s3-express"),
            WalLatency::S3Standard => write!(f, "s3-standard"),
        }
    }
}

/// Transport mode for driving the workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum TransportMode {
    /// Direct mpsc channel to the actor (no serialization overhead).
    Direct,
    /// gRPC client through a loopback tonic server (full stack).
    Grpc,
}

impl std::fmt::Display for TransportMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportMode::Direct => write!(f, "direct"),
            TransportMode::Grpc => write!(f, "grpc (loopback)"),
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
    flush_interval_ms: Option<u64>,

    /// WAL latency profile.
    #[arg(long, value_enum)]
    wal_latency: Option<WalLatency>,

    /// Transport mode: direct (actor channel) or grpc (loopback server).
    #[arg(long, value_enum)]
    transport: Option<TransportMode>,

    #[arg(long)]
    duration: Option<u64>,
    #[arg(long)]
    warmup: Option<u64>,

    /// Emit JSON output instead of human-readable text.
    #[arg(long)]
    json: bool,
}

/// Workload configuration. Deserializable directly from YAML with defaults
/// for every field, so a YAML file only needs to specify what it overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct WorkloadConfig {
    name: String,
    description: String,
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
    flush_interval_ms: u64,
    wal_latency: WalLatency,
    transport: TransportMode,
    duration: u64,
    warmup: u64,
    // Not in YAML — CLI-only flag.
    #[serde(skip)]
    json: bool,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            name: "default".to_string(),
            description: "Default workload".to_string(),
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
            flush_interval_ms: 5,
            wal_latency: WalLatency::Zero,
            transport: TransportMode::Direct,
            duration: 60,
            warmup: 10,
            json: false,
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
                serde_yaml::from_str(&contents)
                    .unwrap_or_else(|e| panic!("failed to parse workload file {}: {}", path, e))
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
            flush_interval_ms,
            wal_latency,
            transport,
            duration,
            warmup,
        );
        cfg.json = cli.json;

        cfg
    }

    fn validate(&self) {
        let total = self.cas_pct + self.scan_pct + self.head_pct + self.truncate_pct;
        assert_eq!(total, 100, "operation mix must sum to 100, got {}", total);
        assert!(self.num_shards > 0, "num_shards must be > 0");
        assert!(self.writers_per_shard > 0, "writers_per_shard must be > 0");
        assert!(
            self.write_rate_per_second > 0.0,
            "write_rate_per_second must be > 0"
        );
        assert!(self.duration > 0, "duration must be > 0");
        assert!(
            self.truncate_to < self.max_entries,
            "truncate_to ({}) must be < max_entries ({})",
            self.truncate_to,
            self.max_entries,
        );
    }

    fn latency_profile(&self) -> LatencyProfile {
        self.wal_latency.to_latency_profile()
    }
}

// ---------------------------------------------------------------------------
// Transport abstraction (direct actor channel vs gRPC)
// ---------------------------------------------------------------------------

/// Transport layer for talking to the consensus service. Each task gets its
/// own clone — the gRPC client is cheap to clone (shared HTTP/2 connection).
#[derive(Clone)]
enum Transport {
    /// Direct mpsc channel to the actor (no serialization overhead).
    Direct(mpsc::Sender<ActorCommand>),
    /// gRPC client connected to a loopback tonic server.
    Grpc(ConsensusServiceClient<tonic::transport::Channel>),
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
            Transport::Direct(tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if tx
                    .send(ActorCommand::CompareAndSet {
                        key: key.to_string(),
                        expected,
                        new: ProtoVersionedData { seqno, data },
                        reply: reply_tx,
                    })
                    .await
                    .is_err()
                {
                    return CasResult::Shutdown;
                }
                match reply_rx.await {
                    Ok(Ok(resp)) if resp.committed => CasResult::Committed,
                    Ok(Ok(_)) => CasResult::Rejected,
                    _ => CasResult::Shutdown,
                }
            }
            Transport::Grpc(client) => {
                match client
                    .compare_and_set(ProtoCompareAndSetRequest {
                        key: key.to_string(),
                        expected,
                        new: Some(ProtoVersionedData { seqno, data }),
                    })
                    .await
                {
                    Ok(r) => {
                        if r.into_inner().committed {
                            CasResult::Committed
                        } else {
                            CasResult::Rejected
                        }
                    }
                    Err(_) => CasResult::Shutdown,
                }
            }
        }
    }

    async fn head(&mut self, key: &str) -> Option<(u64, usize)> {
        match self {
            Transport::Direct(tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                tx.send(ActorCommand::Head {
                    key: key.to_string(),
                    reply: reply_tx,
                })
                .await
                .ok()?;
                let resp = reply_rx.await.ok()?.ok()?;
                resp.data.map(|d| (d.seqno, d.data.len()))
            }
            Transport::Grpc(client) => {
                let resp = client
                    .head(ProtoHeadRequest {
                        key: key.to_string(),
                    })
                    .await
                    .ok()?;
                resp.into_inner().data.map(|d| (d.seqno, d.data.len()))
            }
        }
    }

    async fn scan(&mut self, key: &str, from: u64, limit: u64) -> bool {
        match self {
            Transport::Direct(tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if tx
                    .send(ActorCommand::Scan {
                        key: key.to_string(),
                        from,
                        limit,
                        reply: reply_tx,
                    })
                    .await
                    .is_err()
                {
                    return false;
                }
                reply_rx.await.is_ok()
            }
            Transport::Grpc(client) => client
                .scan(ProtoScanRequest {
                    key: key.to_string(),
                    from,
                    limit,
                })
                .await
                .is_ok(),
        }
    }

    async fn truncate(&mut self, key: &str, seqno: u64) -> bool {
        match self {
            Transport::Direct(tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if tx
                    .send(ActorCommand::Truncate {
                        key: key.to_string(),
                        seqno,
                        reply: reply_tx,
                    })
                    .await
                    .is_err()
                {
                    return false;
                }
                reply_rx.await.is_ok()
            }
            Transport::Grpc(client) => client
                .truncate(ProtoTruncateRequest {
                    key: key.to_string(),
                    seqno,
                })
                .await
                .is_ok(),
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
        let elapsed = start.elapsed();
        if let Some(remaining) = sleep_dur.checked_sub(elapsed) {
            tokio::time::sleep(remaining).await;
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
    metrics: &ConsensusMetrics,
    elapsed: Duration,
) {
    let secs = elapsed.as_secs_f64().max(0.001);
    let flush_count = metrics.flush_count.get();
    let total_ops: u64 = ops.iter().map(|o| o.stats.count).sum();
    let wal_bytes = metrics.s3_wal_write_bytes.get();

    // --- Overview ---
    println!();
    println!("=== Consensus Service Spec Sheet ===");
    let target_writes_per_sec =
        f64::cast_lossy(cfg.num_shards) * f64::cast_lossy(cfg.writers_per_shard) * cfg.write_rate_per_second;
    println!(
        "Scenario: {} ({} shards, {} writers/shard, {} values, {} target writes/s)",
        cfg.name,
        format_count(cfg.num_shards),
        cfg.writers_per_shard,
        format_bytes(i64::try_from(cfg.value_size).expect("value_size fits in i64")),
        format_count(u64::cast_lossy(target_writes_per_sec)),
    );
    println!(
        "Transport: {}, WAL latency: {}",
        cfg.transport, cfg.wal_latency
    );
    println!(
        "Duration: {}s ({}s warmup), flush interval: {}ms",
        cfg.duration, cfg.warmup, cfg.flush_interval_ms,
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
        f64::cast_lossy(wal_bytes) / f64::cast_lossy(flush_count)
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

    // Total actual includes rejections (real load on actor).
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
        "Total (actor)",
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

    // --- Actor State ---
    println!();
    println!("--- Actor State (includes warmup) ---");
    println!(
        "  Shards:       {:>10}",
        format_count(u64::try_from(metrics.active_shards.get()).expect("non-negative gauge"))
    );
    println!(
        "  Entries:      {:>10}",
        format_count(u64::try_from(metrics.total_entries.get()).expect("non-negative gauge"))
    );
    println!(
        "  Memory:       {:>10}",
        format_bytes(metrics.approx_bytes.get())
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
    metrics: &ConsensusMetrics,
    elapsed: Duration,
) {
    let secs = elapsed.as_secs_f64().max(0.001);
    let total_ops: u64 = ops.iter().map(|o| o.stats.count).sum();
    let flush_count = metrics.flush_count.get();
    let wal_bytes = metrics.s3_wal_write_bytes.get();

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
        u64::cast_lossy((f64::cast_lossy(wal_bytes) / f64::cast_lossy(flush_count)).round())
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

    let result = serde_json::json!({
        "scenario": cfg.name,
        "config": {
            "num_shards": cfg.num_shards,
            "writers_per_shard": cfg.writers_per_shard,
            "value_size": cfg.value_size,
            "duration": cfg.duration,
            "warmup": cfg.warmup,
            "flush_interval_ms": cfg.flush_interval_ms,
            "wal_latency": cfg.wal_latency.to_string(),
            "transport": cfg.transport.to_string(),
        },
        "latencies": latencies,
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
        "actor_state": {
            "active_shards": metrics.active_shards.get(),
            "total_entries": metrics.total_entries.get(),
            "approx_bytes": metrics.approx_bytes.get(),
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

    let target_writes_per_sec =
        f64::cast_lossy(cfg.num_shards) * f64::cast_lossy(cfg.writers_per_shard) * cfg.write_rate_per_second;
    if !json {
        eprintln!(
            "specsheet: {} shards, {} writers/shard, {} values, {} target writes/s, {}s run + {}s warmup, transport: {}",
            cfg.num_shards,
            cfg.writers_per_shard,
            format_bytes(i64::try_from(cfg.value_size).expect("value_size fits in i64")),
            format_count(u64::cast_lossy(target_writes_per_sec)),
            cfg.duration,
            cfg.warmup,
            cfg.transport,
        );
    }

    // --- Create actor ---
    let registry = MetricsRegistry::new();
    let metrics = ConsensusMetrics::register(&registry);
    let metrics_for_report = metrics.clone();

    let config = ActorConfig {
        queue_depth: (usize::cast_from(cfg.num_shards * cfg.writers_per_shard) + 1024).max(4096),
        flush_interval_ms: cfg.flush_interval_ms,
        snapshot_interval: u64::MAX,
    };
    let latency_profile = cfg.latency_profile();
    let wal = LatencyWalWriter::new(latency_profile);
    let (actor, handle) = Actor::new(config, wal, metrics);
    let tx = handle.sender().clone();
    let actor_handle = mz_ore::task::spawn(|| "specsheet-actor", actor.run());

    // --- Create transport ---
    let grpc_server_handle = if cfg.transport == TransportMode::Grpc {
        let grpc_service = ConsensusGrpcService {
            handle: handle.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind loopback listener");
        let addr = listener.local_addr().unwrap();
        let handle = mz_ore::task::spawn(|| "specsheet-grpc-server", async move {
            Server::builder()
                .add_service(ConsensusServiceServer::new(grpc_service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        })
        .abort_on_drop();
        tokio::time::sleep(Duration::from_millis(50)).await;
        Some((addr, handle))
    } else {
        None
    };

    let transport = if let Some((addr, _)) = &grpc_server_handle {
        let client = ConsensusServiceClient::connect(format!("http://{}", addr))
            .await
            .expect("failed to connect gRPC client");
        Transport::Grpc(client)
    } else {
        Transport::Direct(tx.clone())
    };

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

    let num_clients = cfg.num_shards * cfg.writers_per_shard;
    let mut client_handles = Vec::new();
    for (seed, (shard_idx, _writer_idx)) in (0..cfg.num_shards)
        .flat_map(|s| (0..cfg.writers_per_shard).map(move |w| (s, w)))
        .enumerate()
    {
        let t = transport.clone();
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
        };
        client_handles.push(mz_ore::task::spawn(|| "specsheet-client", async move {
            client_task(t, client_cfg, stop, warmup_end).await
        }));
    }

    // Drop our sender clones so the actor shuts down when tasks finish.
    drop(tx);
    drop(transport);

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

    // Shut down the gRPC server (which holds a tx clone) so the actor can
    // drain and exit. Without this, the actor blocks forever waiting for
    // the server's sender to drop. Dropping the AbortOnDropHandle aborts
    // the task.
    if let Some((_, handle)) = grpc_server_handle {
        drop(handle);
    }

    let _ = actor_handle.await;

    let measurement_elapsed = Duration::from_secs(cfg.duration);
    let metrics = metrics_for_report;

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
            &metrics,
            measurement_elapsed,
        );
    } else {
        print_report(
            &cfg,
            &op_stats,
            &cas_rejected_stats,
            &client_dist,
            &metrics,
            measurement_elapsed,
        );
    }
}
