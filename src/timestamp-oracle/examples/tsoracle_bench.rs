// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A closed-loop benchmark for timestamp oracle implementations.
//!
//! Drives a [`TimestampOracle`] (by default wrapped in the
//! [`BatchingTimestampOracle`], like the adapter does) against a real backing
//! store and reports client-side throughput and latency percentiles as well as
//! the number of queries that actually hit the backing store. The latter is
//! what "load on the backing store" means for oracle scaling purposes.
//!
//! Modes:
//!
//! - `read`: N concurrent tasks calling `read_ts` in a closed loop, for each
//!   requested concurrency level. Shows how far batching collapses concurrent
//!   reads and where the latency ceiling is.
//! - `write`: a single loop that mimics the oracle traffic of the adapter's
//!   group commit: `peek_write_ts`, `write_ts`, a sleep standing in for the
//!   persist append, `apply_write`, and a trailing `read_ts` standing in for
//!   the read-hold downgrade in `advance_timelines`. Flags allow skipping the
//!   peek and the trailing read to model protocol changes.
//! - `mixed`: both at the same time.
//!
//! Example:
//!
//! ```text
//! cargo run --release -p mz-timestamp-oracle --example tsoracle_bench -- \
//!     --url postgres://postgres@localhost:15432/postgres \
//!     --mode read --concurrency 1,8,64,512 --duration 10
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_repr::Timestamp;
use mz_timestamp_oracle::batching_oracle::BatchingTimestampOracle;
use mz_timestamp_oracle::{TimestampOracle, TimestampOracleConfig};

#[derive(Clone)]
struct Args {
    url: String,
    mode: String,
    concurrency: Vec<usize>,
    duration: Duration,
    append_ms: u64,
    skip_peek: bool,
    skip_advance_read: bool,
    raw: bool,
}

const USAGE: &str = "\
tsoracle_bench [options]

  --url URL              backing store URL (or env METADATA_BACKEND_URL)
  --mode MODE            read | write | mixed (default: read)
  --concurrency N,N,...  read concurrency levels (default: 1,8,64,512)
  --duration SECS        seconds per measurement (default: 10)
  --append-ms MS         simulated persist append duration in write mode (default: 10)
  --skip-peek            write mode: skip the pacing peek_write_ts
  --skip-advance-read    write mode: skip the trailing read_ts
  --raw                  don't wrap the oracle in BatchingTimestampOracle
";

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        url: std::env::var("METADATA_BACKEND_URL").unwrap_or_default(),
        mode: "read".to_string(),
        concurrency: vec![1, 8, 64, 512],
        duration: Duration::from_secs(10),
        append_ms: 10,
        skip_peek: false,
        skip_advance_read: false,
        raw: false,
    };

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value =
            |name: &str| it.next().ok_or_else(|| format!("{} requires a value", name));
        match arg.as_str() {
            "--url" => args.url = value("--url")?,
            "--mode" => args.mode = value("--mode")?,
            "--concurrency" => {
                args.concurrency = value("--concurrency")?
                    .split(',')
                    .map(|s| s.parse::<usize>().map_err(|e| e.to_string()))
                    .collect::<Result<_, _>>()?;
            }
            "--duration" => {
                args.duration = Duration::from_secs(
                    value("--duration")?
                        .parse()
                        .map_err(|e| format!("--duration: {}", e))?,
                )
            }
            "--append-ms" => {
                args.append_ms = value("--append-ms")?
                    .parse()
                    .map_err(|e| format!("--append-ms: {}", e))?
            }
            "--skip-peek" => args.skip_peek = true,
            "--skip-advance-read" => args.skip_advance_read = true,
            "--raw" => args.raw = true,
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown argument: {}\n{}", other, USAGE)),
        }
    }

    if args.url.is_empty() {
        return Err(format!(
            "--url or METADATA_BACKEND_URL is required\n{}",
            USAGE
        ));
    }
    match args.mode.as_str() {
        "read" | "write" | "mixed" => {}
        other => return Err(format!("unknown mode: {}\n{}", other, USAGE)),
    }

    Ok(args)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = f64::cast_lossy(sorted.len() - 1) * p / 100.0;
    let idx = usize::cast_lossy(rank.round());
    sorted[idx.min(sorted.len() - 1)]
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

struct LatencySummary {
    count: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

fn summarize(mut latencies: Vec<u64>) -> LatencySummary {
    latencies.sort_unstable();
    LatencySummary {
        count: latencies.len(),
        p50: percentile(&latencies, 50.0),
        p95: percentile(&latencies, 95.0),
        p99: percentile(&latencies, 99.0),
        max: latencies.last().copied().unwrap_or(0),
    }
}

async fn read_workload(
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    concurrency: usize,
    duration: Duration,
) -> Vec<u64> {
    let deadline = Instant::now() + duration;
    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let oracle = Arc::clone(&oracle);
        let handle = mz_ore::task::spawn(|| format!("tsoracle-bench-reader-{}", i), async move {
            let mut latencies = Vec::new();
            while Instant::now() < deadline {
                let start = Instant::now();
                let _ts = oracle.read_ts().await;
                latencies.push(micros(start.elapsed()));
            }
            latencies
        });
        handles.push(handle);
    }
    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await);
    }
    all
}

struct WriteResult {
    commits: usize,
    backend_ops: u64,
    peek: Vec<u64>,
    write: Vec<u64>,
    apply: Vec<u64>,
    advance_read: Vec<u64>,
}

/// Mimics the oracle traffic of one group commit per iteration.
async fn write_workload(
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    args: Args,
) -> WriteResult {
    let deadline = Instant::now() + args.duration;
    let mut result = WriteResult {
        commits: 0,
        backend_ops: 0,
        peek: Vec::new(),
        write: Vec::new(),
        apply: Vec::new(),
        advance_read: Vec::new(),
    };
    while Instant::now() < deadline {
        if !args.skip_peek {
            let start = Instant::now();
            let _ts = oracle.peek_write_ts().await;
            result.peek.push(micros(start.elapsed()));
            result.backend_ops += 1;
        }

        let start = Instant::now();
        let write_ts = oracle.write_ts().await;
        result.write.push(micros(start.elapsed()));
        result.backend_ops += 1;

        tokio::time::sleep(Duration::from_millis(args.append_ms)).await;

        let start = Instant::now();
        oracle.apply_write(write_ts.timestamp).await;
        result.apply.push(micros(start.elapsed()));
        result.backend_ops += 1;

        if !args.skip_advance_read {
            let start = Instant::now();
            let _ts = oracle.read_ts().await;
            result.advance_read.push(micros(start.elapsed()));
        }

        result.commits += 1;
    }
    result
}

fn report_read(concurrency: usize, duration: Duration, latencies: Vec<u64>, backend_selects: u64) {
    let summary = summarize(latencies);
    let ops_per_s = f64::cast_lossy(summary.count) / duration.as_secs_f64();
    let backend_per_s = f64::cast_lossy(backend_selects) / duration.as_secs_f64();
    let avg_batch = if backend_selects > 0 {
        f64::cast_lossy(summary.count) / f64::cast_lossy(backend_selects)
    } else {
        0.0
    };
    println!(
        "RESULT mode=read concurrency={} ops={} ops_per_s={:.1} backend_selects={} \
         backend_selects_per_s={:.1} avg_batch_size={:.1} p50_us={} p95_us={} p99_us={} max_us={}",
        concurrency,
        summary.count,
        ops_per_s,
        backend_selects,
        backend_per_s,
        avg_batch,
        summary.p50,
        summary.p95,
        summary.p99,
        summary.max,
    );
}

fn report_write(args: &Args, result: WriteResult) {
    let commits_per_s = f64::cast_lossy(result.commits) / args.duration.as_secs_f64();
    let backend_per_commit = if result.commits > 0 {
        f64::cast_lossy(result.backend_ops) / f64::cast_lossy(result.commits)
    } else {
        0.0
    };
    let op_summary = |name: &str, lat: Vec<u64>| {
        if lat.is_empty() {
            return String::new();
        }
        let s = summarize(lat);
        format!(" {}_p50_us={} {}_p99_us={}", name, s.p50, name, s.p99)
    };
    println!(
        "RESULT mode=write commits={} commits_per_s={:.1} append_ms={} backend_write_ops={} \
         backend_write_ops_per_commit={:.1} skip_peek={} skip_advance_read={}{}{}{}{}",
        result.commits,
        commits_per_s,
        args.append_ms,
        result.backend_ops,
        backend_per_commit,
        args.skip_peek,
        args.skip_advance_read,
        op_summary("peek", result.peek),
        op_summary("write_ts", result.write),
        op_summary("apply_write", result.apply),
        op_summary("advance_read", result.advance_read),
    );
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{}", msg);
            std::process::exit(1);
        }
    };

    let url = SensitiveUrl::from_str(&args.url).expect("invalid URL");
    let registry = MetricsRegistry::new();
    let config = TimestampOracleConfig::from_url(&url, &registry).expect("unsupported URL");
    let metrics = config.metrics();

    let timeline = format!("tsoracle-bench-{}", uuid::Uuid::new_v4());
    let inner = config
        .open(
            timeline,
            Timestamp::MIN,
            SYSTEM_TIME.clone(),
            false, /* read_only */
        )
        .await;

    let oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> = if args.raw {
        inner
    } else {
        Arc::new(BatchingTimestampOracle::new(Arc::clone(&metrics), inner))
    };

    println!(
        "# url={} mode={} duration={:?} raw={}",
        url, args.mode, args.duration, args.raw
    );

    match args.mode.as_str() {
        "read" => {
            for &concurrency in &args.concurrency {
                let selects_before = metrics.batching.read_ts.batches_count.get();
                let latencies =
                    read_workload(Arc::clone(&oracle), concurrency, args.duration).await;
                // Without the batching wrapper every read_ts is a backend
                // query, but the batching metrics don't tick.
                let backend_selects = if args.raw {
                    u64::try_from(latencies.len()).expect("count fits in u64")
                } else {
                    metrics.batching.read_ts.batches_count.get() - selects_before
                };
                report_read(concurrency, args.duration, latencies, backend_selects);
            }
        }
        "write" => {
            let result = write_workload(Arc::clone(&oracle), args.clone()).await;
            report_write(&args, result);
        }
        "mixed" => {
            for &concurrency in &args.concurrency {
                let selects_before = metrics.batching.read_ts.batches_count.get();
                let write_oracle = Arc::clone(&oracle);
                let write_args = args.clone();
                let writer = mz_ore::task::spawn(|| "tsoracle-bench-writer", async move {
                    write_workload(write_oracle, write_args).await
                });
                let latencies =
                    read_workload(Arc::clone(&oracle), concurrency, args.duration).await;
                let write_result = writer.await;
                let backend_selects = metrics.batching.read_ts.batches_count.get() - selects_before;
                report_read(concurrency, args.duration, latencies, backend_selects);
                report_write(&args, write_result);
            }
        }
        _ => unreachable!("validated in parse_args"),
    }
}
