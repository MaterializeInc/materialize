// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Standalone load generator for persist **shards** against a consensus backend.
//!
//! Rather than poking the `Consensus` trait directly, this opens real persist shards via
//! `mz-persist-client` and drives the consensus store the way Materialize actually does: each of
//! `--shards` shards has a single writer that **advances its write frontier by appending an empty
//! batch** once per `--tick-ms` (exactly what a materialized view's persist sink does when its
//! upper ticks), which the persist state machine turns into a compare-and-set against consensus.
//! Each shard also has `--readers-per-shard` (default 2) **leased readers**, whose lease renewals
//! and periodic `since` downgrades add the reader-side consensus traffic of a real deployment and
//! let state get compacted/GCed.
//!
//! Knobs that map onto the experiments: `--shards` (breadth), `--readers-per-shard`,
//! `--pool-max-size` (`persist_consensus_connection_pool_max_size`), `--tuned-queries`
//! (`persist_use_postgres_tuned_queries`), `--batch-rows`/`--row-pad-bytes` (non-empty inline
//! batches → larger consensus state rows, mimicking real MV shards), `--since-every 1` (per-tick
//! `since` downgrades like the real controller; `0` pins `since` and disables GC/truncation). It reports committed frontier-advances/s and their
//! latency, and serves the full persist metrics (incl. `mz_persist_external_*{op="consensus_cas"}`)
//! at `--metrics-listen-addr`/metrics.
//!
//! Needs both a consensus URL (`--url`, Postgres) and a blob URL (`--blob-url`, defaults to a temp
//! `file://` dir; `s3://...` for full fidelity). Each run uses fresh shard ids, so it never
//! collides with a previous run.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use mz_dyncfg::{ConfigUpdates, ConfigVal};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::postgres::USE_POSTGRES_TUNED_QUERIES;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::{CONSENSUS_CONNECTION_POOL_MAX_SIZE, PersistConfig};
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::StringSchema;
use timely::progress::Antichain;

/// Drive N persist shards (empty-batch frontier advances + leased readers) against a consensus DB.
#[derive(Parser, Debug, Clone)]
#[clap(name = "persist-load-gen", about, long_about = None)]
struct Args {
    /// Consensus Postgres URL, e.g. `postgres://user:pw@host:5432/db?sslmode=require`.
    #[clap(long, env = "CONSENSUS_URL")]
    url: String,

    /// Blob store URL. Defaults to a temp `file://` dir. Use `s3://bucket/prefix` for full fidelity.
    #[clap(long)]
    blob_url: Option<String>,

    /// Number of shards. Each shard is one single-writer (≈ one ticking materialized view).
    #[clap(long, default_value_t = 1000)]
    shards: usize,

    /// Number of active leased readers per shard (each renews a lease + advances `since`).
    #[clap(long, default_value_t = 2)]
    readers_per_shard: usize,

    /// Per-shard minimum interval between frontier advances, in milliseconds (≈ a 1s timestamp
    /// tick). Under DB slowness a shard advances as fast as appends return (the real backpressure).
    #[clap(long, default_value_t = 1000)]
    tick_ms: u64,

    /// `persist_consensus_connection_pool_max_size` — concurrent consensus connections **per pool**.
    #[clap(long, default_value_t = 50)]
    pool_max_size: usize,

    /// Shards served by each independent connection pool (≈ one clusterd/process, each with its own
    /// pool — as in a real deployment with one consensus pool per cluster). Total pools =
    /// ceil(shards / shards_per_pool); total connections = pools × pool_max_size, so the connection
    /// count scales with shards instead of being fixed.
    #[clap(long, default_value_t = 1000)]
    shards_per_pool: usize,

    /// Toggle `persist_use_postgres_tuned_queries`.
    #[clap(long, default_value_t = false)]
    tuned_queries: bool,

    /// How far behind the upper each reader keeps its `since` (in ticks), enabling compaction/GC.
    #[clap(long, default_value_t = 10)]
    since_lag: u64,

    /// Downgrade readers' `since` every this many frontier advances. `1` matches the real
    /// controller's per-tick downgrades; `0` disables downgrades entirely (pins `seqno_since`, so
    /// consensus rows are never truncated — a "no GC" mode that grows the table with live rows).
    #[clap(long, default_value_t = 16)]
    since_every: u64,

    /// Rows appended per tick (0 = empty batch, the pure frontier-advance regime). With the
    /// default `persist_inline_writes_single_max_bytes` (4096), small batches are stored *inline
    /// in the consensus state row*, so this (with `--row-pad-bytes`) directly grows the
    /// `consensus.consensus.data` bytea like real MV shards' batch metadata does.
    #[clap(long, default_value_t = 0)]
    batch_rows: usize,

    /// Pad each appended row's value to roughly this many bytes.
    #[clap(long, default_value_t = 256)]
    row_pad_bytes: usize,

    /// Run for this many seconds (0 = until Ctrl-C).
    #[clap(long, default_value_t = 120)]
    duration_secs: u64,

    /// Metrics report interval, in seconds.
    #[clap(long, default_value_t = 5)]
    report_secs: u64,

    /// Spread shard start-up over this many seconds (avoid a thundering herd at t=0).
    #[clap(long, default_value_t = 15)]
    ramp_secs: u64,

    /// Arbitrary persist dyncfg overrides, `name=value` (repeatable), e.g.
    /// `--cfg persist_rollup_threshold=32 --cfg persist_gc_min_versions=8`. The value is parsed
    /// according to the config's registered type.
    #[clap(long = "cfg", value_name = "NAME=VALUE")]
    cfg: Vec<String>,
}

/// Process-wide counters. `lat_buckets[i]` counts advances whose latency fell in `[2^i, 2^(i+1))` µs.
#[derive(Default)]
struct Stats {
    advances: AtomicU64,
    mismatch: AtomicU64,
    errors: AtomicU64,
    lat_sum_us: AtomicU64,
    lat_buckets: [AtomicU64; 32],
}

impl Stats {
    fn record_advance(&self, latency: Duration) {
        let us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX).max(1);
        self.advances.fetch_add(1, Ordering::Relaxed);
        self.lat_sum_us.fetch_add(us, Ordering::Relaxed);
        let bucket = (63 - us.leading_zeros()) as usize;
        self.lat_buckets[bucket.min(31)].fetch_add(1, Ordering::Relaxed);
    }
    fn percentile_ms(&self, q: f64) -> f64 {
        let total: u64 = self.lat_buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
        if total == 0 {
            return 0.0;
        }
        let target = (total as f64 * q) as u64;
        let mut seen = 0u64;
        for (i, b) in self.lat_buckets.iter().enumerate() {
            seen += b.load(Ordering::Relaxed);
            if seen >= target {
                return (1u64 << (i + 1)) as f64 / 1000.0;
            }
        }
        f64::INFINITY
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,mz_persist_load_gen=info")),
        )
        .with_writer(std::io::stderr)
        .init();

    // Blob defaults to a temp file dir so the tool is self-contained against just a Postgres.
    let blob_url = match &args.blob_url {
        Some(u) => u.clone(),
        None => {
            let dir = std::env::temp_dir().join(format!("persist-load-gen-blob-{}", std::process::id()));
            std::fs::create_dir_all(&dir).context("creating temp blob dir")?;
            format!("file://{}", dir.display())
        }
    };
    eprintln!(
        "config: shards={} readers_per_shard={} tick_ms={} pool_max_size={} tuned_queries={} \
         batch_rows={} row_pad_bytes={} since_every={} duration={}s\n consensus={} blob={}",
        args.shards,
        args.readers_per_shard,
        args.tick_ms,
        args.pool_max_size,
        args.tuned_queries,
        args.batch_rows,
        args.row_pad_bytes,
        args.since_every,
        args.duration_secs,
        // redact consensus creds
        args.url.split('@').last().unwrap_or("?"),
        blob_url,
    );

    // Build the persist config and push our dyncfgs (pool size, tuned queries).
    let cfg = PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone());
    let mut updates = ConfigUpdates::default();
    updates.add(&CONSENSUS_CONNECTION_POOL_MAX_SIZE, args.pool_max_size);
    if args.tuned_queries {
        updates.add(&USE_POSTGRES_TUNED_QUERIES, true);
    }
    // Arbitrary --cfg name=value overrides, parsed per the registered config's type.
    for pair in &args.cfg {
        let (name, value) = pair
            .split_once('=')
            .with_context(|| format!("--cfg {pair}: expected NAME=VALUE"))?;
        let entry = cfg
            .configs
            .entry(name)
            .with_context(|| format!("--cfg {name}: no such persist config"))?;
        let val = match entry.default() {
            ConfigVal::Bool(_) => ConfigVal::Bool(value.parse()?),
            ConfigVal::U32(_) => ConfigVal::U32(value.parse()?),
            ConfigVal::Usize(_) => ConfigVal::Usize(value.parse()?),
            ConfigVal::OptUsize(_) => ConfigVal::OptUsize(Some(value.parse()?)),
            ConfigVal::F64(_) => ConfigVal::F64(value.parse()?),
            ConfigVal::String(_) => ConfigVal::String(value.to_string()),
            ConfigVal::Duration(_) => {
                ConfigVal::Duration(Duration::from_millis(value.parse().with_context(
                    || format!("--cfg {name}: duration value must be integer milliseconds"),
                )?))
            }
            other => anyhow::bail!("--cfg {name}: unsupported config type {other:?}"),
        };
        eprintln!("cfg override: {name}={value}");
        updates.add_dynamic(entry.name(), val);
    }
    cfg.apply_from(&updates);

    let location = PersistLocation {
        blob_uri: SensitiveUrl::from_str(&blob_url).context("parsing blob url")?,
        consensus_uri: SensitiveUrl::from_str(&args.url).context("parsing --url")?,
    };

    // One *independent* PersistClient (hence its own consensus connection pool) per
    // `shards_per_pool` block of shards — mimicking one pool per clusterd. Total connections =
    // pools × pool_max_size scales with shards. Separate caches → separate pools; each needs its
    // own metrics registry (sharing one would double-register metrics).
    let spp = args.shards_per_pool.max(1);
    let num_pools = args.shards.div_ceil(spp).max(1);
    let mut pools: Vec<PersistClient> = Vec::with_capacity(num_pools);
    let mut registries: Vec<MetricsRegistry> = Vec::with_capacity(num_pools);
    for _ in 0..num_pools {
        let registry = MetricsRegistry::new();
        let client =
            PersistClientCache::new(cfg.clone(), &registry, |_, _| PubSubClientConnection::noop())
                .open(location.clone())
                .await
                .context("opening persist client")?;
        pools.push(client);
        registries.push(registry);
    }
    eprintln!(
        "pools={} (1 per {} shards) × pool_max_size={} => up to {} total consensus connections",
        num_pools,
        spp,
        args.pool_max_size,
        num_pools * args.pool_max_size,
    );

    let stats = Arc::new(Stats::default());
    let start = Instant::now();
    let deadline = (args.duration_secs > 0).then(|| start + Duration::from_secs(args.duration_secs));

    let mut handles = Vec::with_capacity(args.shards);
    for i in 0..args.shards {
        let client = pools[i / spp].clone();
        let stats = Arc::clone(&stats);
        let args = args.clone();
        let stagger = if args.shards > 1 {
            Duration::from_millis(args.ramp_secs * 1000 * i as u64 / args.shards as u64)
        } else {
            Duration::ZERO
        };
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(stagger).await;
            if let Err(e) = run_shard(client, i, args, deadline, stats).await {
                tracing::warn!("shard {i} setup failed: {e:#}");
            }
        }));
    }

    let reporter = tokio::spawn(report_loop(
        Arc::clone(&stats),
        registries,
        Duration::from_secs(args.report_secs.max(1)),
        start,
        deadline,
    ));
    for h in handles {
        let _ = h.await;
    }
    let _ = reporter.await;

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    let advances = stats.advances.load(Ordering::Relaxed);
    let mean_ms = if advances > 0 {
        stats.lat_sum_us.load(Ordering::Relaxed) as f64 / advances as f64 / 1000.0
    } else {
        0.0
    };
    eprintln!(
        "\n==== SUMMARY ====\n shards={} readers_per_shard={} pool={} tuned={}\n \
         advances={} ({:.1}/s) mismatch={} errors={}\n mean_ms={:.2} p50_ms={:.1} p99_ms={:.1}",
        args.shards,
        args.readers_per_shard,
        args.pool_max_size,
        args.tuned_queries,
        advances,
        advances as f64 / elapsed,
        stats.mismatch.load(Ordering::Relaxed),
        stats.errors.load(Ordering::Relaxed),
        mean_ms,
        stats.percentile_ms(0.50),
        stats.percentile_ms(0.99),
    );
    Ok(())
}

/// Open one shard's writer + leased readers, then advance its frontier with empty batches.
async fn run_shard(
    client: PersistClient,
    idx: usize,
    args: Args,
    deadline: Option<Instant>,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    let shard_id = ShardId::new();
    let diag = Diagnostics {
        shard_name: format!("loadgen-{idx}"),
        handle_purpose: "persist-load-gen".to_string(),
    };
    let mut write = client
        .open_writer::<String, String, u64, i64>(
            shard_id,
            Arc::new(StringSchema),
            Arc::new(StringSchema),
            diag.clone(),
        )
        .await?;
    let mut readers = Vec::with_capacity(args.readers_per_shard);
    for _ in 0..args.readers_per_shard {
        readers.push(
            client
                .open_leased_reader::<String, String, u64, i64>(
                    shard_id,
                    Arc::new(StringSchema),
                    Arc::new(StringSchema),
                    diag.clone(),
                    true,
                )
                .await?,
        );
    }

    let mut upper: u64 = 0;
    let mut n: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.tick_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let pad = "x".repeat(args.row_pad_bytes);

    loop {
        ticker.tick().await;
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        // Advance the write frontier by one tick. With `--batch-rows 0` the batch is empty (a pure
        // frontier advance); otherwise it carries padded rows that (below the inline-writes
        // threshold) land inline in the consensus state row.
        let updates: Vec<((String, String), u64, i64)> = (0..args.batch_rows)
            .map(|r| ((format!("k-{idx}-{upper}-{r}"), pad.clone()), upper, 1))
            .collect();
        let t0 = Instant::now();
        match write
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(upper + 1),
            )
            .await
        {
            Ok(Ok(())) => {
                stats.record_advance(t0.elapsed());
                upper += 1;
                n += 1;
                // Periodically advance readers' `since` so state can be compacted/GCed.
                if args.since_every > 0 && n % args.since_every == 0 && upper > args.since_lag {
                    let new_since = Antichain::from_elem(upper - args.since_lag);
                    for r in &mut readers {
                        r.downgrade_since(&new_since).await;
                    }
                }
            }
            Ok(Err(mismatch)) => {
                // Single-writer, so this is unexpected; resync the upper and continue.
                stats.mismatch.fetch_add(1, Ordering::Relaxed);
                upper = mismatch.current.into_option().unwrap_or(upper);
            }
            Err(invalid) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("shard {idx} invalid usage: {invalid}");
                break;
            }
        }
    }

    // Let the readers/writer expire their leases cleanly.
    for r in readers {
        r.expire().await;
    }
    write.expire().await;
    Ok(())
}

/// Sum/aggregate a handful of persist-internal metrics across all pools' registries and render
/// them as one log line: state-fetch fast/slow path counts, GC started/finished/merged (queue
/// depth = started − finished is the serial-GC-worker backlog), per-op consensus call counts, and
/// the per-shard `seqnos_held` gauge (sum/max — the retained-rows-per-shard proxy).
fn gather_gauges(registries: &[MetricsRegistry]) -> String {
    let (mut fast, mut slow) = (0i64, 0i64);
    let (mut gc_started, mut gc_finished, mut gc_merged) = (0i64, 0i64, 0i64);
    let (mut gc_noop, mut rollup_ok) = (0i64, 0i64);
    let (mut rollup_noop_latest, mut rollup_noop_trunc) = (0i64, 0i64);
    let (mut held_sum, mut held_max, mut held_n) = (0i64, 0i64, 0i64);
    let mut ops: std::collections::BTreeMap<String, i64> = Default::default();
    for r in registries {
        for mf in r.gather() {
            match mf.name() {
                "mz_persist_state_fetch_recent_live_diffs_fast_path" => {
                    fast += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_state_fetch_recent_live_diffs_slow_path" => {
                    slow += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_gc_started" => {
                    gc_started += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_gc_finished" => {
                    gc_finished += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_gc_merged_reqs" => {
                    gc_merged += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_gc_noop" => {
                    gc_noop += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_state_rollup_write_success" => {
                    rollup_ok += mf.get_metric().iter().map(|m| m.get_counter().value() as i64).sum::<i64>();
                }
                "mz_persist_state_rollup_write_noop" => {
                    for m in mf.get_metric() {
                        let reason = m
                            .get_label()
                            .iter()
                            .find(|l| l.name() == "reason")
                            .map(|l| l.value())
                            .unwrap_or("?");
                        let v = m.get_counter().value() as i64;
                        match reason {
                            "latest" => rollup_noop_latest += v,
                            "truncated" => rollup_noop_trunc += v,
                            _ => rollup_noop_latest += v,
                        }
                    }
                }
                "mz_persist_shard_seqnos_held" => {
                    for m in mf.get_metric() {
                        let v = m.get_gauge().value() as i64;
                        held_sum += v;
                        held_max = held_max.max(v);
                        held_n += 1;
                    }
                }
                "mz_persist_external_succeeded_count" => {
                    for m in mf.get_metric() {
                        let op = m
                            .get_label()
                            .iter()
                            .find(|l| l.name() == "op")
                            .map(|l| l.value())
                            .unwrap_or("?");
                        if op.starts_with("consensus_") {
                            *ops.entry(op.to_string()).or_default() +=
                                m.get_counter().value() as i64;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let ops = ops
        .iter()
        .map(|(k, v)| format!("{}={}", k.trim_start_matches("consensus_"), v))
        .collect::<Vec<_>>()
        .join(" ");
    let held_avg = if held_n > 0 { held_sum / held_n } else { 0 };
    format!(
        "fetch_fast={fast} fetch_slow={slow} gc_started={gc_started} gc_finished={gc_finished} \
         gc_queued={} gc_merged={gc_merged} gc_noop={gc_noop} rollup_ok={rollup_ok} \
         rollup_noop_latest={rollup_noop_latest} rollup_noop_trunc={rollup_noop_trunc} \
         seqnos_held avg={held_avg} max={held_max} | {ops}",
        gc_started - gc_finished,
    )
}

/// Periodically print per-interval frontier-advance throughput / latency / failure rates.
async fn report_loop(
    stats: Arc<Stats>,
    registries: Vec<MetricsRegistry>,
    every: Duration,
    start: Instant,
    deadline: Option<Instant>,
) {
    let mut last_advances = 0u64;
    let mut last_sum_us = 0u64;
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let advances = stats.advances.load(Ordering::Relaxed);
        let sum_us = stats.lat_sum_us.load(Ordering::Relaxed);
        let dc = advances - last_advances;
        let dsum = sum_us - last_sum_us;
        let interval_mean_ms = if dc > 0 {
            dsum as f64 / dc as f64 / 1000.0
        } else {
            0.0
        };
        eprintln!(
            "[t+{:>4}s] advances/s={:>8.1}  interval_mean_ms={:>8.2}  p50_ms={:>6.1}  \
             p99_ms={:>7.1}  mismatch={}  errors={}",
            start.elapsed().as_secs(),
            dc as f64 / every.as_secs_f64(),
            interval_mean_ms,
            stats.percentile_ms(0.50),
            stats.percentile_ms(0.99),
            stats.mismatch.load(Ordering::Relaxed),
            stats.errors.load(Ordering::Relaxed),
        );
        eprintln!("[gauges] {}", gather_gauges(&registries));
        last_advances = advances;
        last_sum_us = sum_us;
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
    }
}
