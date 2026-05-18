// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist-only CAS slope microbenchmark.
//!
//! Opens a single persist shard, fills it to a target SeqNo with a
//! catalog-like write pattern (one small `compare_and_append` per "DDL"),
//! then takes a window of timed `compare_and_append` measurements at that
//! state size. Repeats for each rung of a size-ladder.
//!
//! Runs against any [`Consensus`] / [`Blob`] backend pair, so we can compare
//! the slope under real CockroachDB vs. bogo-consensus vs. in-memory and
//! decide whether the per-CAS slope lives above or below the [`Consensus`]
//! trait boundary.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use mz_build_info::DUMMY_BUILD_INFO;
use mz_dyncfg::ConfigSet;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::bogo::{BogoConsensus, BogoConsensusConfig};
use mz_persist::file::{FileBlob, FileBlobConfig};
use mz_persist::location::{Blob, Consensus};
use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
use mz_persist::postgres::{
    PostgresConsensus, PostgresConsensusConfig, USE_POSTGRES_TUNED_QUERIES,
};
use mz_persist_client::async_runtime::IsolatedRuntime;
use mz_persist_client::cache::StateCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_postgres_client::PostgresClientKnobs;
use mz_postgres_client::metrics::PostgresClientMetrics;
use timely::progress::Antichain;

/// One CAS measurement row in the output CSV.
#[derive(Debug)]
struct Sample {
    backend: String,
    blob: String,
    size: u64,
    rep: usize,
    ms: f64,
}

#[derive(Parser, Debug)]
#[command(about = "Persist-only CAS slope microbenchmark", long_about = None)]
struct Args {
    /// `bogo://host:port`, `postgres://...`, or `mem`.
    #[arg(long)]
    consensus: String,

    /// `file:///path` or `mem`.
    #[arg(long, default_value = "mem")]
    blob: String,

    /// Comma-separated pre-fill SeqNo ladder. Each rung gets a fresh shard.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "0,500,1000,2500,5000,10000"
    )]
    sizes: Vec<u64>,

    /// Timed CAS measurements taken at each rung after pre-fill.
    #[arg(long, default_value_t = 200)]
    measurements: usize,

    /// Override `persist_rollup_threshold`; 0 keeps the persist default (128).
    /// Set very high (e.g. 1_000_000) to suppress rollups so state genuinely
    /// grows; leave at default to model production rollup cadence.
    #[arg(long, default_value_t = 0)]
    rollup_threshold: usize,

    /// Output CSV path. Appends if it exists.
    #[arg(long)]
    out: PathBuf,

    /// Free-form tag recorded in each CSV row (e.g. `crdb-default-rollup`).
    #[arg(long, default_value = "")]
    tag: String,
}

#[derive(Debug)]
struct CasKnobs;

impl PostgresClientKnobs for CasKnobs {
    fn connection_pool_max_size(&self) -> usize {
        2
    }
    fn connection_pool_max_wait(&self) -> Option<Duration> {
        Some(Duration::from_secs(5))
    }
    fn connection_pool_ttl(&self) -> Duration {
        Duration::MAX
    }
    fn connection_pool_ttl_stagger(&self) -> Duration {
        Duration::MAX
    }
    fn connect_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn tcp_user_timeout(&self) -> Duration {
        Duration::ZERO
    }
    fn keepalives_idle(&self) -> Duration {
        Duration::from_secs(10)
    }
    fn keepalives_interval(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn keepalives_retries(&self) -> u32 {
        5
    }
}

async fn open_consensus(url: &str) -> Result<Arc<dyn Consensus>> {
    if url == "mem" {
        return Ok(Arc::new(MemConsensus::default()));
    }
    if url.starts_with("bogo://") {
        let u = SensitiveUrl::from_str(url).context("parsing bogo url")?;
        let cfg = BogoConsensusConfig::new(u).map_err(|e| anyhow!("bogo config: {e}"))?;
        let c = BogoConsensus::open(cfg)
            .await
            .map_err(|e| anyhow!("bogo open: {e}"))?;
        return Ok(Arc::new(c));
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let u = SensitiveUrl::from_str(url).context("parsing postgres url")?;
        let dyncfg = Arc::new(ConfigSet::default().add(&USE_POSTGRES_TUNED_QUERIES));
        let metrics = PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist");
        let cfg = PostgresConsensusConfig::new(&u, Box::new(CasKnobs), metrics, dyncfg)
            .map_err(|e| anyhow!("pg config: {e}"))?;
        let c = PostgresConsensus::open(cfg)
            .await
            .map_err(|e| anyhow!("pg open: {e}"))?;
        return Ok(Arc::new(c));
    }
    bail!("unsupported consensus URL: {}", url);
}

async fn open_blob(url: &str) -> Result<Arc<dyn Blob>> {
    if url == "mem" {
        return Ok(Arc::new(MemBlob::open(MemBlobConfig::default())));
    }
    if let Some(path) = url.strip_prefix("file://") {
        std::fs::create_dir_all(path).context("creating blob dir")?;
        let cfg = FileBlobConfig::from(path);
        let b = FileBlob::open(cfg)
            .await
            .map_err(|e| anyhow!("file blob open: {e}"))?;
        return Ok(Arc::new(b));
    }
    bail!("unsupported blob URL: {}", url);
}

fn make_client(
    consensus: Arc<dyn Consensus>,
    blob: Arc<dyn Blob>,
    rollup_threshold: usize,
) -> Result<PersistClient> {
    let cfg = PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
    if rollup_threshold > 0 {
        cfg.set_rollup_threshold(rollup_threshold);
    }
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let isolated_runtime = Arc::new(IsolatedRuntime::new_for_tests());
    let pubsub_sender = PubSubClientConnection::noop().sender;
    let shared_states = Arc::new(StateCache::new(
        &cfg,
        Arc::clone(&metrics),
        Arc::clone(&pubsub_sender),
    ));
    PersistClient::new(
        cfg,
        blob,
        consensus,
        metrics,
        isolated_runtime,
        shared_states,
        pubsub_sender,
    )
    .map_err(|e| anyhow!("persist client: {e}"))
}

/// One iteration of the catalog-like CAS pattern: append a tiny batch that
/// advances `upper` from `ts` to `ts+1`. Returns the new upper.
async fn cas_once(
    write: &mut mz_persist_client::write::WriteHandle<String, (), u64, i64>,
    ts: u64,
    row: &str,
) -> Result<u64> {
    let updates = vec![((row.to_owned(), ()), ts, 1i64)];
    let expected = Antichain::from_elem(ts);
    let new = Antichain::from_elem(ts + 1);
    let res = write
        .compare_and_append(updates, expected, new)
        .await
        .map_err(|e| anyhow!("invalid usage: {e:?}"))?;
    res.map_err(|m| anyhow!("upper mismatch: {m:?}"))?;
    Ok(ts + 1)
}

async fn run_rung(
    client: &PersistClient,
    backend_tag: &str,
    blob_tag: &str,
    size: u64,
    measurements: usize,
) -> Result<Vec<Sample>> {
    let shard_id = ShardId::new();
    let diag = Diagnostics {
        shard_name: "persist_cas_bench".to_string(),
        handle_purpose: "cas-microbench".to_string(),
    };
    let (mut write, _read) = client
        .open::<String, (), u64, i64>(
            shard_id,
            Arc::new(StringSchema),
            Arc::new(UnitSchema),
            diag,
            true,
        )
        .await
        .map_err(|e| anyhow!("open: {e:?}"))?;

    // Pre-fill: advance the shard's history to `size` SeqNos by issuing
    // `size` non-empty compare_and_appends.
    let mut ts: u64 = 0;
    let row = "catalog-like-small-row".to_string();
    let mut last_print = Instant::now();
    while ts < size {
        ts = cas_once(&mut write, ts, &row).await?;
        if last_print.elapsed() > Duration::from_secs(5) {
            eprintln!("  pre-fill {}/{} ts={}", ts, size, ts);
            last_print = Instant::now();
        }
    }

    // Measurement window.
    let mut samples = Vec::with_capacity(measurements);
    for rep in 0..measurements {
        let start = Instant::now();
        ts = cas_once(&mut write, ts, &row).await?;
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        samples.push(Sample {
            backend: backend_tag.to_string(),
            blob: blob_tag.to_string(),
            size,
            rep,
            ms,
        });
    }

    Ok(samples)
}

fn write_csv(path: &PathBuf, tag: &str, samples: &[Sample]) -> Result<()> {
    let already = path.exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    use std::io::Write;
    if !already {
        writeln!(f, "tag,backend,blob,size,rep,ms")?;
    }
    for s in samples {
        writeln!(
            f,
            "{},{},{},{},{},{:.4}",
            tag, s.backend, s.blob, s.size, s.rep, s.ms
        )?;
    }
    Ok(())
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx]
}

fn summarize(samples: &[Sample]) {
    let mut ms: Vec<f64> = samples.iter().map(|s| s.ms).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sum: f64 = ms.iter().sum();
    let mean = sum / (ms.len() as f64);
    eprintln!(
        "  n={} mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        ms.len(),
        mean,
        percentile(&ms, 0.50),
        percentile(&ms, 0.95),
        percentile(&ms, 0.99),
        ms.last().copied().unwrap_or(f64::NAN),
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    mz_ore::test::init_logging_default("warn");

    let args = Args::parse();
    let backend_tag = args.consensus.clone();
    let blob_tag = args.blob.clone();
    eprintln!(
        "persist_cas_bench: consensus={} blob={} sizes={:?} measurements={} rollup_threshold={} tag={}",
        backend_tag, blob_tag, args.sizes, args.measurements, args.rollup_threshold, args.tag
    );

    for size in &args.sizes {
        eprintln!("== size={} ==", size);
        // Build a fresh client per rung so the state cache and metrics
        // don't carry over between rungs.
        let consensus = open_consensus(&args.consensus).await?;
        let blob = open_blob(&args.blob).await?;
        let client = make_client(consensus, blob, args.rollup_threshold)?;
        let samples = run_rung(&client, &backend_tag, &blob_tag, *size, args.measurements).await?;
        summarize(&samples);
        write_csv(&args.out, &args.tag, &samples)?;
    }

    eprintln!("done; wrote {}", args.out.display());
    Ok(())
}
