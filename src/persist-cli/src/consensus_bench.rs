// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Closed-loop benchmark for the persist [Consensus] implementation.
//!
//! Models the write-side load persist puts on the consensus backend: one
//! logical shard is one sequential compare-and-set stream (persist never has
//! more than one CaS in flight per shard), so the concurrency axis is the
//! number of shards. Each shard commits diffs as fast as the backend allows
//! and periodically truncates its prefix, mirroring what state maintenance
//! and GC do in production.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_persist::cfg::ConsensusConfig;
use mz_persist::location::{CaSResult, Consensus, SeqNo, VersionedData};
use mz_persist::postgres::{
    PG_CONSENSUS_PIPELINE_CONNECTIONS, PG_CONSENSUS_PIPELINE_DEPTH, PG_CONSENSUS_READ_COMMITTED,
};
use mz_persist_client::ShardId;
use mz_postgres_client::PostgresClientKnobs;
use mz_postgres_client::metrics::PostgresClientMetrics;
use tokio::sync::Barrier;
use tracing::info;

/// Closed-loop benchmark of consensus compare-and-set across many shards.
#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Consensus URI, e.g. postgres://root@localhost:26257?options=--search_path=consensus
    #[clap(long)]
    consensus_uri: SensitiveUrl,

    /// Number of shards. Each shard is an independent sequential CaS stream,
    /// so this is also the number of concurrent in-flight consensus ops.
    #[clap(long, default_value_t = 64)]
    num_shards: usize,

    /// Length of the measured window.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "30s")]
    runtime: Duration,

    /// Warmup before the measured window. Ops in this period run but are not
    /// recorded, letting the connection pool reach steady state.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "5s")]
    warmup: Duration,

    /// Size of the data payload of each consensus entry, in bytes.
    #[clap(long, default_value_t = 1024)]
    data_size: usize,

    /// After every this many commits a shard truncates all but the most
    /// recent this many entries, keeping the table bounded like GC does.
    #[clap(long, default_value_t = 32)]
    truncate_every: u64,

    /// Maximum size of the Postgres connection pool.
    #[clap(long, default_value_t = 50)]
    pool_size: usize,

    /// Run consensus connections under READ COMMITTED isolation
    /// (vanilla Postgres only).
    #[clap(long)]
    read_committed: bool,

    /// Cap on shared connections that consensus ops run on (pipelining once
    /// all are busy). 0 forces the regular exclusive-checkout pool. Unset
    /// uses the production default.
    #[clap(long)]
    pipeline_connections: Option<usize>,

    /// Maximum in-flight ops per shared connection before new ops wait for a
    /// slot. 0 disables the limit. Unset uses the production default.
    #[clap(long)]
    pipeline_depth: Option<usize>,
}

#[derive(Debug)]
struct BenchKnobs {
    pool_size: usize,
}

/// Production-shaped knobs (see the defaults in mz_persist_client::cfg), with
/// only the pool size under the benchmark's control.
impl PostgresClientKnobs for BenchKnobs {
    fn connection_pool_max_size(&self) -> usize {
        self.pool_size
    }
    fn connection_pool_max_wait(&self) -> Option<Duration> {
        Some(Duration::from_secs(60))
    }
    fn connection_pool_ttl(&self) -> Duration {
        Duration::from_secs(300)
    }
    fn connection_pool_ttl_stagger(&self) -> Duration {
        Duration::from_secs(6)
    }
    fn connect_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn tcp_user_timeout(&self) -> Duration {
        Duration::from_secs(30)
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
    fn statement_timeout(&self) -> Duration {
        Duration::ZERO
    }
}

#[derive(Default)]
struct WorkerStats {
    /// CaS latencies (micros), measured window only.
    cas_latencies_us: Vec<u64>,
    cas_committed: u64,
    cas_mismatches: u64,
    truncates: u64,
    errors: u64,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let configs = mz_persist::cfg::all_dyn_configs(ConfigSet::default());
    let mut updates = ConfigUpdates::default();
    updates.add(&PG_CONSENSUS_READ_COMMITTED, args.read_committed);
    if let Some(pipeline_connections) = args.pipeline_connections {
        updates.add(&PG_CONSENSUS_PIPELINE_CONNECTIONS, pipeline_connections);
    }
    if let Some(pipeline_depth) = args.pipeline_depth {
        updates.add(&PG_CONSENSUS_PIPELINE_DEPTH, pipeline_depth);
    }
    updates.apply(&configs);
    let pipeline_connections = PG_CONSENSUS_PIPELINE_CONNECTIONS.get(&configs);
    let pipeline_depth = PG_CONSENSUS_PIPELINE_DEPTH.get(&configs);

    let consensus = ConsensusConfig::try_from(
        &args.consensus_uri,
        Box::new(BenchKnobs {
            pool_size: args.pool_size,
        }),
        PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist"),
        Arc::new(configs),
    )?
    .open()
    .await?;

    let data = Bytes::from(vec![0xa5u8; args.data_size]);

    info!(
        "initializing {} shards (pool_size={}, read_committed={})",
        args.num_shards, args.pool_size, args.read_committed
    );

    // Every worker (plus the coordinator) meets at the barrier after shard
    // initialization so that the warmup starts with all streams live.
    let barrier = Arc::new(Barrier::new(args.num_shards + 1));
    let progress = Arc::new(AtomicU64::new(0));
    let start_time = Arc::new(std::sync::Mutex::new(None::<(Instant, Instant)>));

    let mut handles = Vec::with_capacity(args.num_shards);
    for i in 0..args.num_shards {
        let consensus = Arc::clone(&consensus);
        let data = data.clone();
        let barrier = Arc::clone(&barrier);
        let progress = Arc::clone(&progress);
        let start_time = Arc::clone(&start_time);
        let truncate_every = args.truncate_every;
        handles.push(mz_ore::task::spawn(
            || format!("consensus_bench_worker_{i}"),
            async move {
                let key = ShardId::new().to_string();
                init_shard(&*consensus, &key, &data).await?;
                barrier.wait().await;
                let (warmup_end, deadline) = *start_time
                    .lock()
                    .expect("lock poisoned")
                    .as_ref()
                    .expect("coordinator sets timestamps before releasing the barrier");
                Ok::<_, anyhow::Error>(
                    shard_worker(
                        &*consensus,
                        &key,
                        &data,
                        truncate_every,
                        warmup_end,
                        deadline,
                        &progress,
                    )
                    .await,
                )
            },
        ));
    }

    // Stamp the shared warmup/deadline right before releasing the workers, so
    // slow shard initialization doesn't eat into the measured window.
    let warmup_end = Instant::now() + args.warmup;
    let deadline = warmup_end + args.runtime;
    *start_time.lock().expect("lock poisoned") = Some((warmup_end, deadline));
    barrier.wait().await;
    info!(
        "shards initialized, warming up for {:?} then measuring for {:?}",
        args.warmup, args.runtime
    );

    // Progress reporter, exits on its own at the deadline.
    {
        let progress = Arc::clone(&progress);
        mz_ore::task::spawn(|| "consensus_bench_progress", async move {
            let mut last = 0u64;
            let mut last_at = Instant::now();
            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let now = Instant::now();
                let cur = progress.load(Ordering::Relaxed);
                let rate = f64::cast_lossy(cur - last) / now.duration_since(last_at).as_secs_f64();
                info!("committed={} rate={:.0}/s", cur, rate);
                last = cur;
                last_at = now;
            }
        });
    }

    let mut merged = WorkerStats::default();
    for handle in handles {
        let stats = handle.await?;
        merged.cas_latencies_us.extend(stats.cas_latencies_us);
        merged.cas_committed += stats.cas_committed;
        merged.cas_mismatches += stats.cas_mismatches;
        merged.truncates += stats.truncates;
        merged.errors += stats.errors;
    }

    merged.cas_latencies_us.sort_unstable();
    let lat = &merged.cas_latencies_us;
    let runtime_s = args.runtime.as_secs_f64();
    let ms = |us: u64| f64::cast_lossy(us) / 1000.0;
    let result = serde_json::json!({
        "num_shards": args.num_shards,
        "pool_size": args.pool_size,
        "pipeline_connections": pipeline_connections,
        "pipeline_depth": pipeline_depth,
        "read_committed": args.read_committed,
        "data_size": args.data_size,
        "runtime_s": runtime_s,
        "cas_committed": merged.cas_committed,
        "cas_per_s": f64::cast_lossy(merged.cas_committed) / runtime_s,
        "cas_mismatches": merged.cas_mismatches,
        "truncates": merged.truncates,
        "errors": merged.errors,
        "p50_ms": ms(percentile(lat, 0.50)),
        "p95_ms": ms(percentile(lat, 0.95)),
        "p99_ms": ms(percentile(lat, 0.99)),
        "max_ms": ms(lat.last().copied().unwrap_or(0)),
    });
    println!("RESULT {result}");
    Ok(())
}

async fn init_shard(
    consensus: &dyn Consensus,
    key: &str,
    data: &Bytes,
) -> Result<(), anyhow::Error> {
    // Transient errors (e.g. pool contention while hundreds of shards
    // initialize at once) get a few retries; a mismatch on a fresh random key
    // is a bug.
    for attempt in 0.. {
        let new = VersionedData {
            seqno: SeqNo::minimum(),
            data: data.clone(),
        };
        match consensus.compare_and_set(key, new).await {
            Ok(CaSResult::Committed) => return Ok(()),
            Ok(CaSResult::ExpectationMismatch) => {
                bail!("mismatch initializing fresh shard {key}")
            }
            Err(e) if attempt < 10 => {
                tracing::warn!("init {key} attempt {attempt}: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    unreachable!()
}

async fn shard_worker(
    consensus: &dyn Consensus,
    key: &str,
    data: &Bytes,
    truncate_every: u64,
    warmup_end: Instant,
    deadline: Instant,
    progress: &AtomicU64,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut seqno = SeqNo::minimum();
    while Instant::now() < deadline {
        let next = seqno.next();
        let start = Instant::now();
        let res = consensus
            .compare_and_set(
                key,
                VersionedData {
                    seqno: next,
                    data: data.clone(),
                },
            )
            .await;
        let elapsed = start.elapsed();
        let measured = start >= warmup_end;
        match res {
            Ok(CaSResult::Committed) => {
                seqno = next;
                progress.fetch_add(1, Ordering::Relaxed);
                if measured {
                    stats.cas_committed += 1;
                    stats
                        .cas_latencies_us
                        .push(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
                }
                if seqno.0 % truncate_every == 0 && seqno.0 >= 2 * truncate_every {
                    match consensus
                        .truncate(key, SeqNo(seqno.0 - truncate_every))
                        .await
                    {
                        Ok(_) => {
                            if measured {
                                stats.truncates += 1;
                            }
                        }
                        Err(_) => stats.errors += 1,
                    }
                }
            }
            // Nobody else writes to this shard, so a mismatch means an earlier
            // "indeterminate" error actually committed. Resync from head.
            Ok(CaSResult::ExpectationMismatch) => {
                stats.cas_mismatches += 1;
                if let Ok(Some(head)) = consensus.head(key).await {
                    seqno = head.seqno;
                }
            }
            Err(_) => {
                stats.errors += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    stats
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = usize::cast_lossy((f64::cast_lossy(sorted.len() - 1) * q).round());
    sorted[idx]
}
