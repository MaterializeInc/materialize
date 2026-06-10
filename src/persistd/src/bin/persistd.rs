// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Standalone persist committer service.
//!
//! `persistd` runs the same [`mz_persist_committer::PersistCommitter`] logic
//! that `environmentd` embeds, but in its own process. Clusterds (and a
//! co-deployed `environmentd`) point `--persist-committer-url` at this
//! service. Useful for isolating consensus traffic, for separately
//! provisioning the committer pool, and as a stepping stone toward fully
//! moving persist out of `environmentd`.
//!
//! See `doc/developer/design/20260527_persist_committer.md`.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::cfg::ConsensusConfig;
use mz_persist::location::{
    CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
};
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_committer::metrics::CommitterMetrics;
use mz_persist_committer::{CommitterConfig, start_committer};
use tokio::sync::Mutex;
use tracing::{info, warn};

pub const BUILD_INFO: BuildInfo = build_info!();

#[derive(Debug, clap::Parser)]
#[clap(name = "persistd", about = "Standalone persist committer", long_about = None)]
struct Args {
    /// TCP address the committer's gRPC server binds to. Clusterds and any
    /// environmentd in the same deployment point `--persist-committer-url`
    /// at this address.
    #[clap(
        long,
        env = "LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6882"
    )]
    listen_addr: SocketAddr,

    /// URL of the consensus backing store (e.g. CockroachDB / Postgres).
    #[clap(long, env = "CONSENSUS_URL", value_name = "URL")]
    consensus_url: SensitiveUrl,

    /// Hard cap on the number of shards held in the committer's in-memory
    /// cache. Mirrors the `persist_committer_max_cached_shards` dyncfg used
    /// by the in-envd committer.
    #[clap(long, env = "MAX_CACHED_SHARDS", default_value = "10000")]
    max_cached_shards: usize,

    /// Interval between INFO-level stats heartbeat lines. `0s` disables.
    #[clap(
        long,
        env = "STATS_HEARTBEAT_INTERVAL",
        value_parser = humantime_duration_parser,
        default_value = "30s",
    )]
    stats_heartbeat_interval: Duration,

    /// When > 0, coalesce concurrent CaS requests into batched backing
    /// statements of at most this many elements. 0 disables coalescing.
    #[clap(long, env = "COALESCE_MAX_BATCH", default_value = "0")]
    coalesce_max_batch: usize,

    /// Maximum number of coalesced batches in flight against the backing
    /// store at once. Only meaningful with `--coalesce-max-batch > 0`.
    #[clap(long, env = "COALESCE_CONCURRENCY", default_value = "8")]
    coalesce_concurrency: usize,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

fn humantime_duration_parser(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_"),
        enable_version_flag: true,
    });

    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("failed building tokio runtime");

    let metrics_registry = MetricsRegistry::new();
    let _tracing_handle = runtime
        .block_on(args.tracing.configure_tracing(
            StaticTracingConfig {
                service_name: "persistd",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        ))
        .expect("failed to init tracing");
    runtime.block_on(mz_alloc::register_metrics_into(&metrics_registry));

    if let Err(err) = runtime.block_on(run(args, metrics_registry)) {
        panic!("persistd: fatal: {}", err.display_with_causes());
    }
}

async fn run(args: Args, metrics_registry: MetricsRegistry) -> Result<(), anyhow::Error> {
    // `SensitiveUrl::Display` masks any password component, so this is safe
    // to log verbatim. Operators commonly hit "wrong URL" misconfigurations
    // (search_path missing, host typo); recording the resolved value at
    // INFO removes the guesswork.
    info!(
        listen_addr = %args.listen_addr,
        consensus_url = %args.consensus_url,
        "persistd starting",
    );

    // Build the same PersistConfig surface the in-envd committer uses so
    // dyncfg knobs (timeouts, retry parameters) keep their defaults.
    let persist_cfg = PersistConfig::new_default_configs(&BUILD_INFO, SYSTEM_TIME.clone());
    let persist_metrics = Arc::new(Metrics::new(&persist_cfg, &metrics_registry));
    let configs = Arc::clone(&persist_cfg.configs);

    // Retry indefinitely. The metadata store (CRDB / Postgres) only signals
    // `service_started` to compose dependents, not `service_healthy`, so
    // persistd can outrun the backend's listener-ready point by tens of
    // seconds. A bounded retry would kill the container while the operator
    // is still waiting for the backend, racing rather than waiting; loop
    // forever and let compose's own healthcheck timeout govern.
    let consensus = mz_ore::retry::Retry::default()
        .clamp_backoff(Duration::from_secs(2))
        .retry_async(|_| async {
            let consensus_cfg = ConsensusConfig::try_from(
                &args.consensus_url,
                Box::new(persist_cfg.clone()),
                persist_metrics.postgres_consensus.clone(),
                Arc::clone(&configs),
            )?;
            consensus_cfg
                .open()
                .await
                .inspect_err(|e| warn!("waiting for consensus backend: {e}"))
        })
        .await
        .context("opening consensus backend")?;
    info!("consensus backend ready");

    // Recreate the schema in place if the backing store reports the consensus
    // relation is missing, rather than failing forever. See
    // `SelfHealingConsensus`. The factory rebuilds the same config the boot
    // path used; opening it re-runs the idempotent schema DDL.
    let reopen: ReopenFn = {
        let consensus_url = args.consensus_url.clone();
        let persist_cfg = persist_cfg.clone();
        let postgres_metrics = persist_metrics.postgres_consensus.clone();
        let configs = Arc::clone(&configs);
        Box::new(move || {
            let consensus_url = consensus_url.clone();
            let persist_cfg = persist_cfg.clone();
            let postgres_metrics = postgres_metrics.clone();
            let configs = Arc::clone(&configs);
            Box::pin(async move {
                let consensus_cfg = ConsensusConfig::try_from(
                    &consensus_url,
                    Box::new(persist_cfg),
                    postgres_metrics,
                    configs,
                )?;
                let consensus: Arc<dyn Consensus + Send + Sync> = consensus_cfg.open().await?;
                Ok(consensus)
            })
        })
    };
    let consensus: Arc<dyn Consensus + Send + Sync> = Arc::new(SelfHealingConsensus {
        inner: consensus,
        reopen,
        heal_lock: Mutex::new(()),
    });

    let committer_metrics = CommitterMetrics::register(&metrics_registry);
    let committer_config = CommitterConfig {
        listen_addr: args.listen_addr,
        max_cached_shards: args.max_cached_shards,
        heartbeat_interval: args.stats_heartbeat_interval,
        coalesce_max_batch: args.coalesce_max_batch,
        coalesce_concurrency: args.coalesce_concurrency,
    };
    let _handle = start_committer(consensus, committer_metrics, committer_config)
        .context("starting committer")?;

    // Block until the process is signaled to exit. Dropping `_handle` aborts
    // the gRPC server and refresh tasks.
    tokio::signal::ctrl_c()
        .await
        .context("waiting for shutdown signal")?;
    info!("persistd shutting down");
    Ok(())
}

/// A consensus handle, as returned by a [`ReopenFn`].
type DynConsensus = Arc<dyn Consensus + Send + Sync>;

/// The future produced by a [`ReopenFn`].
type ReopenFuture = Pin<Box<dyn Future<Output = anyhow::Result<DynConsensus>> + Send>>;

/// Factory that re-opens the consensus backend, re-running the idempotent
/// schema DDL in `PostgresConsensus::open`. Used by [`SelfHealingConsensus`].
type ReopenFn = Box<dyn Fn() -> ReopenFuture + Send + Sync>;

/// A [`Consensus`] wrapper that recreates the consensus schema in place when
/// the backing store reports the relation is gone (SQLSTATE 42P01), rather
/// than failing forever.
///
/// `persistd`'s schema is created once, in `PostgresConsensus::open`. If the
/// backing store is reset out from under a running `persistd` (a test harness
/// wiping the metadata store between phases, a freshly provisioned store, ...)
/// every op then fails with `undefined_table`, which no retry can clear.
/// Reopening re-runs the idempotent `CREATE SCHEMA/TABLE IF NOT EXISTS` DDL,
/// restoring service against the now-empty store without restarting the
/// process. The freshly opened handle is discarded; the original connection
/// pool finds the recreated table on retry.
///
/// This policy is intentionally local to the standalone binary. The in-process
/// committer in `environmentd` shares the committer code but must never
/// silently recreate a missing schema, which would mask catastrophic data
/// loss; it does not install this wrapper.
struct SelfHealingConsensus {
    inner: Arc<dyn Consensus + Send + Sync>,
    reopen: ReopenFn,
    /// Serializes heals so a burst of concurrent `undefined_table` errors does
    /// not trigger a stampede of reopens. The DDL is idempotent, so redundant
    /// reopens would be harmless, but serializing keeps it tidy.
    heal_lock: Mutex<()>,
}

impl std::fmt::Debug for SelfHealingConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelfHealingConsensus")
            .finish_non_exhaustive()
    }
}

impl SelfHealingConsensus {
    /// If `err` reports the consensus relation is missing, reopen the backing
    /// store to recreate the schema and return `true` so the caller retries.
    /// Returns `false` for any other error, or if the reopen itself failed.
    async fn maybe_heal(&self, err: &ExternalError) -> bool {
        if !err.is_undefined_table() {
            return false;
        }
        let _guard = self.heal_lock.lock().await;
        match (self.reopen)().await {
            Ok(_fresh) => {
                info!(
                    "consensus relation was missing; recreated the schema after the backing \
                     store was reset"
                );
                true
            }
            Err(e) => {
                warn!(
                    error = %e.display_with_causes(),
                    "failed to reopen consensus to recreate the missing schema",
                );
                false
            }
        }
    }
}

#[async_trait]
impl Consensus for SelfHealingConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        // Administrative path; not retried on a missing schema. The reads
        // clients drive (head/scan/cas) are what recover the schema.
        self.inner.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        match self.inner.head(key).await {
            Err(e) if self.maybe_heal(&e).await => self.inner.head(key).await,
            other => other,
        }
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        match self.inner.compare_and_set(key, new.clone()).await {
            Err(e) if self.maybe_heal(&e).await => self.inner.compare_and_set(key, new).await,
            other => other,
        }
    }

    async fn compare_and_set_multi(
        &self,
        batch: Vec<(String, VersionedData)>,
    ) -> Result<Vec<CaSResult>, ExternalError> {
        match self.inner.compare_and_set_multi(batch.clone()).await {
            Err(e) if self.maybe_heal(&e).await => self.inner.compare_and_set_multi(batch).await,
            other => other,
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        match self.inner.scan(key, from, limit).await {
            Err(e) if self.maybe_heal(&e).await => self.inner.scan(key, from, limit).await,
            other => other,
        }
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        match self.inner.truncate(key, seqno).await {
            Err(e) if self.maybe_heal(&e).await => self.inner.truncate(key, seqno).await,
            other => other,
        }
    }
}
