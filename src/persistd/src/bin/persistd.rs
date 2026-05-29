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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
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
use tokio::sync::Notify;
use tracing::{error, info, warn};

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

    // Watch for the consensus relation disappearing (e.g. the backing store
    // was reset). See `TerminateOnMissingSchema`.
    let schema_lost = Arc::new(Notify::new());
    let consensus: Arc<dyn Consensus + Send + Sync> = Arc::new(TerminateOnMissingSchema {
        inner: consensus,
        schema_lost: Arc::clone(&schema_lost),
    });

    let committer_metrics = CommitterMetrics::register(&metrics_registry);
    let committer_config = CommitterConfig {
        listen_addr: args.listen_addr,
        max_cached_shards: args.max_cached_shards,
        heartbeat_interval: args.stats_heartbeat_interval,
    };
    let _handle = start_committer(consensus, committer_metrics, committer_config)
        .context("starting committer")?;

    // Block until the process is signaled to exit or the consensus schema
    // vanishes. Dropping `_handle` aborts the gRPC server and refresh tasks.
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res.context("waiting for shutdown signal")?;
            info!("persistd shutting down");
            Ok(())
        }
        // The wrapper already logged the underlying cause at error level.
        // Return an error so the process exits non-zero and its supervisor
        // restarts it, re-running schema creation in `open`.
        _ = schema_lost.notified() => Err(anyhow!(
            "consensus relation vanished from the backing store; exiting so the supervisor \
             restarts persistd and recreates the schema"
        )),
    }
}

/// Fire `schema_lost` and log if `err` reports the consensus relation is gone.
fn flag_missing_schema(err: &ExternalError, schema_lost: &Notify) {
    if err.is_undefined_table() {
        error!(
            error = %err.display_with_causes(),
            "consensus relation does not exist; the backing store appears to have been reset. \
             Terminating so the supervisor restarts persistd and recreates the schema",
        );
        schema_lost.notify_one();
    }
}

/// A [`Consensus`] wrapper that detects the backing store losing the consensus
/// relation and signals `persistd` to terminate.
///
/// `persistd` creates its schema exactly once, in `PostgresConsensus::open`,
/// during startup. If the backing store is reset under a running `persistd`
/// the relation disappears and every op then fails forever with SQLSTATE
/// 42P01 (`undefined_table`); no in-process retry can clear it. Rather than
/// spin silently, the standalone process treats this as fatal: this wrapper
/// fires `schema_lost`, `run` exits non-zero, and the supervisor restarts
/// `persistd`, which recreates the schema.
///
/// This policy is intentionally local to the standalone binary. The in-process
/// committer in `environmentd` shares the same committer code but must never
/// self-terminate, so it does not install this wrapper.
#[derive(Debug)]
struct TerminateOnMissingSchema {
    inner: Arc<dyn Consensus + Send + Sync>,
    schema_lost: Arc<Notify>,
}

#[async_trait]
impl Consensus for TerminateOnMissingSchema {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let schema_lost = Arc::clone(&self.schema_lost);
        Box::pin(self.inner.list_keys().inspect(move |item| {
            if let Err(err) = item {
                flag_missing_schema(err, &schema_lost);
            }
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let res = self.inner.head(key).await;
        if let Err(err) = &res {
            flag_missing_schema(err, &self.schema_lost);
        }
        res
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let res = self.inner.compare_and_set(key, new).await;
        if let Err(err) = &res {
            flag_missing_schema(err, &self.schema_lost);
        }
        res
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let res = self.inner.scan(key, from, limit).await;
        if let Err(err) = &res {
            flag_missing_schema(err, &self.schema_lost);
        }
        res
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let res = self.inner.truncate(key, seqno).await;
        if let Err(err) = &res {
            flag_missing_schema(err, &self.schema_lost);
        }
        res
    }
}
