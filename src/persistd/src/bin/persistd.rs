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

use anyhow::Context;
use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::cfg::ConsensusConfig;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_committer::metrics::CommitterMetrics;
use mz_persist_committer::{CommitterConfig, start_committer};
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

    /// TTL at which the committer re-reads subscribed shards from the
    /// backing store. Mirrors `persist_committer_cache_refresh_interval`.
    #[clap(
        long,
        env = "CACHE_REFRESH_INTERVAL",
        value_parser = humantime_duration_parser,
        default_value = "5s",
    )]
    cache_refresh_interval: Duration,

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
    info!(listen_addr = %args.listen_addr, "persistd starting");

    // Build the same PersistConfig surface the in-envd committer uses so
    // dyncfg knobs (timeouts, retry parameters) keep their defaults.
    let persist_cfg = PersistConfig::new_default_configs(&BUILD_INFO, SYSTEM_TIME.clone());
    let persist_metrics = Arc::new(Metrics::new(&persist_cfg, &metrics_registry));
    let configs = Arc::clone(&persist_cfg.configs);

    let consensus = mz_ore::retry::Retry::default()
        .clamp_backoff(Duration::from_secs(2))
        .max_duration(Duration::from_secs(60))
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

    let committer_metrics = CommitterMetrics::register(&metrics_registry);
    let committer_config = CommitterConfig {
        listen_addr: args.listen_addr,
        max_cached_shards: args.max_cached_shards,
        cache_refresh_interval: args.cache_refresh_interval,
        heartbeat_interval: args.stats_heartbeat_interval,
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
