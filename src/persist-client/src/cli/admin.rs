// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{Blob, Consensus};
use prometheus::proto::{MetricFamily, MetricType};
use tracing::info;

use crate::async_runtime::CpuHeavyRuntime;
use crate::cli::inspect::StateArgs;
use crate::internal::compact::{CompactReq, Compactor};
use crate::internal::machine::Machine;
use crate::internal::metrics::{MetricsBlob, MetricsConsensus};
use crate::internal::trace::{ApplyMergeResult, FueledMergeRes};
use crate::write::WriterId;
use crate::{Metrics, PersistConfig, ShardId, StateVersions, BUILD_INFO};

/// Commands for read-write administration of persist state
#[derive(Debug, clap::Args)]
pub struct AdminArgs {
    #[clap(subcommand)]
    command: Command,

    /// Whether to commit any modifications (defaults to dry run).
    #[clap(long)]
    pub(crate) commit: bool,
}

/// Individual subcommands of admin
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Manually completes all fueled compactions in a shard.
    ForceCompaction(ForceCompactionArgs),
}

/// Manually completes all fueled compactions in a shard.
#[derive(Debug, clap::Parser)]
pub(crate) struct ForceCompactionArgs {
    #[clap(flatten)]
    state: StateArgs,

    /// An upper bound on compaction's memory consumption.
    #[clap(long, default_value_t = 0)]
    compaction_memory_bound_bytes: usize,
}

/// Runs the given read-write admin command.
pub async fn run(command: AdminArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::ForceCompaction(args) => {
            let shard_id = ShardId::from_str(&args.state.shard_id).expect("invalid shard id");
            let mut cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone());
            if args.compaction_memory_bound_bytes > 0 {
                cfg.compaction_memory_bound_bytes = args.compaction_memory_bound_bytes;
            }
            let metrics_registry = MetricsRegistry::new();
            let () = force_compaction(
                cfg,
                &metrics_registry,
                shard_id,
                &args.state.consensus_uri,
                &args.state.blob_uri,
                command.commit,
            )
            .await?;
            info_log_non_zero_metrics(&metrics_registry.gather());
        }
    }
    Ok(())
}

fn info_log_non_zero_metrics(metric_families: &[MetricFamily]) {
    for mf in metric_families {
        for m in mf.get_metric() {
            let val = match mf.get_field_type() {
                MetricType::COUNTER => m.get_counter().get_value(),
                MetricType::GAUGE => m.get_gauge().get_value(),
                x => unimplemented!("unhandled metric type: {:?}", x),
            };
            if val == 0.0 {
                continue;
            }
            let label_pairs = m.get_label();
            let mut labels = String::new();
            if !label_pairs.is_empty() {
                labels.push_str("{");
                for lb in label_pairs {
                    if labels != "{" {
                        labels.push_str(",");
                    }
                    labels.push_str(lb.get_name());
                    labels.push_str(":");
                    labels.push_str(lb.get_value());
                }
                labels.push_str("}");
            }
            info!("{}{} {}", mf.get_name(), labels, val);
        }
    }
}

/// Manually completes all fueled compactions in a shard.
pub async fn force_compaction(
    cfg: PersistConfig,
    metrics_registry: &MetricsRegistry,
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
    commit: bool,
) -> Result<(), anyhow::Error> {
    let metrics = Arc::new(Metrics::new(&cfg, metrics_registry));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let consensus: Arc<dyn Consensus + Send + Sync> =
        Arc::new(MetricsConsensus::new(consensus, Arc::clone(&metrics)));
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;
    let blob: Arc<dyn Blob + Send + Sync> = Arc::new(MetricsBlob::new(blob, Arc::clone(&metrics)));

    let state_versions = Arc::new(StateVersions::new(
        cfg.clone(),
        consensus,
        Arc::clone(&blob),
        Arc::clone(&metrics),
    ));

    // Prime the K V codec magic
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    loop {
        let state_res = state_versions
            .fetch_current_state::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>(
                &shard_id,
                versions.0.clone(),
            )
            .await;
        let state = match state_res {
            Ok(state) => state,
            Err(codec) => {
                let mut kvtd = crate::cli::inspect::KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
                continue;
            }
        };
        // This isn't the perfect place to put this check, the ideal would be in
        // the apply_unbatched_cmd loop, but I don't want to pollute the prod
        // code with this logic.
        if cfg.build_version != state.applier_version {
            // We could add a flag to override this check, if that comes up.
            return Err(anyhow!("version of this tool {} does not match version of state {}. bailing so we don't corrupt anything", cfg.build_version, state.applier_version));
        }
        break;
    }

    let mut machine = Machine::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>::new(
        cfg.clone(),
        shard_id,
        Arc::clone(&metrics),
        Arc::clone(&state_versions),
    )
    .await?;

    let writer_id = WriterId::new();
    info!("registering writer {}", writer_id);
    // We don't bother expiring this writer in various error codepaths, instead
    // letting it time out. /shrug
    let _ = machine
        .register_writer(
            &writer_id,
            "persistcli admin force-compaction",
            cfg.writer_lease_duration,
            (cfg.now)(),
        )
        .await;

    let mut attempt = 0;
    'outer: loop {
        machine.fetch_and_update_state().await;
        let reqs = machine.state().collections.trace.all_fueled_merge_reqs();
        info!("attempt {}: got {} compaction reqs", attempt, reqs.len());
        for (idx, req) in reqs.clone().into_iter().enumerate() {
            let req = CompactReq {
                shard_id,
                desc: req.desc,
                inputs: req.inputs,
            };
            let parts = req.inputs.iter().map(|x| x.parts.len()).sum::<usize>();
            let bytes = req
                .inputs
                .iter()
                .flat_map(|x| x.parts.iter().map(|x| x.encoded_size_bytes))
                .sum::<usize>();
            let start = Instant::now();
            info!(
                "attempt {} req {}: compacting {} batches {} in parts {} totaling bytes: lower={:?} upper={:?} since={:?}",
                attempt,
                idx,
                req.inputs.len(),
                parts,
                bytes,
                req.desc.lower().elements(),
                req.desc.upper().elements(),
                req.desc.since().elements(),
            );
            if !commit {
                info!("skipping compaction because --commit is not set");
                continue;
            }
            let res =
                Compactor::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>::compact(
                    cfg.clone(),
                    Arc::clone(&blob),
                    Arc::clone(&metrics),
                    Arc::new(CpuHeavyRuntime::new()),
                    req,
                    writer_id.clone(),
                )
                .await?;
            info!(
                "attempt {} req {}: compacted into {} parts {} bytes in {:?}",
                attempt,
                idx,
                res.output.parts.len(),
                res.output
                    .parts
                    .iter()
                    .map(|x| x.encoded_size_bytes)
                    .sum::<usize>(),
                start.elapsed(),
            );
            let (apply_res, maintenance) = machine
                .merge_res(&FueledMergeRes { output: res.output })
                .await;
            if !maintenance.is_empty() {
                info!("ignoring non-empty requested maintenance: {maintenance:?}")
            }
            match apply_res {
                ApplyMergeResult::AppliedExact | ApplyMergeResult::AppliedSubset => {
                    info!("attempt {} req {}: {:?}", attempt, idx, apply_res);
                }
                ApplyMergeResult::NotAppliedInvalidSince
                | ApplyMergeResult::NotAppliedNoMatch
                | ApplyMergeResult::NotAppliedTooManyUpdates => {
                    info!(
                        "attempt {} req {}: {:?} trying again",
                        attempt, idx, apply_res
                    );
                    attempt += 1;
                    continue 'outer;
                }
            }
        }
        info!("attempt {}: did {} compactions", attempt, reqs.len());
        let _ = machine.expire_writer(&writer_id).await;
        info!("expired writer {}", writer_id);
        return Ok(());
    }
}
