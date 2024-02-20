// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::any::Any;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::{stream, StreamExt, TryStreamExt};
use mz_dyncfg::ConfigSet;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::location::{Blob, Consensus, ExternalError};
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::{Codec, Codec64};
use prometheus::proto::{MetricFamily, MetricType};
use timely::progress::Timestamp;
use tracing::info;

use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cfg::all_dyncfgs;
use crate::cli::args::{make_blob, make_consensus, StateArgs, StoreArgs};
use crate::internal::compact::{CompactConfig, CompactReq, Compactor};
use crate::internal::encoding::Schemas;
use crate::internal::gc::{GarbageCollector, GcReq};
use crate::internal::machine::Machine;
use crate::internal::trace::{ApplyMergeResult, FueledMergeRes};
use crate::rpc::NoopPubSubSender;
use crate::write::WriterId;
use crate::{Diagnostics, Metrics, PersistConfig, ShardId, StateVersions, BUILD_INFO};

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
    /// Manually kick off a GC run for a shard.
    ForceGc(ForceGcArgs),
    /// Manually finalize an unfinalized shard.
    Finalize(StateArgs),
    /// Attempt to ensure that all the files referenced by consensus are available
    /// in Blob.
    RestoreBlob(RestoreBlobArgs),
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

/// Manually completes all fueled compactions in a shard.
#[derive(Debug, clap::Parser)]
pub(crate) struct ForceGcArgs {
    #[clap(flatten)]
    state: StateArgs,
}

/// Attempt to restore all the blobs that are referenced by the current state of consensus.
#[derive(Debug, clap::Parser)]
pub(crate) struct RestoreBlobArgs {
    #[clap(flatten)]
    state: StoreArgs,

    /// The number of concurrent restore operations to run at once.
    #[clap(long, default_value_t = 16)]
    concurrency: usize,
}

/// Runs the given read-write admin command.
pub async fn run(command: AdminArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::ForceCompaction(args) => {
            let shard_id = ShardId::from_str(&args.state.shard_id).expect("invalid shard id");
            let configs = all_dyncfgs(ConfigSet::default());
            // TODO: Fetch the latest values of these configs from Launch Darkly.
            let cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), configs);
            if args.compaction_memory_bound_bytes > 0 {
                cfg.dynamic
                    .set_compaction_memory_bound_bytes(args.compaction_memory_bound_bytes);
            }
            let metrics_registry = MetricsRegistry::new();
            let () = force_compaction::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>(
                cfg,
                &metrics_registry,
                shard_id,
                &args.state.consensus_uri,
                &args.state.blob_uri,
                Arc::new(TodoSchema::default()),
                Arc::new(TodoSchema::default()),
                command.commit,
            )
            .await?;
            info_log_non_zero_metrics(&metrics_registry.gather());
        }
        Command::ForceGc(args) => {
            let shard_id = ShardId::from_str(&args.state.shard_id).expect("invalid shard id");
            let configs = all_dyncfgs(ConfigSet::default());
            // TODO: Fetch the latest values of these configs from Launch Darkly.
            let cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), configs);
            let metrics_registry = MetricsRegistry::new();
            // We don't actually care about the return value here, but we do need to prevent
            // the shard metrics from being dropped before they're reported below.
            let _machine = force_gc(
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
        Command::Finalize(args) => {
            let StateArgs {
                shard_id,
                consensus_uri,
                blob_uri,
            } = args;
            let shard_id = ShardId::from_str(&shard_id).expect("invalid shard id");
            let commit = command.commit;

            let configs = all_dyncfgs(ConfigSet::default());
            // TODO: Fetch the latest values of these configs from Launch Darkly.
            let cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), configs);
            let metrics_registry = MetricsRegistry::new();
            let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));
            let consensus =
                make_consensus(&cfg, &consensus_uri, commit, Arc::clone(&metrics)).await?;
            let blob = make_blob(&cfg, &blob_uri, commit, Arc::clone(&metrics)).await?;
            let mut machine =
                make_machine(&cfg, consensus, blob, metrics, shard_id, commit).await?;
            let maintenance = machine.become_tombstone().await?;
            if !maintenance.is_empty() {
                info!("ignoring non-empty requested maintenance: {maintenance:?}")
            }
            info_log_non_zero_metrics(&metrics_registry.gather());
        }
        Command::RestoreBlob(args) => {
            let RestoreBlobArgs {
                state:
                    StoreArgs {
                        consensus_uri,
                        blob_uri,
                    },
                concurrency,
            } = args;
            let commit = command.commit;
            let configs = all_dyncfgs(ConfigSet::default());
            // TODO: Fetch the latest values of these configs from Launch Darkly.
            let cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), configs);
            let metrics_registry = MetricsRegistry::new();
            let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));
            let consensus =
                make_consensus(&cfg, &consensus_uri, commit, Arc::clone(&metrics)).await?;
            let blob = make_blob(&cfg, &blob_uri, commit, Arc::clone(&metrics)).await?;
            let versions = StateVersions::new(
                cfg.clone(),
                Arc::clone(&consensus),
                Arc::clone(&blob),
                metrics,
            );

            let not_restored: Vec<_> = consensus
                .list_keys()
                .flat_map_unordered(concurrency, |shard| {
                    stream::once(Box::pin(async {
                        let shard_id = shard?;
                        let shard_id = ShardId::from_str(&shard_id).expect("invalid shard id");
                        let start = Instant::now();
                        info!("Restoring blob state for shard {shard_id}.",);
                        let shard_not_restored = crate::internal::restore::restore_blob(
                            &versions,
                            blob.as_ref(),
                            &cfg.build_version,
                            shard_id,
                        )
                        .await?;
                        info!(
                            "Restored blob state for shard {shard_id}; {} errors, {:?} elapsed.",
                            shard_not_restored.len(),
                            start.elapsed()
                        );
                        Ok::<_, ExternalError>(shard_not_restored)
                    }))
                })
                .try_fold(vec![], |mut a, b| async move {
                    a.extend(b);
                    Ok(a)
                })
                .await?;

            info_log_non_zero_metrics(&metrics_registry.gather());
            if !not_restored.is_empty() {
                bail!("referenced blobs were not restored: {not_restored:#?}")
            }
        }
    }
    Ok(())
}

pub(crate) fn info_log_non_zero_metrics(metric_families: &[MetricFamily]) {
    for mf in metric_families {
        for m in mf.get_metric() {
            let val = match mf.get_field_type() {
                MetricType::COUNTER => m.get_counter().get_value(),
                MetricType::GAUGE => m.get_gauge().get_value(),
                x => {
                    info!("unhandled {} metric type: {:?}", mf.get_name(), x);
                    continue;
                }
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
pub async fn force_compaction<K, V, T, D>(
    cfg: PersistConfig,
    metrics_registry: &MetricsRegistry,
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    commit: bool,
) -> Result<(), anyhow::Error>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    let metrics = Arc::new(Metrics::new(&cfg, metrics_registry));
    let consensus = make_consensus(&cfg, consensus_uri, commit, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, blob_uri, commit, Arc::clone(&metrics)).await?;

    let mut machine = make_typed_machine::<K, V, T, D>(
        &cfg,
        consensus,
        Arc::clone(&blob),
        Arc::clone(&metrics),
        shard_id,
        commit,
    )
    .await?;

    let writer_id = WriterId::new();

    let mut attempt = 0;
    'outer: loop {
        machine.applier.fetch_and_update_state(None).await;
        let reqs = machine.applier.all_fueled_merge_reqs();
        info!("attempt {}: got {} compaction reqs", attempt, reqs.len());
        for (idx, req) in reqs.clone().into_iter().enumerate() {
            let req = CompactReq {
                shard_id,
                desc: req.desc,
                inputs: req.inputs.iter().map(|b| b.batch.clone()).collect(),
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
            let schemas = Schemas {
                key: Arc::clone(&key_schema),
                val: Arc::clone(&val_schema),
            };

            let res = Compactor::<K, V, T, D>::compact(
                CompactConfig::new(&cfg, &writer_id),
                Arc::clone(&blob),
                Arc::clone(&metrics),
                Arc::clone(&machine.applier.shard_metrics),
                Arc::new(IsolatedRuntime::default()),
                req,
                schemas,
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

async fn make_machine(
    cfg: &PersistConfig,
    consensus: Arc<dyn Consensus + Send + Sync>,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    commit: bool,
) -> anyhow::Result<Machine<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>> {
    make_typed_machine::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>(
        cfg, consensus, blob, metrics, shard_id, commit,
    )
    .await
}

async fn make_typed_machine<K, V, T, D>(
    cfg: &PersistConfig,
    consensus: Arc<dyn Consensus + Send + Sync>,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    commit: bool,
) -> anyhow::Result<Machine<K, V, T, D>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    let state_versions = Arc::new(StateVersions::new(
        cfg.clone(),
        consensus,
        blob,
        Arc::clone(&metrics),
    ));

    // Prime the K V codec magic
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;

    loop {
        let state_res = state_versions
            .fetch_current_state::<u64>(&shard_id, versions.0.clone())
            .await
            .check_codecs::<crate::cli::inspect::K, crate::cli::inspect::V, i64>(&shard_id);
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
        let safe_version_change = if commit {
            cfg.build_version == state.applier_version
        } else {
            // We never actually write out state changes, so increasing the version is okay.
            cfg.build_version >= state.applier_version
        };
        if !safe_version_change {
            // We could add a flag to override this check, if that comes up.
            return Err(anyhow!("version of this tool {} does not match version of state {} when --commit is {commit}. bailing so we don't corrupt anything", cfg.build_version, state.applier_version));
        }
        break;
    }

    let machine = Machine::<K, V, T, D>::new(
        cfg.clone(),
        shard_id,
        Arc::clone(&metrics),
        state_versions,
        Arc::new(StateCache::new(cfg, metrics, Arc::new(NoopPubSubSender))),
        Arc::new(NoopPubSubSender),
        Arc::new(IsolatedRuntime::default()),
        Diagnostics::from_purpose("admin"),
    )
    .await?;

    Ok(machine)
}

async fn force_gc(
    cfg: PersistConfig,
    metrics_registry: &MetricsRegistry,
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
    commit: bool,
) -> anyhow::Result<Box<dyn Any>> {
    let metrics = Arc::new(Metrics::new(&cfg, metrics_registry));
    let consensus = make_consensus(&cfg, consensus_uri, commit, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, blob_uri, commit, Arc::clone(&metrics)).await?;
    let mut machine = make_machine(&cfg, consensus, blob, metrics, shard_id, commit).await?;
    let gc_req = GcReq {
        shard_id,
        new_seqno_since: machine.applier.seqno_since(),
    };
    let (maintenance, _stats) = GarbageCollector::gc_and_truncate(&mut machine, gc_req).await;
    if !maintenance.is_empty() {
        info!("ignoring non-empty requested maintenance: {maintenance:?}")
    }

    Ok(Box::new(machine))
}
