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
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::{StreamExt, TryStreamExt, stream};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::location::{Blob, Consensus, ExternalError};
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::{Codec, Codec64};
use prometheus::proto::{MetricFamily, MetricType};
use semver::Version;
use timely::progress::{Antichain, Timestamp};
use tracing::{info, warn};

use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cfg::{COMPACTION_MEMORY_BOUND_BYTES, all_dyncfgs};
use crate::cli::args::{StateArgs, StoreArgs, make_blob, make_consensus};
use crate::cli::inspect::FAKE_OPAQUE_CODEC;
use crate::internal::compact::{CompactConfig, CompactReq, Compactor};
use crate::internal::encoding::Schemas;
use crate::internal::gc::{GarbageCollector, GcReq};
use crate::internal::machine::Machine;
use crate::internal::trace::FueledMergeRes;
use crate::rpc::{NoopPubSubSender, PubSubSender};
use crate::write::{WriteHandle, WriterId};
use crate::{
    BUILD_INFO, Diagnostics, Metrics, PersistClient, PersistConfig, ShardId, StateVersions,
};

/// Commands for read-write administration of persist state
#[derive(Debug, clap::Args)]
pub struct AdminArgs {
    #[clap(subcommand)]
    command: Command,

    /// Whether to commit any modifications (defaults to dry run).
    #[clap(long)]
    pub(crate) commit: bool,

    /// !!DANGER ZONE!! - Has the posibility of breaking production!
    ///
    /// Allows specifying an expected `applier_version` of the shard we're operating on, so we can
    /// modify old/leaked shards.
    #[clap(long)]
    pub(crate) expected_version: Option<String>,
}

/// Individual subcommands of admin
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Manually completes all fueled compactions in a shard.
    ForceCompaction(ForceCompactionArgs),
    /// Manually kick off a GC run for a shard.
    ForceGc(ForceGcArgs),
    /// Manually finalize an unfinalized shard.
    Finalize(FinalizeArgs),
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

/// Manually finalizes a shard.
#[derive(Debug, clap::Parser)]
pub(crate) struct FinalizeArgs {
    #[clap(flatten)]
    state: StateArgs,

    /// Force downgrade the `since` of the shard to the empty antichain.
    #[clap(long, default_value_t = false)]
    force_downgrade_since: bool,

    /// Force downgrade the `upper` of the shard to the empty antichain.
    #[clap(long, default_value_t = false)]
    force_downgrade_upper: bool,
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
            cfg.set_config(
                &COMPACTION_MEMORY_BOUND_BYTES,
                args.compaction_memory_bound_bytes,
            );

            let metrics_registry = MetricsRegistry::new();
            let expected_version = command
                .expected_version
                .as_ref()
                .map(|v| Version::parse(v))
                .transpose()?;
            let () = force_compaction::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>(
                cfg,
                &metrics_registry,
                shard_id,
                &args.state.consensus_uri,
                &args.state.blob_uri,
                Arc::new(TodoSchema::default()),
                Arc::new(TodoSchema::default()),
                command.commit,
                expected_version,
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
            let expected_version = command
                .expected_version
                .as_ref()
                .map(|v| Version::parse(v))
                .transpose()?;
            // We don't actually care about the return value here, but we do need to prevent
            // the shard metrics from being dropped before they're reported below.
            let _machine = force_gc(
                cfg,
                &metrics_registry,
                shard_id,
                &args.state.consensus_uri,
                &args.state.blob_uri,
                command.commit,
                expected_version,
            )
            .await?;
            info_log_non_zero_metrics(&metrics_registry.gather());
        }
        Command::Finalize(args) => {
            let FinalizeArgs {
                state:
                    StateArgs {
                        shard_id,
                        consensus_uri,
                        blob_uri,
                    },
                force_downgrade_since,
                force_downgrade_upper,
            } = args;
            let shard_id = ShardId::from_str(&shard_id).expect("invalid shard id");
            let commit = command.commit;
            let expected_version = command
                .expected_version
                .as_ref()
                .map(|v| Version::parse(v))
                .transpose()?;

            let configs = all_dyncfgs(ConfigSet::default());
            // TODO: Fetch the latest values of these configs from Launch Darkly.
            let cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), configs);
            let metrics_registry = MetricsRegistry::new();
            let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));
            let consensus =
                make_consensus(&cfg, &consensus_uri, commit, Arc::clone(&metrics)).await?;
            let blob = make_blob(&cfg, &blob_uri, commit, Arc::clone(&metrics)).await?;

            // Open a machine so we can read the state of the Opaque, and set
            // our fake codecs.
            let machine = make_machine(
                &cfg,
                Arc::clone(&consensus),
                Arc::clone(&blob),
                Arc::clone(&metrics),
                shard_id,
                commit,
                expected_version,
            )
            .await?;

            if force_downgrade_upper {
                let isolated_runtime = Arc::new(IsolatedRuntime::new(&metrics_registry, None));
                let pubsub_sender: Arc<dyn PubSubSender> = Arc::new(NoopPubSubSender);
                let shared_states = Arc::new(StateCache::new(
                    &cfg,
                    Arc::clone(&metrics),
                    Arc::clone(&pubsub_sender),
                ));

                let persist_client = PersistClient::new(
                    cfg,
                    blob,
                    consensus,
                    metrics,
                    isolated_runtime,
                    shared_states,
                    pubsub_sender,
                )?;
                let diagnostics = Diagnostics {
                    shard_name: shard_id.to_string(),
                    handle_purpose: "persist-cli finalize shard".to_string(),
                };

                let mut write_handle: WriteHandle<
                    crate::cli::inspect::K,
                    crate::cli::inspect::V,
                    u64,
                    i64,
                > = persist_client
                    .open_writer(
                        shard_id,
                        Arc::new(TodoSchema::<crate::cli::inspect::K>::default()),
                        Arc::new(TodoSchema::<crate::cli::inspect::V>::default()),
                        diagnostics,
                    )
                    .await?;
                write_handle.advance_upper(&Antichain::new()).await;
            }

            if force_downgrade_since {
                let (state, _maintenance) = machine
                    .register_critical_reader::<crate::cli::inspect::O>(
                        &crate::PersistClient::CONTROLLER_CRITICAL_SINCE,
                        "persist-cli finalize with force downgrade",
                    )
                    .await;

                // HACK: Internally we have a check that the Opaque is using
                // the correct codec. For the purposes of this command we want
                // to side step that check so we set our reported codec to
                // whatever the current state of the Shard is.
                let expected_opaque = crate::cli::inspect::O::decode(state.opaque.0);
                FAKE_OPAQUE_CODEC
                    .lock()
                    .expect("lockable")
                    .clone_from(&state.opaque_codec);

                let (result, _maintenance) = machine
                    .compare_and_downgrade_since(
                        &crate::PersistClient::CONTROLLER_CRITICAL_SINCE,
                        &expected_opaque,
                        (&expected_opaque, &Antichain::new()),
                    )
                    .await;
                if let Err((actual_opaque, _since)) = result {
                    bail!(
                        "opaque changed, expected: {expected_opaque:?}, actual: {actual_opaque:?}"
                    )
                }
            }

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
                Arc::clone(&metrics),
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
                            &*metrics,
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
                    info!("unhandled {} metric type: {:?}", mf.name(), x);
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
                    labels.push_str(lb.name());
                    labels.push_str(":");
                    labels.push_str(lb.name());
                }
                labels.push_str("}");
            }
            info!("{}{} {}", mf.name(), labels, val);
        }
    }
}

/// Manually completes all fueled compactions in a shard.
pub async fn force_compaction<K, V, T, D>(
    cfg: PersistConfig,
    metrics_registry: &MetricsRegistry,
    shard_id: ShardId,
    consensus_uri: &SensitiveUrl,
    blob_uri: &SensitiveUrl,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    commit: bool,
    expected_version: Option<Version>,
) -> Result<(), anyhow::Error>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    let metrics = Arc::new(Metrics::new(&cfg, metrics_registry));
    let consensus = make_consensus(&cfg, consensus_uri, commit, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, blob_uri, commit, Arc::clone(&metrics)).await?;

    let machine = make_typed_machine::<K, V, T, D>(
        &cfg,
        consensus,
        Arc::clone(&blob),
        Arc::clone(&metrics),
        shard_id,
        commit,
        expected_version,
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
                inputs: req.inputs,
            };
            let parts = req
                .inputs
                .iter()
                .map(|x| x.batch.part_count())
                .sum::<usize>();
            let bytes = req
                .inputs
                .iter()
                .map(|x| x.batch.encoded_size_bytes())
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
                id: None,
                key: Arc::clone(&key_schema),
                val: Arc::clone(&val_schema),
            };

            let res = Compactor::<K, V, T, D>::compact(
                CompactConfig::new(&cfg, shard_id),
                Arc::clone(&blob),
                Arc::clone(&metrics),
                Arc::clone(&machine.applier.shard_metrics),
                Arc::new(IsolatedRuntime::new(
                    metrics_registry,
                    Some(cfg.isolated_runtime_worker_threads),
                )),
                req,
                schemas,
            )
            .await?;
            metrics.compaction.admin_count.inc();
            info!(
                "attempt {} req {}: compacted into {} parts {} bytes in {:?}",
                attempt,
                idx,
                res.output.part_count(),
                res.output.encoded_size_bytes(),
                start.elapsed(),
            );
            let (apply_res, maintenance) = machine
                .merge_res(&FueledMergeRes {
                    output: res.output,
                    input: res.input,
                    new_active_compaction: None,
                })
                .await;
            if !maintenance.is_empty() {
                info!("ignoring non-empty requested maintenance: {maintenance:?}")
            }
            if apply_res.applied() {
                info!("attempt {} req {}: {:?}", attempt, idx, apply_res);
            } else {
                info!(
                    "attempt {} req {}: {:?} trying again",
                    attempt, idx, apply_res
                );
                attempt += 1;
                continue 'outer;
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
    consensus: Arc<dyn Consensus>,
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    commit: bool,
    expected_version: Option<Version>,
) -> anyhow::Result<Machine<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>> {
    make_typed_machine::<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64>(
        cfg,
        consensus,
        blob,
        metrics,
        shard_id,
        commit,
        expected_version,
    )
    .await
}

async fn make_typed_machine<K, V, T, D>(
    cfg: &PersistConfig,
    consensus: Arc<dyn Consensus>,
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    commit: bool,
    expected_version: Option<Version>,
) -> anyhow::Result<Machine<K, V, T, D>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
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
        let safe_version_change = match (commit, expected_version) {
            // We never actually write out state changes, so increasing the version is okay.
            (false, _) => cfg.build_version >= state.applier_version,
            // If the versions match that's okay because any commits won't change it.
            (true, None) => cfg.build_version == state.applier_version,
            // !!DANGER ZONE!!
            (true, Some(expected)) => {
                // If we're not _extremely_ careful, the persistcli could make shards unreadable by
                // production. But there are times when we want to operate on a leaked shard with a
                // newer version of the build.
                //
                // We only allow a mismatch in version if we provided the expected version to the
                // command, and the expected version is less than the current build, which
                // indicates this is an old shard.
                state.applier_version == expected && expected <= cfg.build_version
            }
        };
        if !safe_version_change {
            // We could add a flag to override this check, if that comes up.
            return Err(anyhow!(
                "version of this tool {} does not match version of state {} when --commit is {commit}. bailing so we don't corrupt anything",
                cfg.build_version,
                state.applier_version
            ));
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
        Arc::new(IsolatedRuntime::new(
            &MetricsRegistry::new(),
            Some(cfg.isolated_runtime_worker_threads),
        )),
        Diagnostics::from_purpose("admin"),
    )
    .await?;

    Ok(machine)
}

async fn force_gc(
    cfg: PersistConfig,
    metrics_registry: &MetricsRegistry,
    shard_id: ShardId,
    consensus_uri: &SensitiveUrl,
    blob_uri: &SensitiveUrl,
    commit: bool,
    expected_version: Option<Version>,
) -> anyhow::Result<Box<dyn Any>> {
    let metrics = Arc::new(Metrics::new(&cfg, metrics_registry));
    let consensus = make_consensus(&cfg, consensus_uri, commit, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, blob_uri, commit, Arc::clone(&metrics)).await?;
    let machine = make_machine(
        &cfg,
        consensus,
        blob,
        metrics,
        shard_id,
        commit,
        expected_version,
    )
    .await?;
    let gc_req = GcReq {
        shard_id,
        new_seqno_since: machine.applier.seqno_since(),
    };
    let (maintenance, _stats) = GarbageCollector::gc_and_truncate(&machine, gc_req).await;
    if !maintenance.is_empty() {
        info!("ignoring non-empty requested maintenance: {maintenance:?}")
    }

    Ok(Box::new(machine))
}

/// Exposed for `mz-catalog`.
pub const CATALOG_FORCE_COMPACTION_FUEL: Config<usize> = Config::new(
    "persist_catalog_force_compaction_fuel",
    1024,
    "fuel to use in catalog dangerous_force_compaction task",
);

/// Exposed for `mz-catalog`.
pub const CATALOG_FORCE_COMPACTION_WAIT: Config<Duration> = Config::new(
    "persist_catalog_force_compaction_wait",
    Duration::from_secs(60),
    "wait to use in catalog dangerous_force_compaction task",
);

/// Exposed for `mz-catalog`.
pub const EXPRESSION_CACHE_FORCE_COMPACTION_FUEL: Config<usize> = Config::new(
    "persist_expression_cache_force_compaction_fuel",
    131_072,
    "fuel to use in expression cache dangerous_force_compaction",
);

/// Exposed for `mz-catalog`.
pub const EXPRESSION_CACHE_FORCE_COMPACTION_WAIT: Config<Duration> = Config::new(
    "persist_expression_cache_force_compaction_wait",
    Duration::from_secs(0),
    "wait to use in expression cache dangerous_force_compaction",
);

/// Attempts to compact all batches in a shard into a minimal number.
///
/// This destroys most hope of filter pushdown doing anything useful. If you
/// think you want this, you almost certainly want something else, please come
/// talk to the persist team before using.
///
/// This is accomplished by adding artificial fuel (representing some count of
/// updates) to the shard's internal Spine of batches structure on some schedule
/// and doing any compaction work that results. Both the amount of fuel and
/// cadence are dynamically tunable. However, it is not clear exactly how much
/// fuel is safe to add in each call: 10 definitely seems fine, usize::MAX may
/// not be. This also adds to the danger in "dangerous".
///
/// This is intentionally not even hooked up to `persistcli admin` for the above
/// reasons.
///
/// Exits once the shard is sufficiently compacted, but can be called in a loop
/// to keep it compacted.
pub async fn dangerous_force_compaction_and_break_pushdown<K, V, T, D>(
    write: &WriteHandle<K, V, T, D>,
    fuel: impl Fn() -> usize,
    wait: impl Fn() -> Duration,
) where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    let machine = write.machine.clone();

    let mut last_exert: Instant;

    loop {
        last_exert = Instant::now();
        let fuel = fuel();
        let (reqs, mut maintenance) = machine.spine_exert(fuel).await;
        for req in reqs {
            info!(
                "force_compaction {} {} compacting {} batches in {} parts with {} runs totaling {} bytes: lower={:?} upper={:?} since={:?}",
                machine.applier.shard_metrics.name,
                machine.applier.shard_metrics.shard_id,
                req.inputs.len(),
                req.inputs.iter().flat_map(|x| &x.batch.parts).count(),
                req.inputs
                    .iter()
                    .map(|x| x.batch.runs().count())
                    .sum::<usize>(),
                req.inputs
                    .iter()
                    .flat_map(|x| &x.batch.parts)
                    .map(|x| x.encoded_size_bytes())
                    .sum::<usize>(),
                req.desc.lower().elements(),
                req.desc.upper().elements(),
                req.desc.since().elements(),
            );
            machine.applier.metrics.compaction.requested.inc();
            let start = Instant::now();
            let res = Compactor::<K, V, T, D>::compact_and_apply(
                &machine,
                req,
                write.write_schemas.clone(),
            )
            .await;
            let apply_maintenance = match res {
                Ok(x) => x,
                Err(err) => {
                    warn!(
                        "force_compaction {} {} errored in compaction: {:?}",
                        machine.applier.shard_metrics.name,
                        machine.applier.shard_metrics.shard_id,
                        err
                    );
                    continue;
                }
            };
            machine.applier.metrics.compaction.admin_count.inc();
            info!(
                "force_compaction {} {} compacted in {:?}",
                machine.applier.shard_metrics.name,
                machine.applier.shard_metrics.shard_id,
                start.elapsed(),
            );
            maintenance.merge(apply_maintenance);
        }
        maintenance.perform(&machine, &write.gc).await;

        // Now sleep before the next one. Make sure to delay from when we
        // started the last exert in case the compaction takes non-trivial time.
        let next_exert = last_exert + wait();
        tokio::time::sleep_until(next_exert.into()).await;

        // NB: This check is intentionally at the end so that it's safe to call
        // this method in a loop.
        let num_runs: usize = machine
            .applier
            .all_batches()
            .iter()
            .map(|x| x.runs().count())
            .sum();
        if num_runs <= 1 {
            info!(
                "force_compaction {} {} exiting with {} runs",
                machine.applier.shard_metrics.name,
                machine.applier.shard_metrics.shard_id,
                num_runs
            );
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_dyncfg::ConfigUpdates;
    use mz_persist_types::ShardId;

    use crate::tests::new_test_client;

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn dangerous_force_compaction_and_break_pushdown(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        for num_batches in 0..=17 {
            let (mut write, _read) = client
                .expect_open::<String, (), u64, i64>(ShardId::new())
                .await;
            let machine = write.machine.clone();

            for idx in 0..num_batches {
                let () = write
                    .expect_compare_and_append(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
                    .await;
            }

            // Run the tool and verify that we get down to at most two.
            super::dangerous_force_compaction_and_break_pushdown(&write, || 1, || Duration::ZERO)
                .await;
            let batches_after = machine.applier.all_batches().len();
            assert!(batches_after < 2, "{} vs {}", num_batches, batches_after);
        }
    }
}
