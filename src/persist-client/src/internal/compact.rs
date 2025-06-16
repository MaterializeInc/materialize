// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::{Stream, pin_mut};
use futures_util::StreamExt;
use itertools::Itertools;
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::location::Blob;
use mz_persist_types::arrow::ArrayBound;
use mz_persist_types::part::Part;
use mz_persist_types::{Codec, Codec64};
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::Sender;
use tokio::sync::{TryAcquireError, mpsc, oneshot};
use tracing::{Instrument, Span, debug, debug_span, error, info, trace, warn};

use crate::async_runtime::IsolatedRuntime;
use crate::batch::{BatchBuilderConfig, BatchBuilderInternal, BatchParts, PartDeletes};
use crate::cfg::{
    COMPACTION_HEURISTIC_MIN_INPUTS, COMPACTION_HEURISTIC_MIN_PARTS,
    COMPACTION_HEURISTIC_MIN_UPDATES, COMPACTION_MEMORY_BOUND_BYTES,
    GC_BLOB_DELETE_CONCURRENCY_LIMIT, INCREMENTAL_COMPACTIONS_SINGLE_RUN_ENABLED, MiB,
};
use crate::fetch::{EncodedPart, FetchBatchFilter};
use crate::internal::encoding::Schemas;
use crate::internal::gc::GarbageCollector;
use crate::internal::machine::Machine;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::ShardMetrics;
use crate::internal::state::{HollowBatch, RunMeta, RunOrder, RunPart};
use crate::internal::trace::{ApplyMergeResult, FueledMergeRes};
use crate::iter::{Consolidator, LowerBound, StructuredSort};
use crate::{Metrics, PersistConfig, ShardId};

use super::trace::{ActiveCompaction, IdHollowBatch, RunId};

/// A request for compaction.
///
/// This is similar to FueledMergeReq, but intentionally a different type. If we
/// move compaction to an rpc server, this one will become a protobuf; the type
/// parameters will become names of codecs to look up in some registry.
#[derive(Debug, Clone)]
pub struct CompactReq<T> {
    /// The shard the input and output batches belong to.
    pub shard_id: ShardId,
    /// A description for the output batch.
    pub desc: Description<T>,
    /// The updates to include in the output batch. Any data in these outside of
    /// the output descriptions bounds should be ignored.
    /// If this compaction is a resume of a previously interrupted compaction
    /// then prev_batch contains the work done so far.
    pub prev_batch: Option<HollowBatch<T>>,
    pub inputs: Vec<IdHollowBatch<T>>,
}

/// A response from compaction.
#[derive(Debug)]
pub struct CompactRes<T> {
    /// The compacted batch.
    pub output: HollowBatch<T>,
    /// The runs that were compacted together to produce the output batch.
    pub inputs: Vec<RunId>,
}

/// A snapshot of dynamic configs to make it easier to reason about an
/// individual run of compaction.
#[derive(Debug, Clone)]
pub struct CompactConfig {
    pub(crate) compaction_memory_bound_bytes: usize,
    pub(crate) compaction_yield_after_n_updates: usize,
    pub(crate) version: semver::Version,
    pub(crate) batch: BatchBuilderConfig,
}

impl CompactConfig {
    /// Initialize the compaction config from Persist configuration.
    pub fn new(value: &PersistConfig, shard_id: ShardId) -> Self {
        CompactConfig {
            compaction_memory_bound_bytes: COMPACTION_MEMORY_BOUND_BYTES.get(value),
            compaction_yield_after_n_updates: value.compaction_yield_after_n_updates,
            version: value.build_version.clone(),
            batch: BatchBuilderConfig::new(value, shard_id),
        }
    }
}

/// A service for performing physical and logical compaction.
///
/// This will possibly be called over RPC in the future. Physical compaction is
/// merging adjacent batches. Logical compaction is advancing timestamps to a
/// new since and consolidating the resulting updates.
#[derive(Debug)]
pub struct Compactor<K, V, T, D> {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    sender: Sender<(
        Instant,
        CompactReq<T>,
        Machine<K, V, T, D>,
        oneshot::Sender<Result<(), anyhow::Error>>,
    )>,
    _phantom: PhantomData<fn() -> D>,
}

impl<K, V, T, D> Clone for Compactor<K, V, T, D> {
    fn clone(&self) -> Self {
        Compactor {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            sender: self.sender.clone(),
            _phantom: Default::default(),
        }
    }
}

/// In Compactor::compact_and_apply_background, the minimum amount of time to
/// allow a compaction request to run before timing it out. A request may be
/// given a timeout greater than this value depending on the inputs' size
pub(crate) const COMPACTION_MINIMUM_TIMEOUT: Config<Duration> = Config::new(
    "persist_compaction_minimum_timeout",
    Duration::from_secs(90),
    "\
    The minimum amount of time to allow a persist compaction request to run \
    before timing it out (Materialize).",
);

pub(crate) const COMPACTION_USE_MOST_RECENT_SCHEMA: Config<bool> = Config::new(
    "persist_compaction_use_most_recent_schema",
    true,
    "\
    Use the most recent schema from all the Runs that are currently being \
    compacted, instead of the schema on the current write handle (Materialize).
    ",
);

pub(crate) const COMPACTION_CHECK_PROCESS_FLAG: Config<bool> = Config::new(
    "persist_compaction_check_process_flag",
    true,
    "Whether Compactor will obey the process_requests flag in PersistConfig, \
        which allows dynamically disabling compaction. If false, all compaction requests will be processed.",
);

impl<K, V, T, D> Compactor<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    pub fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        write_schemas: Schemas<K, V>,
        gc: GarbageCollector<K, V, T, D>,
    ) -> Self {
        let (compact_req_sender, mut compact_req_receiver) = mpsc::channel::<(
            Instant,
            CompactReq<T>,
            Machine<K, V, T, D>,
            oneshot::Sender<Result<(), anyhow::Error>>,
        )>(cfg.compaction_queue_size);
        let concurrency_limit = Arc::new(tokio::sync::Semaphore::new(
            cfg.compaction_concurrency_limit,
        ));
        let check_process_requests = COMPACTION_CHECK_PROCESS_FLAG.handle(&cfg.configs);
        let process_requests = Arc::clone(&cfg.compaction_process_requests);

        // spin off a single task responsible for executing compaction requests.
        // work is enqueued into the task through a channel
        let _worker_handle = mz_ore::task::spawn(|| "PersistCompactionScheduler", async move {
            while let Some((enqueued, req, machine, completer)) = compact_req_receiver.recv().await
            {
                assert_eq!(req.shard_id, machine.shard_id());
                let metrics = Arc::clone(&machine.applier.metrics);

                // Only allow skipping compaction requests if the dyncfg is enabled.
                if check_process_requests.get()
                    && !process_requests.load(std::sync::atomic::Ordering::Relaxed)
                {
                    // Respond to the requester, track in our metrics, and log
                    // that compaction is disabled.
                    let _ = completer.send(Err(anyhow::anyhow!("compaction disabled")));
                    metrics.compaction.disabled.inc();
                    tracing::warn!(shard_id = ?req.shard_id, "Dropping compaction request on the floor.");

                    continue;
                }

                let permit = {
                    let inner = Arc::clone(&concurrency_limit);
                    // perform a non-blocking attempt to acquire a permit so we can
                    // record how often we're ever blocked on the concurrency limit
                    match inner.try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(TryAcquireError::NoPermits) => {
                            metrics.compaction.concurrency_waits.inc();
                            Arc::clone(&concurrency_limit)
                                .acquire_owned()
                                .await
                                .expect("semaphore is never closed")
                        }
                        Err(TryAcquireError::Closed) => {
                            // should never happen in practice. the semaphore is
                            // never explicitly closed, nor will it close on Drop
                            warn!("semaphore for shard {} is closed", machine.shard_id());
                            continue;
                        }
                    }
                };
                metrics
                    .compaction
                    .queued_seconds
                    .inc_by(enqueued.elapsed().as_secs_f64());

                let write_schemas = write_schemas.clone();

                let compact_span =
                    debug_span!(parent: None, "compact::apply", shard_id=%machine.shard_id());
                compact_span.follows_from(&Span::current());
                let gc = gc.clone();
                mz_ore::task::spawn(|| "PersistCompactionWorker", async move {
                    let res = Self::compact_and_apply(&machine, req, write_schemas)
                        .instrument(compact_span)
                        .await;
                    let res = res.map(|maintenance| {
                        maintenance.start_performing(&machine, &gc);
                    });

                    // we can safely ignore errors here, it's possible the caller
                    // wasn't interested in waiting and dropped their receiver
                    let _ = completer.send(res);

                    // moves `permit` into async scope so it can be dropped upon completion
                    drop(permit);
                });
            }
        });

        Compactor {
            cfg,
            metrics,
            sender: compact_req_sender,
            _phantom: PhantomData,
        }
    }

    /// Enqueues a [CompactReq] to be consumed by the compaction background task when available.
    ///
    /// Returns a receiver that indicates when compaction has completed. The receiver can be
    /// safely dropped at any time if the caller does not wish to wait on completion.
    pub fn compact_and_apply_background(
        &self,
        req: CompactReq<T>,
        machine: &Machine<K, V, T, D>,
    ) -> Option<oneshot::Receiver<Result<(), anyhow::Error>>> {
        // Run some initial heuristics to ignore some requests for compaction.
        // We don't gain much from e.g. compacting two very small batches that
        // were just written, but it does result in non-trivial blob traffic
        // (especially in aggregate). This heuristic is something we'll need to
        // tune over time.
        let should_compact = req.inputs.len() >= COMPACTION_HEURISTIC_MIN_INPUTS.get(&self.cfg)
            || req
                .inputs
                .iter()
                .map(|x| x.batch.part_count())
                .sum::<usize>()
                >= COMPACTION_HEURISTIC_MIN_PARTS.get(&self.cfg)
            || req.inputs.iter().map(|x| x.batch.len).sum::<usize>()
                >= COMPACTION_HEURISTIC_MIN_UPDATES.get(&self.cfg);
        if !should_compact {
            self.metrics.compaction.skipped.inc();
            return None;
        }

        let (compaction_completed_sender, compaction_completed_receiver) = oneshot::channel();
        let new_compaction_sender = self.sender.clone();

        self.metrics.compaction.requested.inc();
        // NB: we intentionally pass along the input machine, as it ought to come from the
        // writer that generated the compaction request / maintenance. this machine has a
        // spine structure that generated the request, so it has a much better chance of
        // merging and committing the result than a machine kept up-to-date through state
        // diffs, which may have a different spine structure less amenable to merging.
        let send = new_compaction_sender.try_send((
            Instant::now(),
            req,
            machine.clone(),
            compaction_completed_sender,
        ));
        if let Err(_) = send {
            self.metrics.compaction.dropped.inc();
            return None;
        }

        Some(compaction_completed_receiver)
    }

    pub(crate) async fn compact_and_apply(
        machine: &Machine<K, V, T, D>,
        req: CompactReq<T>,
        write_schemas: Schemas<K, V>,
    ) -> Result<RoutineMaintenance, anyhow::Error> {
        let metrics = Arc::clone(&machine.applier.metrics);
        metrics.compaction.started.inc();
        let start = Instant::now();

        // pick a timeout for our compaction request proportional to the amount
        // of data that must be read (with a minimum set by PersistConfig)
        let total_input_bytes = req
            .inputs
            .iter()
            .map(|x| x.batch.encoded_size_bytes())
            .sum::<usize>();
        let timeout = Duration::max(
            // either our minimum timeout
            COMPACTION_MINIMUM_TIMEOUT.get(&machine.applier.cfg),
            // or 1s per MB of input data
            Duration::from_secs(u64::cast_from(total_input_bytes / MiB)),
        );
        // always use most recent schema from all the Runs we're compacting to prevent Compactors
        // created before the schema was evolved, from trying to "de-evolve" a Part.
        let compaction_schema_id = req
            .inputs
            .iter()
            .flat_map(|x| x.batch.run_meta.iter())
            .filter_map(|run_meta| run_meta.schema)
            // It's an invariant that SchemaIds are ordered.
            .max();
        let maybe_compaction_schema = match compaction_schema_id {
            Some(id) => machine
                .get_schema(id)
                .map(|(key_schema, val_schema)| (id, key_schema, val_schema)),
            None => None,
        };
        let use_most_recent_schema = COMPACTION_USE_MOST_RECENT_SCHEMA.get(&machine.applier.cfg);

        let compaction_schema = match maybe_compaction_schema {
            Some((id, key_schema, val_schema)) if use_most_recent_schema => {
                metrics.compaction.schema_selection.recent_schema.inc();
                Schemas {
                    id: Some(id),
                    key: Arc::new(key_schema),
                    val: Arc::new(val_schema),
                }
            }
            Some(_) => {
                metrics.compaction.schema_selection.disabled.inc();
                write_schemas
            }
            None => {
                metrics.compaction.schema_selection.no_schema.inc();
                write_schemas
            }
        };

        trace!(
            "compaction request for {}MBs ({} bytes), with timeout of {}s, and schema {:?}.",
            total_input_bytes / MiB,
            total_input_bytes,
            timeout.as_secs_f64(),
            compaction_schema.id,
        );

        let isolated_runtime = Arc::clone(&machine.isolated_runtime);
        let machine_clone = machine.clone();
        let metrics_clone = Arc::clone(&machine.applier.metrics);

        let compact_span = debug_span!("compact::consolidate");
        let res = tokio::time::timeout(timeout, async {
            isolated_runtime
                .spawn_named(
                    || "persist::compact::consolidate",
                    async move {
                        let stream = Self::compact_stream(
                            CompactConfig::new(
                                &machine_clone.applier.cfg,
                                machine_clone.shard_id(),
                            ),
                            Arc::clone(&machine_clone.applier.state_versions.blob),
                            Arc::clone(&metrics_clone),
                            Arc::clone(&machine_clone.applier.shard_metrics),
                            Arc::clone(&machine_clone.isolated_runtime),
                            req.clone(),
                            compaction_schema,
                            &machine_clone,
                        );

                        let maintenance = if INCREMENTAL_COMPACTIONS_SINGLE_RUN_ENABLED
                            .get(&machine_clone.applier.cfg)
                        {
                            let mut maintenance = RoutineMaintenance::default();
                            pin_mut!(stream);
                            while let Some(res) = stream.next().await {
                                let res = res?;
                                let new_maintenance =
                                    Self::apply(res, &metrics_clone, &machine_clone).await?;
                                maintenance = std::cmp::max(maintenance, new_maintenance);
                            }
                            maintenance
                        } else {
                            let res = Self::compact_all(stream, req.clone()).await?;
                            Self::apply(
                                FueledMergeRes {
                                    output: res.output,
                                    inputs: res.inputs,
                                    new_active_compaction: None,
                                },
                                &metrics_clone,
                                &machine_clone,
                            )
                            .await?
                        };

                        Ok::<_, anyhow::Error>(maintenance)
                    }
                    .instrument(compact_span),
                )
                .await
                .map_err(|e| anyhow!("compaction task join failed: {e}"))?
        })
        .await
        .map_err(|e| {
            metrics.compaction.timed_out.inc();
            anyhow!("compaction timed out after {:?}: {e}", timeout)
        })?;

        metrics
            .compaction
            .seconds
            .inc_by(start.elapsed().as_secs_f64());

        match res {
            Ok(maintenance) => Ok(maintenance),
            Err(err) => {
                metrics.compaction.failed.inc();
                debug!(
                    "compaction for {} failed: {}",
                    machine.shard_id(),
                    err.display_with_causes()
                );
                Err(err)
            }
        }
    }

    pub async fn apply(
        res: FueledMergeRes<T>,
        metrics: &Metrics,
        machine: &Machine<K, V, T, D>,
    ) -> Result<RoutineMaintenance, anyhow::Error> {
        let (apply_merge_result, maintenance) = machine.merge_res(&res).await;

        match &apply_merge_result {
            ApplyMergeResult::AppliedExact => {
                metrics.compaction.applied.inc();
                metrics.compaction.applied_exact_match.inc();
                machine.applier.shard_metrics.compaction_applied.inc();
            }
            ApplyMergeResult::AppliedSubset => {
                metrics.compaction.applied.inc();
                metrics.compaction.applied_subset_match.inc();
                machine.applier.shard_metrics.compaction_applied.inc();
            }
            ApplyMergeResult::NotAppliedNoMatch
            | ApplyMergeResult::NotAppliedInvalidSince
            | ApplyMergeResult::NotAppliedTooManyUpdates => {
                if let ApplyMergeResult::NotAppliedTooManyUpdates = &apply_merge_result {
                    metrics.compaction.not_applied_too_many_updates.inc();
                }
                metrics.compaction.noop.inc();
                let mut part_deletes = PartDeletes::default();
                for part in &res.output.parts {
                    part_deletes.add(&part);
                }
                part_deletes
                    .delete(
                        machine.applier.state_versions.blob.as_ref(),
                        machine.shard_id(),
                        GC_BLOB_DELETE_CONCURRENCY_LIMIT.get(&machine.applier.cfg),
                        &*metrics,
                        &metrics.retries.external.compaction_noop_delete,
                    )
                    .await;
            }
        };

        Ok(maintenance)
    }

    pub async fn compact_all(
        stream: impl Stream<Item = Result<FueledMergeRes<T>, anyhow::Error>>,
        req: CompactReq<T>,
    ) -> Result<CompactRes<T>, anyhow::Error> {
        pin_mut!(stream);

        let mut all_parts = vec![];
        let mut all_run_splits = vec![];
        let mut all_run_meta = vec![];
        let mut len = 0;

        while let Some(res) = stream.next().await {
            let res = res?;
            let (parts, updates, run_meta, run_splits) = (
                res.output.parts,
                res.output.len,
                res.output.run_meta,
                res.output.run_splits,
            );
            let run_offset = all_parts.len();
            if !all_parts.is_empty() {
                all_run_splits.push(run_offset);
            }
            all_run_splits.extend(run_splits.iter().map(|r| r + run_offset));
            all_run_meta.extend(run_meta);
            all_parts.extend(parts);
            len += updates;
        }

        let run_inputs = req
            .inputs
            .iter()
            .map(|x| {
                x.batch
                    .runs()
                    .enumerate()
                    .map(|(i, _)| RunId(x.id, i))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();

        Ok(CompactRes {
            output: HollowBatch::new(
                req.desc.clone(),
                all_parts,
                len,
                all_run_meta,
                all_run_splits,
            ),
            inputs: run_inputs,
        })
    }

    /// Compacts input batches in bounded memory.
    ///
    /// The memory bound is broken into pieces:
    ///     1. in-progress work
    ///     2. fetching parts from runs
    ///     3. additional in-flight requests to Blob
    ///
    /// 1. In-progress work is bounded by 2 * [BatchBuilderConfig::blob_target_size]. This
    ///    usage is met at two mutually exclusive moments:
    ///   * When reading in a part, we hold the columnar format in memory while writing its
    ///     contents into a heap.
    ///   * When writing a part, we hold a temporary updates buffer while encoding/writing
    ///     it into a columnar format for Blob.
    ///
    /// 2. When compacting runs, only 1 part from each one is held in memory at a time.
    ///    Compaction will determine an appropriate number of runs to compact together
    ///    given the memory bound and accounting for the reservation in (1). A minimum
    ///    of 2 * [BatchBuilderConfig::blob_target_size] of memory is expected, to be
    ///    able to at least have the capacity to compact two runs together at a time,
    ///    and more runs will be compacted together if more memory is available.
    ///
    /// 3. If there is excess memory after accounting for (1) and (2), we increase the
    ///    number of outstanding parts we can keep in-flight to Blob.
    pub fn compact_stream(
        cfg: CompactConfig,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        isolated_runtime: Arc<IsolatedRuntime>,
        req: CompactReq<T>,
        write_schemas: Schemas<K, V>,
        machine: &Machine<K, V, T, D>,
    ) -> impl Stream<Item = Result<FueledMergeRes<T>, anyhow::Error>> {
        async_stream::stream! {
            let _ = Self::validate_req(&req)?;

            // We introduced a fast-path optimization in https://github.com/MaterializeInc/materialize/pull/15363
            // but had to revert it due to a very scary bug. Here we count how many of our compaction reqs
            // could be eligible for the optimization to better understand whether it's worth trying to
            // reintroduce it.
            let mut single_nonempty_batch = None;
            for batch in req.inputs.iter().map(|x| &x.batch) {
                if batch.len > 0 {
                    match single_nonempty_batch {
                        None => single_nonempty_batch = Some(batch),
                        Some(_previous_nonempty_batch) => {
                            single_nonempty_batch = None;
                            break;
                        }
                    }
                }
            }
            if let Some(single_nonempty_batch) = single_nonempty_batch {
                if single_nonempty_batch.run_splits.len() == 0
                    && single_nonempty_batch.desc.since() != &Antichain::from_elem(T::minimum())
                {
                    metrics.compaction.fast_path_eligible.inc();
                }
            }

            assert!(cfg.compaction_memory_bound_bytes >= 4 * cfg.batch.blob_target_size);

            // Prepare memory bounds for compaction
            let in_progress_part_reserved_memory_bytes = 2 * cfg.batch.blob_target_size;
            let run_reserved_memory_bytes =
                cfg.compaction_memory_bound_bytes - in_progress_part_reserved_memory_bytes;

            info!("input runs: {:?}", req.inputs.iter().map(|x| x.id).collect::<Vec<_>>());
            // Flatten the input batches into a single list of runs
            let ordered_runs =
                Self::order_runs(&req, cfg.batch.preferred_order, &*blob, &*metrics).await?;

            info!("ordered runs: {:?}", ordered_runs.iter().map(|(run_id, _, _, _)| run_id).collect::<Vec<_>>());

            // There are two cases to consider here:
            // 1. There are as many runs as there are input batches in which case we
            //    should compact them together as space allows.
            // 2. There are more runs than input batches, in which case we should compact them in chunks
            //    grouped by the batch they belong to.
            // In both cases, we should compact runs in the order they were written.
            // This is all to make it easy to apply the compaction result incrementally.
            // By ensuring we never write out batches that contain runs from seperate input batches
            // (except for when each input batch has exactly one run), we can easily slot the
            // results in to the existing batches. The special case of a single run per input batch
            // means that each batch is, by itself, fully "compact", and the result of compaction
            // will cleanly replace the input batches in a grouped manner.


            // Split the runs into manageable chunks
            let chunked_runs =
                Self::chunk_runs(&ordered_runs, &cfg, &*metrics, run_reserved_memory_bytes);

            let (incremental_tx, mut incremental_rx) = mpsc::channel(1);

            let machine = machine.clone();
            let incremental_handle = tokio::spawn(
                async move {
                    while let Some(res) = incremental_rx.recv().await {
                        let now = SYSTEM_TIME.clone();

                        machine.checkpoint_compaction_progress(&res, now()).await;
                    }
                }
                .instrument(debug_span!("compact::incremental")),
            );

            for (runs, run_chunk_max_memory_usage) in chunked_runs {
                info!("compacting runs for chunk: {:?}", runs.iter().map(|(run_id, _, _, _)| run_id).collect::<Vec<_>>());
                metrics.compaction.chunks_compacted.inc();
                metrics
                    .compaction
                    .runs_compacted
                    .inc_by(u64::cast_from(runs.len()));

                // Adjust parallelism based on how much memory we have left
                let extra_outstanding_parts = (run_reserved_memory_bytes
                    .saturating_sub(run_chunk_max_memory_usage))
                    / cfg.batch.blob_target_size;

                let mut run_cfg = cfg.clone();
                run_cfg.batch.batch_builder_max_outstanding_parts = 1 + extra_outstanding_parts;

                let input_runs = &runs.iter().map(|(run_id, _, _, _)| *run_id.clone()).collect::<Vec<_>>();
                let runs = runs.iter()
                    .map(|(_, desc, meta, run)| (desc.clone(), meta.clone(), run.clone()))
                    .collect::<Vec<_>>();

                let batch = Self::compact_runs(
                    &run_cfg,
                    &req.shard_id,
                    &req.desc,
                    runs,
                    Arc::clone(&blob),
                    Arc::clone(&metrics),
                    Arc::clone(&shard_metrics),
                    Arc::clone(&isolated_runtime),
                    write_schemas.clone(),
                    &req.prev_batch,
                    Some(incremental_tx.clone())
                ).await?;

                let (parts, run_splits, run_meta, updates) =
                    (batch.parts, batch.run_splits, batch.run_meta, batch.len);

                assert!(
                    (updates == 0 && parts.is_empty()) || (updates > 0 && !parts.is_empty()),
                    "updates={}, parts={}",
                    updates,
                    parts.len(),
                );

                if updates == 0 {
                    continue;
                }

                let res = CompactRes {
                    output: HollowBatch::new(
                        batch.desc,
                        parts.clone(),
                        updates,
                        run_meta.clone(),
                        run_splits.clone(),
                    ),
                    inputs: input_runs.clone(),
                };

                let res = FueledMergeRes {
                    output: res.output,
                    new_active_compaction: None,
                    inputs: res.inputs,
                };

                yield Ok(res);
            }
            drop(incremental_tx);
            // Wait for the incremental handle to finish processing any remaining batches.
            // TODO: perhaps we should have a way to cancel this handle once we reach this point?
            let _ = incremental_handle.await;
        }
    }

    /// Sorts and groups all runs from the inputs into chunks, each of which has been determined
    /// to consume no more than `run_reserved_memory_bytes` at a time, unless the input parts
    /// were written with a different target size than this build. Uses [Self::order_runs] to
    /// determine the order in which runs are selected.
    fn chunk_runs<'a>(
        ordered_runs: &'a [(
            RunId,
            &'a Description<T>,
            &'a RunMeta,
            Cow<'a, [RunPart<T>]>,
        )],
        cfg: &CompactConfig,
        metrics: &Metrics,
        run_reserved_memory_bytes: usize,
    ) -> Vec<(
        Vec<(&'a RunId, &'a Description<T>, &'a RunMeta, &'a [RunPart<T>])>,
        usize,
    )> {
        // Group the runs by the SpineId they belong to.
        let ordered_chunks = ordered_runs
            .into_iter()
            .chunk_by(|(run_id, _, _, _)| run_id.0)
            .into_iter()
            .map(|(id, chunk)| (id, chunk.collect()))
            .collect::<Vec<(_, Vec<_>)>>();

        let groups = if ordered_chunks.iter().all(|(_, chunk)| chunk.len() == 1) {
            // If each chunk has only one run, we can just flatten it into a single list.
            vec![
                ordered_chunks
                    .into_iter()
                    .flat_map(|(_, chunk)| chunk)
                    .collect::<Vec<_>>(),
            ]
        } else {
            ordered_chunks
                .into_iter()
                .map(|(_id, chunk)| chunk)
                .collect::<Vec<_>>()
        };

        let mut chunks = vec![];
        for ordered_runs in groups {
            let mut current_chunk = vec![];
            let mut current_chunk_max_memory_usage = 0;
            let mut ordered_runs = ordered_runs.into_iter().peekable();
            while let Some((run_id, desc, meta, run)) = ordered_runs.next() {
                let run_greatest_part_size = run
                    .iter()
                    .map(|x| x.max_part_bytes())
                    .max()
                    .unwrap_or(cfg.batch.blob_target_size);
                current_chunk.push((run_id, *desc, *meta, &**run));
                current_chunk_max_memory_usage += run_greatest_part_size;

                if let Some((_next_run_id, _next_desc, _next_meta, next_run)) = ordered_runs.peek()
                {
                    let next_run_greatest_part_size = next_run
                        .iter()
                        .map(|x| x.max_part_bytes())
                        .max()
                        .unwrap_or(cfg.batch.blob_target_size);

                    // if we can fit the next run in our chunk without going over our reserved memory, we should do so
                    if current_chunk_max_memory_usage + next_run_greatest_part_size
                        <= run_reserved_memory_bytes
                    {
                        continue;
                    }

                    // NB: There's an edge case where we cannot fit at least 2 runs into a chunk
                    // with our reserved memory. This could happen if blobs were written with a
                    // larger target size than the current build. When this happens, we violate
                    // our memory requirement and force chunks to be at least length 2, so that we
                    // can be assured runs are merged and converge over time.
                    if current_chunk.len() == 1 {
                        // in the steady state we expect this counter to be 0, and would only
                        // anticipate it being temporarily nonzero if we changed target blob size
                        // or our memory requirement calculations
                        metrics.compaction.memory_violations.inc();
                        continue;
                    }
                }

                chunks.push((
                    std::mem::take(&mut current_chunk),
                    current_chunk_max_memory_usage,
                ));
                current_chunk_max_memory_usage = 0;
            }
        }

        chunks
    }

    async fn order_runs<'a>(
        req: &'a CompactReq<T>,
        target_order: RunOrder,
        blob: &'a dyn Blob,
        metrics: &'a Metrics,
    ) -> anyhow::Result<
        Vec<(
            RunId,
            &'a Description<T>,
            &'a RunMeta,
            Cow<'a, [RunPart<T>]>,
        )>,
    > {
        let total_number_of_runs = req
            .inputs
            .iter()
            .map(|x| x.batch.run_splits.len() + 1)
            .sum::<usize>();

        let mut batch_runs: VecDeque<_> = req
            .inputs
            .iter()
            .map(|x| (x.id, &x.batch.desc, x.batch.runs()))
            .collect();

        let mut ordered_runs = Vec::with_capacity(total_number_of_runs);

        while let Some((spine_id, desc, runs)) = batch_runs.pop_front() {
            for (i, (meta, run)) in runs.enumerate() {
                let run_id = RunId(spine_id, i);
                let same_order = meta.order.unwrap_or(RunOrder::Codec) == target_order;
                if same_order {
                    ordered_runs.push((run_id, desc, meta, Cow::Borrowed(run)));
                } else {
                    // The downstream consolidation step will handle a long run that's not in
                    // the desired order by splitting it up into many single-element runs. This preserves
                    // correctness, but it means that we may end up needing to iterate through
                    // many more parts concurrently than expected, increasing memory use. Instead,
                    // we break up those runs into individual batch parts, fetching hollow runs as
                    // necessary, before they're grouped together to be passed to consolidation.
                    // The downside is that this breaks the usual property that compaction produces
                    // fewer runs than it takes in. This should generally be resolved by future
                    // runs of compaction.
                    for part in run {
                        let mut batch_parts = pin!(part.part_stream(req.shard_id, blob, metrics));
                        while let Some(part) = batch_parts.next().await {
                            ordered_runs.push((
                                run_id,
                                desc,
                                meta,
                                Cow::Owned(vec![RunPart::Single(part?.into_owned())]),
                            ));
                        }
                    }
                }
            }
        }

        Ok(ordered_runs)
    }

    fn combine_hollow_batch_with_previous(
        previous_batch: &HollowBatch<T>,
        batch: &HollowBatch<T>,
    ) -> HollowBatch<T> {
        // Simplifying assumption: you can't combine batches with different descriptions.
        assert_eq!(previous_batch.desc, batch.desc);
        info!(
            "Combining batches: previous_batch.len={} batch.len={}",
            previous_batch.len, batch.len
        );
        let len = previous_batch.len + batch.len;
        let mut parts = Vec::with_capacity(previous_batch.parts.len() + batch.parts.len());
        parts.extend(previous_batch.parts.clone());
        parts.extend(batch.parts.clone());
        assert!(previous_batch.run_splits.is_empty());
        assert!(batch.run_splits.is_empty());
        HollowBatch::new(
            previous_batch.desc.clone(),
            parts,
            len,
            previous_batch.run_meta.clone(),
            previous_batch.run_splits.clone(),
        )
    }

    /// Compacts runs together. If the input runs are sorted, a single run will be created as output.
    ///
    /// Maximum possible memory usage is `(# runs + 2) * [crate::PersistConfig::blob_target_size]`
    pub(crate) async fn compact_runs(
        cfg: &CompactConfig,
        shard_id: &ShardId,
        desc: &Description<T>,
        mut runs: Vec<(&Description<T>, &RunMeta, &[RunPart<T>])>,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        isolated_runtime: Arc<IsolatedRuntime>,
        write_schemas: Schemas<K, V>,
        batch_so_far: &Option<HollowBatch<T>>,
        incremental_tx: Option<Sender<HollowBatch<T>>>,
    ) -> Result<HollowBatch<T>, anyhow::Error> {
        // TODO: Figure out a more principled way to allocate our memory budget.
        // Currently, we give any excess budget to write parallelism. If we had
        // to pick between 100% towards writes vs 100% towards reads, then reads
        // is almost certainly better, but the ideal is probably somewhere in
        // between the two.
        //
        // For now, invent some some extra budget out of thin air for prefetch.
        let prefetch_budget_bytes = 2 * cfg.batch.blob_target_size;

        let mut timings = Timings::default();

        let mut batch_cfg = cfg.batch.clone();

        let mut lower_bound = None;

        // Use compaction as a method of getting inline writes out of state, to
        // make room for more inline writes. We could instead do this at the end
        // of compaction by flushing out the batch, but doing it here based on
        // the config allows BatchBuilder to do its normal pipelining of writes.
        batch_cfg.inline_writes_single_max_bytes = 0;

        if let Some(batch_so_far) = batch_so_far.as_ref() {
            let last_part = batch_so_far
                .last_part(shard_id.clone(), &*blob, &metrics)
                .await;
            if let Some(last_part) = last_part {
                let fetched = EncodedPart::fetch(
                    shard_id,
                    &*blob,
                    &metrics,
                    &shard_metrics,
                    &metrics.read.batch_fetcher,
                    &batch_so_far.desc,
                    &last_part,
                )
                .await
                .map_err(|blob_key| anyhow!("missing key {blob_key}"))?;

                let updates = fetched.normalize(&metrics.columnar);
                let structured = updates
                    .as_structured::<K, V>(write_schemas.key.as_ref(), write_schemas.val.as_ref());
                let part = match structured.as_part() {
                    Some(p) => p,
                    None => return Err(anyhow!("unexpected empty part")),
                };

                let last = part.len() - 1;
                let key_bound = ArrayBound::new(Arc::clone(&part.key), last);
                let val_bound = ArrayBound::new(Arc::clone(&part.val), last);
                let t = T::decode(part.time.values()[last].to_le_bytes());
                lower_bound = Some(LowerBound {
                    val_bound,
                    key_bound,
                    t,
                });
            }
        };

        if let Some(lower_bound) = lower_bound.as_ref() {
            for (_, _, run) in &mut runs {
                let start = run
                    .iter()
                    .position(|part| {
                        part.structured_key_lower()
                            .map_or(true, |lower| lower.get() >= lower_bound.key_bound.get())
                    })
                    .unwrap_or(run.len());

                *run = &run[start.saturating_sub(1)..];
            }
        }

        let parts = BatchParts::new_ordered::<D>(
            batch_cfg,
            cfg.batch.preferred_order,
            Arc::clone(&metrics),
            Arc::clone(&shard_metrics),
            *shard_id,
            Arc::clone(&blob),
            Arc::clone(&isolated_runtime),
            &metrics.compaction.batch,
        );
        let mut batch = BatchBuilderInternal::<K, V, T, D>::new(
            cfg.batch.clone(),
            parts,
            Arc::clone(&metrics),
            write_schemas.clone(),
            Arc::clone(&blob),
            shard_id.clone(),
            cfg.version.clone(),
        );

        let mut consolidator = Consolidator::new(
            format!(
                "{}[lower={:?},upper={:?}]",
                shard_id,
                desc.lower().elements(),
                desc.upper().elements()
            ),
            *shard_id,
            StructuredSort::<K, V, T, D>::new(write_schemas.clone()),
            blob,
            Arc::clone(&metrics),
            shard_metrics,
            metrics.read.compaction.clone(),
            FetchBatchFilter::Compaction {
                since: desc.since().clone(),
            },
            lower_bound,
            prefetch_budget_bytes,
        );

        for (desc, meta, parts) in runs {
            consolidator.enqueue_run(desc, meta, parts.iter().cloned());
        }

        let remaining_budget = consolidator.start_prefetches();
        if remaining_budget.is_none() {
            metrics.compaction.not_all_prefetched.inc();
        }

        loop {
            let mut chunks = vec![];
            let mut total_bytes = 0;
            // We attempt to pull chunks out of the consolidator that match our target size,
            // but it's possible that we may get smaller chunks... for example, if not all
            // parts have been fetched yet. Loop until we've got enough data to justify flushing
            // it out to blob (or we run out of data.)
            while total_bytes < cfg.batch.blob_target_size {
                let fetch_start = Instant::now();
                let Some(chunk) = consolidator
                    .next_chunk(
                        cfg.compaction_yield_after_n_updates,
                        cfg.batch.blob_target_size - total_bytes,
                    )
                    .await?
                else {
                    break;
                };
                timings.part_fetching += fetch_start.elapsed();
                total_bytes += chunk.goodbytes();
                chunks.push(chunk);
                tokio::task::yield_now().await;
            }

            // In the hopefully-common case of a single chunk, this will not copy.
            let Some(updates) = Part::concat(&chunks).expect("compaction produces well-typed data")
            else {
                break;
            };

            batch.flush_part(desc.clone(), updates).await;

            if let Some(tx) = incremental_tx.as_ref() {
                // This is where we record whatever parts were successfully flushed
                // to blob. That way we can resume an interrupted compaction later.
                let partial_batch = batch.batch_with_finished_parts(desc.clone());

                if let Some(partial_batch) = partial_batch {
                    let hollow_batch = if let Some(batch_so_far) = batch_so_far.as_ref() {
                        Self::combine_hollow_batch_with_previous(batch_so_far, &partial_batch)
                    } else {
                        partial_batch
                    };
                    match tx.send(hollow_batch).await {
                        Ok(_) => {
                            // metrics.compaction.incremental_batch_sent.inc();
                        }
                        Err(e) => {
                            error!("Failed to send batch to incremental compaction: {}", e);
                            // metrics.compaction.incremental_batch_send_fail.inc()
                        }
                    };
                }
            }
        }
        let mut batch = batch.finish(desc.clone()).await?;

        // We use compaction as a method of getting inline writes out of state,
        // to make room for more inline writes. This happens in
        // `CompactConfig::new` by overriding the inline writes threshold
        // config. This is a bit action-at-a-distance, so defensively detect if
        // this breaks here and log and correct it if so.
        let has_inline_parts = batch.batch.parts.iter().any(|x| x.is_inline());
        if has_inline_parts {
            error!(%shard_id, ?cfg, "compaction result unexpectedly had inline writes");
            let () = batch
                .flush_to_blob(
                    &cfg.batch,
                    &metrics.compaction.batch,
                    &isolated_runtime,
                    &write_schemas,
                )
                .await;
        }

        let hollow_batch = if let Some(batch_so_far) = batch_so_far.as_ref() {
            let hollow_batch = batch.into_hollow_batch();
            Self::combine_hollow_batch_with_previous(batch_so_far, &hollow_batch)
        } else {
            batch.into_hollow_batch()
        };

        timings.record(&metrics);
        Ok(hollow_batch)
    }

    fn validate_req(req: &CompactReq<T>) -> Result<(), anyhow::Error> {
        let mut frontier = req.desc.lower();
        for input in req.inputs.iter().map(|x| &x.batch) {
            if PartialOrder::less_than(req.desc.since(), input.desc.since()) {
                return Err(anyhow!(
                    "output since {:?} must be at or in advance of input since {:?}",
                    req.desc.since(),
                    input.desc.since()
                ));
            }
            if frontier != input.desc.lower() {
                return Err(anyhow!(
                    "invalid merge of non-consecutive batches {:?} vs {:?}",
                    frontier,
                    input.desc.lower()
                ));
            }
            frontier = input.desc.upper();
        }
        if frontier != req.desc.upper() {
            return Err(anyhow!(
                "invalid merge of non-consecutive batches {:?} vs {:?}",
                frontier,
                req.desc.upper()
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Timings {
    part_fetching: Duration,
    heap_population: Duration,
}

impl Timings {
    fn record(self, metrics: &Metrics) {
        // intentionally deconstruct so we don't forget to consider each field
        let Timings {
            part_fetching,
            heap_population,
        } = self;

        metrics
            .compaction
            .steps
            .part_fetch_seconds
            .inc_by(part_fetching.as_secs_f64());
        metrics
            .compaction
            .steps
            .heap_population_seconds
            .inc_by(heap_population.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use clap::Id;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::{assert_contains, assert_err};
    use mz_persist_types::codec_impls::StringSchema;
    use timely::order::Product;
    use timely::progress::Antichain;

    use crate::PersistLocation;
    use crate::batch::BLOB_TARGET_SIZE;
    use crate::cfg::BATCH_BUILDER_MAX_OUTSTANDING_PARTS;
    use crate::internal::trace::SpineId;
    use crate::tests::{all_ok, expect_fetch_part, new_test_client_cache};

    use super::*;

    // A regression test for a bug caught during development of materialize#13160 (never
    // made it to main) where batches written by compaction would always have a
    // since of the minimum timestamp.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn regression_minimum_since(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("0".to_owned(), "zero".to_owned()), 1, -1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
        ];

        let cache = new_test_client_cache(&dyncfgs);
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 100);
        let (mut write, _) = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        let b0 = write
            .expect_batch(&data[..1], 0, 1)
            .await
            .into_hollow_batch();
        let b1 = write
            .expect_batch(&data[1..], 1, 2)
            .await
            .into_hollow_batch();

        let req = CompactReq {
            shard_id: write.machine.shard_id(),
            desc: Description::new(
                b0.desc.lower().clone(),
                b1.desc.upper().clone(),
                Antichain::from_elem(10u64),
            ),
            inputs: vec![
                IdHollowBatch {
                    batch: Arc::new(b0),
                    id: SpineId(0, 1),
                },
                IdHollowBatch {
                    batch: Arc::new(b1),
                    id: SpineId(1, 2),
                },
            ],
            prev_batch: None,
        };
        let schemas = Schemas {
            id: None,
            key: Arc::new(StringSchema),
            val: Arc::new(StringSchema),
        };
        let stream = Compactor::<String, String, u64, i64>::compact_stream(
            CompactConfig::new(&write.cfg, write.shard_id()),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            write.metrics.shards.shard(&write.machine.shard_id(), ""),
            Arc::new(IsolatedRuntime::default()),
            req.clone(),
            schemas.clone(),
            &write.machine,
        );

        let res = Compactor::<String, String, u64, i64>::compact_all(stream, req.clone())
            .await
            .expect("compaction failed");

        assert_eq!(res.output.desc, req.desc);
        assert_eq!(res.output.len, 1);
        assert_eq!(res.output.part_count(), 1);
        let part = res.output.parts[0].expect_hollow_part();
        let (part, updates) = expect_fetch_part(
            write.blob.as_ref(),
            &part.key.complete(&write.machine.shard_id()),
            &write.metrics,
            &schemas,
        )
        .await;
        assert_eq!(part.desc, res.output.desc);
        assert_eq!(updates, all_ok(&data, 10));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn compaction_partial_order(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("0".to_owned(), "zero".to_owned()), Product::new(0, 10), 1),
            (("1".to_owned(), "one".to_owned()), Product::new(10, 0), 1),
        ];

        let cache = new_test_client_cache(&dyncfgs);
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 100);
        let (mut write, _) = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
            .expect_open::<String, String, Product<u32, u32>, i64>(ShardId::new())
            .await;
        let b0 = write
            .batch(
                &data[..1],
                Antichain::from_elem(Product::new(0, 0)),
                Antichain::from_iter([Product::new(0, 11), Product::new(10, 0)]),
            )
            .await
            .expect("invalid usage")
            .into_hollow_batch();

        let b1 = write
            .batch(
                &data[1..],
                Antichain::from_iter([Product::new(0, 11), Product::new(10, 0)]),
                Antichain::from_elem(Product::new(10, 1)),
            )
            .await
            .expect("invalid usage")
            .into_hollow_batch();

        let req = CompactReq {
            shard_id: write.machine.shard_id(),
            desc: Description::new(
                b0.desc.lower().clone(),
                b1.desc.upper().clone(),
                Antichain::from_elem(Product::new(10, 0)),
            ),
            inputs: vec![
                IdHollowBatch {
                    batch: Arc::new(b0),
                    id: SpineId(0, 1),
                },
                IdHollowBatch {
                    batch: Arc::new(b1),
                    id: SpineId(1, 2),
                },
            ],
            prev_batch: None,
        };
        let schemas = Schemas {
            id: None,
            key: Arc::new(StringSchema),
            val: Arc::new(StringSchema),
        };
        let stream = Compactor::<String, String, Product<u32, u32>, i64>::compact_stream(
            CompactConfig::new(&write.cfg, write.shard_id()),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            write.metrics.shards.shard(&write.machine.shard_id(), ""),
            Arc::new(IsolatedRuntime::default()),
            req.clone(),
            schemas.clone(),
            &write.machine,
        );

        let res =
            Compactor::<String, String, Product<u32, u32>, i64>::compact_all(stream, req.clone())
                .await
                .expect("compaction failed");

        assert_eq!(res.output.desc, req.desc);
        assert_eq!(res.output.len, 2);
        assert_eq!(res.output.part_count(), 1);
        let part = res.output.parts[0].expect_hollow_part();
        let (part, updates) = expect_fetch_part(
            write.blob.as_ref(),
            &part.key.complete(&write.machine.shard_id()),
            &write.metrics,
            &schemas,
        )
        .await;
        assert_eq!(part.desc, res.output.desc);
        assert_eq!(updates, all_ok(&data, Product::new(10, 0)));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn disable_compaction(dyncfgs: ConfigUpdates) {
        let data = [
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("0".to_owned(), "zero".to_owned()), 1, -1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
        ];

        let cache = new_test_client_cache(&dyncfgs);
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 100);
        let (mut write, _) = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        let b0 = write
            .expect_batch(&data[..1], 0, 1)
            .await
            .into_hollow_batch();
        let b1 = write
            .expect_batch(&data[1..], 1, 2)
            .await
            .into_hollow_batch();

        let req = CompactReq {
            shard_id: write.machine.shard_id(),
            desc: Description::new(
                b0.desc.lower().clone(),
                b1.desc.upper().clone(),
                Antichain::from_elem(10u64),
            ),
            inputs: vec![
                IdHollowBatch {
                    batch: Arc::new(b0),
                    id: SpineId(0, 1),
                },
                IdHollowBatch {
                    batch: Arc::new(b1),
                    id: SpineId(1, 2),
                },
            ],
            prev_batch: None,
        };
        write.cfg.set_config(&COMPACTION_HEURISTIC_MIN_INPUTS, 1);
        let compactor = write.compact.as_ref().expect("compaction hard disabled");

        write.cfg.disable_compaction();
        let result = compactor
            .compact_and_apply_background(req.clone(), &write.machine)
            .expect("listener")
            .await
            .expect("channel closed");
        assert_err!(result);
        assert_contains!(result.unwrap_err().to_string(), "compaction disabled");

        write.cfg.enable_compaction();
        compactor
            .compact_and_apply_background(req, &write.machine)
            .expect("listener")
            .await
            .expect("channel closed")
            .expect("compaction success");

        // Make sure our CYA dyncfg works.
        let data2 = [
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("2".to_owned(), "two".to_owned()), 3, -1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let b2 = write
            .expect_batch(&data2[..1], 2, 3)
            .await
            .into_hollow_batch();
        let b3 = write
            .expect_batch(&data2[1..], 3, 4)
            .await
            .into_hollow_batch();

        let req = CompactReq {
            shard_id: write.machine.shard_id(),
            desc: Description::new(
                b2.desc.lower().clone(),
                b3.desc.upper().clone(),
                Antichain::from_elem(20u64),
            ),
            inputs: vec![
                IdHollowBatch {
                    batch: Arc::new(b2),
                    id: SpineId(0, 1),
                },
                IdHollowBatch {
                    batch: Arc::new(b3),
                    id: SpineId(1, 2),
                },
            ],
            prev_batch: None,
        };
        let compactor = write.compact.as_ref().expect("compaction hard disabled");

        // When the dyncfg is set to false we should ignore the process flag.
        write.cfg.set_config(&COMPACTION_CHECK_PROCESS_FLAG, false);
        write.cfg.disable_compaction();
        // Compaction still succeeded!
        compactor
            .compact_and_apply_background(req, &write.machine)
            .expect("listener")
            .await
            .expect("channel closed")
            .expect("compaction success");
    }

    #[mz_persist_proc::test(tokio::test)]
    // #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn incremental_compaction(dyncfgs: ConfigUpdates) {
        // let dyncfgs = ::mz_dyncfg::ConfigUpdates::default();
        // Generate a bunch of data and batches for testing incremental compaction.
        let mut data = Vec::new();
        let num_keys = 10;
        let num_times = 10;
        for time in 0..num_times {
            for key in 0..num_keys {
                // Ensure time is monotonically increasing across all keys
                let t = time * num_keys + key;
                data.push(((key.to_string(), format!("val_{key}")), t as u64, 1));
            }
        }

        let cache = new_test_client_cache(&dyncfgs);
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 100);

        let (mut write, _) = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // Split data into batches of 3 updates each.
        let batch_size = 3;
        let mut batches = Vec::new();
        let mut lower = 0;
        while lower < data.len() {
            let upper = (lower + batch_size).min(data.len());
            let batch = write
                .expect_batch(&data[lower..upper], lower as u64, upper as u64)
                .await
                .into_hollow_batch();
            let batch = IdHollowBatch {
                batch: Arc::new(batch),
                id: SpineId(lower, upper),
            };
            batches.push(batch);
            lower = upper;
        }

        let req = CompactReq {
            shard_id: write.machine.shard_id(),
            desc: Description::new(
                batches.first().unwrap().batch.desc.lower().clone(),
                batches.last().unwrap().batch.desc.upper().clone(),
                Antichain::from_elem(10u64),
            ),
            inputs: batches.clone(),
            prev_batch: None,
        };
        let schemas = Schemas {
            id: None,
            key: Arc::new(StringSchema),
            val: Arc::new(StringSchema),
        };
        let ordered_runs = Compactor::<String, String, u64, i64>::order_runs(
            &req,
            RunOrder::Structured,
            &*write.blob,
            &write.metrics,
        )
        .await
        .expect("order runs failed");

        // Set this to an arbitrarily small number to force writes to flush
        // to blob.
        write
            .cfg
            .set_config(&BATCH_BUILDER_MAX_OUTSTANDING_PARTS, 1);

        // Set this to an arbitrarily small number to force multiple parts to be
        // written.
        write.cfg.set_config(&BLOB_TARGET_SIZE, 10);

        let cfg = CompactConfig::new(&write.cfg, write.shard_id());

        let chunked_runs = Compactor::<String, String, u64, i64>::chunk_runs(
            &ordered_runs,
            &cfg,
            &write.metrics,
            1000000,
        );

        let mut incremental_result = None;
        let mut first_batch = None;
        let mut second_batch = None;

        let chunked_runs_clone = chunked_runs.clone();

        for (runs, _max_memory) in chunked_runs_clone.iter() {
            let runs = runs
                .iter()
                .map(|(_, desc, meta, run)| (desc.clone(), meta.clone(), run.clone()))
                .collect::<Vec<_>>();
            let (incremental_tx, mut incremental_rx) = tokio::sync::mpsc::channel(1);
            let shard_id = write.shard_id();

            let batch_handle = Compactor::<String, String, u64, i64>::compact_runs(
                &cfg,
                &shard_id,
                &req.desc,
                runs.clone(),
                Arc::clone(&write.blob),
                Arc::clone(&write.metrics),
                write.metrics.shards.shard(&write.machine.shard_id(), ""),
                Arc::new(IsolatedRuntime::default()),
                schemas.clone(),
                &None,
                Some(incremental_tx),
            );

            let incremental_handle = tokio::spawn(async move {
                let mut batches = vec![];
                while let Some(b) = incremental_rx.recv().await {
                    batches.push(b);
                }
                batches
            });

            let (batch_result, incremental) = tokio::join! {
                batch_handle,
                incremental_handle,
            };
            let incremental = incremental.unwrap();

            incremental_result = Some(incremental[incremental.len() - 1].clone());
            first_batch = Some(batch_result.unwrap());
        }

        for (runs, _max_memory) in chunked_runs.iter() {
            let runs = runs
                .iter()
                .map(|(_, desc, meta, run)| (desc.clone(), meta.clone(), run.clone()))
                .collect::<Vec<_>>();
            let (incremental_tx, mut incremental_rx) = tokio::sync::mpsc::channel(1);
            let shard_id = write.shard_id();

            let batch_handle = Compactor::<String, String, u64, i64>::compact_runs(
                &cfg,
                &shard_id,
                &req.desc,
                runs.clone(),
                Arc::clone(&write.blob),
                Arc::clone(&write.metrics),
                write.metrics.shards.shard(&write.machine.shard_id(), ""),
                Arc::new(IsolatedRuntime::default()),
                schemas.clone(),
                &incremental_result,
                Some(incremental_tx),
            );

            let incremental_handle = tokio::spawn(async move {
                let mut batch = None;
                while let Some(b) = incremental_rx.recv().await {
                    batch = Some(b);
                }
                batch
            });

            let (batch_result, _incremental) = tokio::join! {
                batch_handle,
                incremental_handle,
            };

            let batch_result = batch_result.unwrap();
            second_batch = Some(batch_result.clone());
        }

        // We want to assert that the first batch is equal to the second batch,
        // _except_ for the last part, which is where the second run should have
        // picked up incrementally.
        let (mut first_batch, mut second_batch) = (
            first_batch.expect("first batch"),
            second_batch.expect("second batch"),
        );
        let first_diffs_sum = first_batch
            .parts
            .iter()
            .map(|part| {
                part.diffs_sum::<i64>(&write.metrics.columnar)
                    .expect("diffs sum")
            })
            .sum::<i64>();
        let second_diffs_sum = second_batch
            .parts
            .iter()
            .map(|part| {
                part.diffs_sum::<i64>(&write.metrics.columnar)
                    .expect("diffs sum")
            })
            .sum::<i64>();

        assert_eq!(first_diffs_sum, second_diffs_sum);
        // assert_eq!(first_batch.len, second_batch.len);

        let first = first_batch.parts.pop();
        let second = second_batch.parts.pop();
        info!("first={:?} second={:?}", first, second);

        assert_eq!(first_batch, second_batch);

        println!("chunked_runs={:#?}", chunked_runs.iter().len());
    }
}
