// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::{Stream, pin_mut};
use futures_util::StreamExt;
use itertools::Either;
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::now::NowFn;
use mz_ore::soft_assert_or_log;
use mz_persist::location::Blob;
use mz_persist_types::part::Part;
use mz_persist_types::{Codec, Codec64};
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::Sender;
use tokio::sync::{TryAcquireError, mpsc, oneshot};
use tracing::{Instrument, Span, debug, debug_span, error, trace, warn};

use crate::async_runtime::IsolatedRuntime;
use crate::batch::{BatchBuilderConfig, BatchBuilderInternal, BatchParts, PartDeletes};
use crate::cfg::{
    COMPACTION_HEURISTIC_MIN_INPUTS, COMPACTION_HEURISTIC_MIN_PARTS,
    COMPACTION_HEURISTIC_MIN_UPDATES, COMPACTION_MEMORY_BOUND_BYTES,
    GC_BLOB_DELETE_CONCURRENCY_LIMIT, MiB,
};
use crate::fetch::{FetchBatchFilter, FetchConfig};
use crate::internal::encoding::Schemas;
use crate::internal::gc::GarbageCollector;
use crate::internal::machine::Machine;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::ShardMetrics;
use crate::internal::state::{HollowBatch, RunMeta, RunOrder, RunPart};
use crate::internal::trace::{
    ActiveCompaction, ApplyMergeResult, CompactionInput, FueledMergeRes, IdHollowBatch, SpineId,
    id_range,
};
use crate::iter::{Consolidator, StructuredSort};
use crate::{Metrics, PersistConfig, ShardId};

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
    pub inputs: Vec<IdHollowBatch<T>>,
}

/// A response from compaction.
#[derive(Debug)]
pub struct CompactRes<T> {
    /// The compacted batch.
    pub output: HollowBatch<T>,
    /// The runs that were compacted together to produce the output batch.
    pub input: CompactionInput,
}

/// A snapshot of dynamic configs to make it easier to reason about an
/// individual run of compaction.
#[derive(Debug, Clone)]
pub struct CompactConfig {
    pub(crate) compaction_memory_bound_bytes: usize,
    pub(crate) compaction_yield_after_n_updates: usize,
    pub(crate) version: semver::Version,
    pub(crate) batch: BatchBuilderConfig,
    pub(crate) fetch_config: FetchConfig,
    pub(crate) now: NowFn,
}

impl CompactConfig {
    /// Initialize the compaction config from Persist configuration.
    pub fn new(value: &PersistConfig, shard_id: ShardId) -> Self {
        CompactConfig {
            compaction_memory_bound_bytes: COMPACTION_MEMORY_BOUND_BYTES.get(value),
            compaction_yield_after_n_updates: value.compaction_yield_after_n_updates,
            version: value.build_version.clone(),
            batch: BatchBuilderConfig::new(value, shard_id),
            fetch_config: FetchConfig::from_persist_config(value),
            now: value.now.clone(),
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

pub(crate) const COMPACTION_CHECK_PROCESS_FLAG: Config<bool> = Config::new(
    "persist_compaction_check_process_flag",
    true,
    "Whether Compactor will obey the process_requests flag in PersistConfig, \
        which allows dynamically disabling compaction. If false, all compaction requests will be processed.",
);

/// Create a `[CompactionInput::IdRange]` from a set of `SpineId`s.
fn input_id_range(ids: BTreeSet<SpineId>) -> CompactionInput {
    let id = id_range(ids);

    CompactionInput::IdRange(id)
}

impl<K, V, T, D> Compactor<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Ord + Codec64 + Send + Sync,
{
    pub fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
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

                let compact_span =
                    debug_span!(parent: None, "compact::apply", shard_id=%machine.shard_id());
                compact_span.follows_from(&Span::current());
                let gc = gc.clone();
                mz_ore::task::spawn(|| "PersistCompactionWorker", async move {
                    let res = Self::compact_and_apply(&machine, req)
                        .instrument(compact_span)
                        .await;

                    match res {
                        Ok(maintenance) => maintenance.start_performing(&machine, &gc),
                        Err(err) => {
                            debug!(shard_id =? machine.shard_id(), "compaction failed: {err:#}")
                        }
                    }

                    // we can safely ignore errors here, it's possible the caller
                    // wasn't interested in waiting and dropped their receiver
                    let _ = completer.send(Ok(()));

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
    ) -> Result<RoutineMaintenance, anyhow::Error> {
        let metrics = Arc::clone(&machine.applier.metrics);
        metrics.compaction.started.inc();
        let start = Instant::now();

        // pick a timeout for our compaction request proportional to the amount
        // of data that must be read (with a minimum set by PersistConfig)
        let total_input_bytes = req
            .inputs
            .iter()
            .map(|batch| batch.batch.encoded_size_bytes())
            .sum::<usize>();
        let timeout = Duration::max(
            // either our minimum timeout
            COMPACTION_MINIMUM_TIMEOUT.get(&machine.applier.cfg),
            // or 1s per MB of input data
            Duration::from_secs(u64::cast_from(total_input_bytes / MiB)),
        );
        // always use most recent schema from all the Runs we're compacting to prevent Compactors
        // created before the schema was evolved, from trying to "de-evolve" a Part.
        let Some(compaction_schema_id) = req
            .inputs
            .iter()
            .flat_map(|batch| batch.batch.run_meta.iter())
            .filter_map(|run_meta| run_meta.schema)
            // It's an invariant that SchemaIds are ordered.
            .max()
        else {
            metrics.compaction.schema_selection.no_schema.inc();
            metrics.compaction.failed.inc();
            return Err(anyhow!(
                "compacting {shard_id} and spine ids {spine_ids}: could not determine schema id from inputs",
                shard_id = req.shard_id,
                spine_ids = mz_ore::str::separated(", ", req.inputs.iter().map(|i| i.id))
            ));
        };
        let Some((key_schema, val_schema)) = machine.get_schema(compaction_schema_id) else {
            metrics.compaction.schema_selection.no_schema.inc();
            metrics.compaction.failed.inc();
            return Err(anyhow!(
                "compacting {shard_id} and spine ids {spine_ids}: schema id {compaction_schema_id} not present in machine state",
                shard_id = req.shard_id,
                spine_ids = mz_ore::str::separated(", ", req.inputs.iter().map(|i| i.id))
            ));
        };

        metrics.compaction.schema_selection.recent_schema.inc();

        let compaction_schema = Schemas {
            id: Some(compaction_schema_id),
            key: Arc::new(key_schema),
            val: Arc::new(val_schema),
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
        let res = tokio::time::timeout(
            timeout,
            // Compaction is cpu intensive, so be polite and spawn it on the isolated runtime.
            isolated_runtime.spawn_named(
                || "persist::compact::consolidate",
                async move {
                    // If the batches we are compacting are written with old versions of persist,
                    // we may not have run UUIDs for them, meaning we don't have enough info to
                    // safely compact them incrementally.
                    let all_runs_have_uuids = req
                        .inputs
                        .iter()
                        .all(|x| x.batch.runs().all(|(meta, _)| meta.id.is_some()));
                    let all_runs_have_len = req
                        .inputs
                        .iter()
                        .all(|x| x.batch.runs().all(|(meta, _)| meta.len.is_some()));

                    let compact_cfg =
                        CompactConfig::new(&machine_clone.applier.cfg, machine_clone.shard_id());
                    let incremental_enabled = compact_cfg.batch.enable_incremental_compaction
                        && all_runs_have_uuids
                        && all_runs_have_len;
                    let stream = Self::compact_stream(
                        compact_cfg,
                        Arc::clone(&machine_clone.applier.state_versions.blob),
                        Arc::clone(&metrics_clone),
                        Arc::clone(&machine_clone.applier.shard_metrics),
                        Arc::clone(&machine_clone.isolated_runtime),
                        req.clone(),
                        compaction_schema,
                        incremental_enabled,
                    );

                    let maintenance = if incremental_enabled {
                        let mut maintenance = RoutineMaintenance::default();
                        pin_mut!(stream);
                        while let Some(res) = stream.next().await {
                            let res = res?;
                            let new_maintenance =
                                Self::apply(res, &metrics_clone, &machine_clone).await?;
                            maintenance.merge(new_maintenance);
                        }
                        maintenance
                    } else {
                        let res = Self::compact_all(stream, req.clone()).await?;
                        Self::apply(
                            FueledMergeRes {
                                output: res.output,
                                input: CompactionInput::Legacy,
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
            ),
        )
        .await;

        metrics
            .compaction
            .seconds
            .inc_by(start.elapsed().as_secs_f64());
        let res = res.map_err(|e| {
            metrics.compaction.timed_out.inc();
            anyhow!(
                "compaction timed out after {}s: {}",
                timeout.as_secs_f64(),
                e
            )
        })?;

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
            let res = res?.output;
            let (parts, updates, run_meta, run_splits) =
                (res.parts, res.len, res.run_meta, res.run_splits);

            if updates == 0 {
                continue;
            }

            let run_offset = all_parts.len();
            if !all_parts.is_empty() {
                all_run_splits.push(run_offset);
            }
            all_run_splits.extend(run_splits.iter().map(|r| r + run_offset));
            all_run_meta.extend(run_meta);
            all_parts.extend(parts);
            len += updates;
        }

        let batches = req.inputs.iter().map(|x| x.id).collect::<BTreeSet<_>>();
        let input = input_id_range(batches);

        Ok(CompactRes {
            output: HollowBatch::new(
                req.desc.clone(),
                all_parts,
                len,
                all_run_meta,
                all_run_splits,
            ),
            input,
        })
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
                    part_deletes.add(part);
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
        incremental_enabled: bool,
    ) -> impl Stream<Item = Result<FueledMergeRes<T>, anyhow::Error>> {
        async_stream::stream! {
            let () = Self::validate_req(&req)?;

            // We introduced a fast-path optimization in https://github.com/MaterializeInc/materialize/pull/15363
            // but had to revert it due to a very scary bug. Here we count how many of our compaction reqs
            // could be eligible for the optimization to better understand whether it's worth trying to
            // reintroduce it.
            let mut single_nonempty_batch = None;
            for batch in &req.inputs {
                if batch.batch.len > 0 {
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
                if single_nonempty_batch.batch.run_splits.len() == 0
                    && single_nonempty_batch.batch.desc.since() != &Antichain::from_elem(T::minimum())
                {
                    metrics.compaction.fast_path_eligible.inc();
                }
            }

            // Reserve space for the in-progress part to be held in-mem representation and columnar -
            let in_progress_part_reserved_memory_bytes = 2 * cfg.batch.blob_target_size;
            // - then remaining memory will go towards pulling down as many runs as we can.
            // We'll always do at least two runs per chunk, which means we may go over this limit
            // if parts are large or the limit is low... though we do at least increment a metric
            // when that happens.
            let run_reserved_memory_bytes = cfg
                .compaction_memory_bound_bytes
                .saturating_sub(in_progress_part_reserved_memory_bytes);

            let chunked_runs = Self::chunk_runs(
                &req,
                &cfg,
                &*metrics,
                run_reserved_memory_bytes,
                req.desc.since()
            );
            let total_chunked_runs = chunked_runs.len();

            let parts_before = req.inputs.iter().map(|x| x.batch.parts.len()).sum::<usize>();
            let parts_after = chunked_runs.iter().flat_map(|(_, _, runs, _)| runs.iter().map(|(_, _, parts)| parts.len())).sum::<usize>();
            assert_eq!(parts_before, parts_after, "chunking should not change the number of parts");

            for (applied, (input, desc, runs, run_chunk_max_memory_usage)) in
                chunked_runs.into_iter().enumerate()
            {
                metrics.compaction.chunks_compacted.inc();
                metrics
                    .compaction
                    .runs_compacted
                    .inc_by(u64::cast_from(runs.len()));

                // given the runs we actually have in our batch, we might have extra memory
                // available. we reserved enough space to always have 1 in-progress part in
                // flight, but if we have excess, we can use it to increase our write parallelism
                let extra_outstanding_parts = (run_reserved_memory_bytes
                    .saturating_sub(run_chunk_max_memory_usage))
                    / cfg.batch.blob_target_size;
                let mut run_cfg = cfg.clone();
                run_cfg.batch.batch_builder_max_outstanding_parts = 1 + extra_outstanding_parts;

                let desc = if incremental_enabled {
                    desc
                } else {
                    req.desc.clone()
                };

                let runs = runs.iter()
                    .map(|(desc, meta, run)| (*desc, *meta, *run))
                    .collect::<Vec<_>>();

                let batch = Self::compact_runs(
                    &run_cfg,
                    &req.shard_id,
                    &desc,
                    runs,
                    Arc::clone(&blob),
                    Arc::clone(&metrics),
                    Arc::clone(&shard_metrics),
                    Arc::clone(&isolated_runtime),
                    write_schemas.clone(),
                )
                .await?;

                assert!(
                    (batch.len == 0 && batch.parts.len() == 0) || (batch.len > 0 && batch.parts.len() > 0),
                    "updates={}, parts={}",
                    batch.len,
                    batch.parts.len(),
                );

                // Set up active compaction metadata
                let clock = cfg.now.clone();
                let active_compaction = if applied < total_chunked_runs - 1 {
                    Some(ActiveCompaction { start_ms: clock() })
                } else {
                    None
                };

                let res = CompactRes {
                    output: batch,
                    input,
                };

                let res = FueledMergeRes {
                    output: res.output,
                    new_active_compaction: active_compaction,
                    input: res.input,
                };

                yield Ok(res);
            }
        }
    }

    /// Compacts the input batches together, returning a single compacted batch.
    /// Under the hood this just calls [Self::compact_stream] and
    /// [Self::compact_all], but it is a convenience method that allows
    /// the caller to not have to deal with the streaming API.
    pub async fn compact(
        cfg: CompactConfig,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        isolated_runtime: Arc<IsolatedRuntime>,
        req: CompactReq<T>,
        write_schemas: Schemas<K, V>,
    ) -> Result<CompactRes<T>, anyhow::Error> {
        let stream = Self::compact_stream(
            cfg,
            Arc::clone(&blob),
            Arc::clone(&metrics),
            Arc::clone(&shard_metrics),
            Arc::clone(&isolated_runtime),
            req.clone(),
            write_schemas,
            false,
        );

        Self::compact_all(stream, req).await
    }

    /// Chunks runs with the following rules:
    /// 1. Runs from multiple batches are allowed to be mixed as long as _every_ run in the
    ///    batch is present in the chunk.
    /// 2. Otherwise, runs are split into chunks of runs from a single batch.
    fn chunk_runs<'a>(
        req: &'a CompactReq<T>,
        cfg: &CompactConfig,
        metrics: &Metrics,
        run_reserved_memory_bytes: usize,
        since: &Antichain<T>,
    ) -> Vec<(
        CompactionInput,
        Description<T>,
        Vec<(&'a Description<T>, &'a RunMeta, &'a [RunPart<T>])>,
        usize,
    )> {
        // Assert that all of the inputs are contiguous / can be compacted together.
        let _ = input_id_range(req.inputs.iter().map(|x| x.id).collect());

        // Iterate through batches by spine id.
        let mut batches: Vec<_> = req.inputs.iter().map(|x| (x.id, &*x.batch)).collect();
        batches.sort_by_key(|(id, _)| *id);

        let mut chunks = vec![];
        let mut current_chunk_ids = BTreeSet::new();
        let mut current_chunk_descs = Vec::new();
        let mut current_chunk_runs = vec![];
        let mut current_chunk_max_memory_usage = 0;

        fn max_part_bytes<T>(parts: &[RunPart<T>], cfg: &CompactConfig) -> usize {
            parts
                .iter()
                .map(|p| p.max_part_bytes())
                .max()
                .unwrap_or(cfg.batch.blob_target_size)
        }

        fn desc_range<T: Timestamp>(
            descs: impl IntoIterator<Item = Description<T>>,
            since: Antichain<T>,
        ) -> Description<T> {
            let mut descs = descs.into_iter();
            let first = descs.next().expect("non-empty set of descriptions");
            let lower = first.lower().clone();
            let mut upper = first.upper().clone();
            for desc in descs {
                assert_eq!(&upper, desc.lower());
                upper = desc.upper().clone();
            }
            let upper = upper.clone();
            Description::new(lower, upper, since)
        }

        for (spine_id, batch) in batches {
            let batch_size = batch
                .runs()
                .map(|(_, parts)| max_part_bytes(parts, cfg))
                .sum::<usize>();

            let num_runs = batch.run_meta.len();

            let runs = batch.runs().flat_map(|(meta, parts)| {
                if meta.order.unwrap_or(RunOrder::Codec) == cfg.batch.preferred_order {
                    Either::Left(std::iter::once((&batch.desc, meta, parts)))
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
                    soft_assert_or_log!(
                        !parts.iter().any(|r| matches!(r, RunPart::Many(_))),
                        "unexpected out-of-order hollow run"
                    );
                    Either::Right(
                        parts
                            .iter()
                            .map(move |p| (&batch.desc, meta, std::slice::from_ref(p))),
                    )
                }
            });

            // Combine the given batch into the current chunk
            // - if they fit within the memory budget,
            // - if both have only at most a single run, so otherwise compaction wouldn't make progress.
            if current_chunk_max_memory_usage + batch_size <= run_reserved_memory_bytes
                || current_chunk_runs.len() + num_runs <= 2
            {
                if current_chunk_max_memory_usage + batch_size > run_reserved_memory_bytes {
                    // We've chosen to merge these batches together despite being over budget,
                    // which should be rare.
                    metrics.compaction.memory_violations.inc();
                }
                current_chunk_ids.insert(spine_id);
                current_chunk_descs.push(batch.desc.clone());
                current_chunk_runs.extend(runs);
                current_chunk_max_memory_usage += batch_size;
                continue;
            }

            // Otherwise, we cannot mix this batch partially. Flush any existing mixed chunk first.
            if !current_chunk_ids.is_empty() {
                chunks.push((
                    input_id_range(std::mem::take(&mut current_chunk_ids)),
                    desc_range(mem::take(&mut current_chunk_descs), since.clone()),
                    std::mem::take(&mut current_chunk_runs),
                    current_chunk_max_memory_usage,
                ));
                current_chunk_max_memory_usage = 0;
            }

            // If the batch fits within limits, try and accumulate future batches into it.
            if batch_size <= run_reserved_memory_bytes {
                current_chunk_ids.insert(spine_id);
                current_chunk_descs.push(batch.desc.clone());
                current_chunk_runs.extend(runs);
                current_chunk_max_memory_usage += batch_size;
                continue;
            }

            // This batch is too large to compact with others, or even in a single go.
            // Process this batch alone, splitting into single-batch chunks as needed.
            let mut run_iter = runs.into_iter().peekable();
            debug_assert!(current_chunk_ids.is_empty());
            debug_assert!(current_chunk_descs.is_empty());
            debug_assert!(current_chunk_runs.is_empty());
            debug_assert_eq!(current_chunk_max_memory_usage, 0);
            let mut current_chunk_run_ids = BTreeSet::new();

            while let Some((desc, meta, parts)) = run_iter.next() {
                let run_size = max_part_bytes(parts, cfg);
                current_chunk_runs.push((desc, meta, parts));
                current_chunk_max_memory_usage += run_size;
                current_chunk_run_ids.extend(meta.id);

                if let Some((_, _meta, next_parts)) = run_iter.peek() {
                    let next_size = max_part_bytes(next_parts, cfg);
                    if current_chunk_max_memory_usage + next_size > run_reserved_memory_bytes {
                        // If the current chunk only has one run, record a memory violation metric.
                        if current_chunk_runs.len() == 1 {
                            metrics.compaction.memory_violations.inc();
                            continue;
                        }
                        // Flush the current chunk and start a new one.
                        chunks.push((
                            CompactionInput::PartialBatch(
                                spine_id,
                                mem::take(&mut current_chunk_run_ids),
                            ),
                            desc_range([batch.desc.clone()], since.clone()),
                            std::mem::take(&mut current_chunk_runs),
                            current_chunk_max_memory_usage,
                        ));
                        current_chunk_max_memory_usage = 0;
                    }
                }
            }

            if !current_chunk_runs.is_empty() {
                chunks.push((
                    CompactionInput::PartialBatch(spine_id, mem::take(&mut current_chunk_run_ids)),
                    desc_range([batch.desc.clone()], since.clone()),
                    std::mem::take(&mut current_chunk_runs),
                    current_chunk_max_memory_usage,
                ));
                current_chunk_max_memory_usage = 0;
            }
        }

        // If we ended with a mixed-batch chunk in progress, flush it.
        if !current_chunk_ids.is_empty() {
            chunks.push((
                input_id_range(current_chunk_ids),
                desc_range(current_chunk_descs, since.clone()),
                current_chunk_runs,
                current_chunk_max_memory_usage,
            ));
        }

        chunks
    }

    /// Compacts runs together. If the input runs are sorted, a single run will be created as output.
    ///
    /// Maximum possible memory usage is `(# runs + 2) * [crate::PersistConfig::blob_target_size]`
    pub(crate) async fn compact_runs(
        cfg: &CompactConfig,
        shard_id: &ShardId,
        desc: &Description<T>,
        runs: Vec<(&Description<T>, &RunMeta, &[RunPart<T>])>,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        isolated_runtime: Arc<IsolatedRuntime>,
        write_schemas: Schemas<K, V>,
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

        // Use compaction as a method of getting inline writes out of state, to
        // make room for more inline writes. We could instead do this at the end
        // of compaction by flushing out the batch, but doing it here based on
        // the config allows BatchBuilder to do its normal pipelining of writes.
        batch_cfg.inline_writes_single_max_bytes = 0;

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
            cfg.fetch_config.clone(),
            *shard_id,
            StructuredSort::<K, V, T, D>::new(write_schemas.clone()),
            blob,
            Arc::clone(&metrics),
            shard_metrics,
            metrics.read.compaction.clone(),
            FetchBatchFilter::Compaction {
                since: desc.since().clone(),
            },
            None,
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

        timings.record(&metrics);
        Ok(batch.into_hollow_batch())
    }

    fn validate_req(req: &CompactReq<T>) -> Result<(), anyhow::Error> {
        let mut frontier = req.desc.lower();
        for input in req.inputs.iter() {
            if PartialOrder::less_than(req.desc.since(), input.batch.desc.since()) {
                return Err(anyhow!(
                    "output since {:?} must be at or in advance of input since {:?}",
                    req.desc.since(),
                    input.batch.desc.since()
                ));
            }
            if frontier != input.batch.desc.lower() {
                return Err(anyhow!(
                    "invalid merge of non-consecutive batches {:?} vs {:?}",
                    frontier,
                    input.batch.desc.lower()
                ));
            }
            frontier = input.batch.desc.upper();
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
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::{assert_contains, assert_err};
    use mz_persist_types::codec_impls::StringSchema;
    use timely::progress::Antichain;

    use crate::PersistLocation;
    use crate::batch::BLOB_TARGET_SIZE;
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
        };
        let schemas = Schemas {
            id: None,
            key: Arc::new(StringSchema),
            val: Arc::new(StringSchema),
        };
        let res = Compactor::<String, String, u64, i64>::compact(
            CompactConfig::new(&write.cfg, write.shard_id()),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            write.metrics.shards.shard(&write.machine.shard_id(), ""),
            Arc::new(IsolatedRuntime::new_for_tests()),
            req.clone(),
            schemas.clone(),
        )
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
}
