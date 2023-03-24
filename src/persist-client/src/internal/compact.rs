// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::TryFutureExt;
use mz_ore::cast::CastFrom;
use mz_ore::task::spawn;
use mz_persist::location::Blob;
use mz_persist_types::codec_impls::VecU8Schema;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot, TryAcquireError};
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, trace, warn, Instrument, Span};

use crate::async_runtime::CpuHeavyRuntime;
use crate::batch::{BatchBuilderConfig, BatchBuilderInternal};
use crate::cfg::MB;
use crate::fetch::{fetch_batch_part, EncodedPart};
use crate::internal::encoding::Schemas;
use crate::internal::gc::GarbageCollector;
use crate::internal::machine::{retry_external, Machine};
use crate::internal::state::{HollowBatch, HollowBatchPart};
use crate::internal::trace::{ApplyMergeResult, FueledMergeRes};
use crate::{Metrics, PersistConfig, ShardId, WriterId};

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
    pub inputs: Vec<HollowBatch<T>>,
}

/// A response from compaction.
#[derive(Debug)]
pub struct CompactRes<T> {
    /// The compacted batch.
    pub output: HollowBatch<T>,
}

/// A snapshot of dynamic configs to make it easier to reason about an
/// individual run of compaction.
#[derive(Debug, Clone)]
pub struct CompactConfig {
    pub(crate) compaction_memory_bound_bytes: usize,
    pub(crate) batch: BatchBuilderConfig,
}

impl From<&PersistConfig> for CompactConfig {
    fn from(value: &PersistConfig) -> Self {
        CompactConfig {
            compaction_memory_bound_bytes: value.dynamic.compaction_memory_bound_bytes(),
            batch: BatchBuilderConfig::from(value),
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
        oneshot::Sender<Result<ApplyMergeResult, anyhow::Error>>,
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

impl<K, V, T, D> Compactor<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        writer_id: WriterId,
        schemas: Schemas<K, V>,
        gc: GarbageCollector<K, V, T, D>,
    ) -> Self {
        let (compact_req_sender, mut compact_req_receiver) = mpsc::channel::<(
            Instant,
            CompactReq<T>,
            Machine<K, V, T, D>,
            oneshot::Sender<Result<ApplyMergeResult, anyhow::Error>>,
        )>(cfg.compaction_queue_size);
        let concurrency_limit = Arc::new(tokio::sync::Semaphore::new(
            cfg.compaction_concurrency_limit,
        ));

        // spin off a single task responsible for executing compaction requests.
        // work is enqueued into the task through a channel
        let _worker_handle = mz_ore::task::spawn(|| "PersistCompactionScheduler", async move {
            while let Some((enqueued, req, mut machine, completer)) =
                compact_req_receiver.recv().await
            {
                assert_eq!(req.shard_id, machine.shard_id());
                let metrics = Arc::clone(&machine.applier.metrics);

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

                let cfg = machine.applier.cfg.clone();
                let blob = Arc::clone(&machine.applier.state_versions.blob);
                let cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
                let writer_id = writer_id.clone();
                let schemas = schemas.clone();

                let compact_span =
                    debug_span!(parent: None, "compact::apply", shard_id=%machine.shard_id());
                compact_span.follows_from(&Span::current());
                let gc = gc.clone();
                mz_ore::task::spawn(|| "PersistCompactionWorker", async move {
                    let res = Self::compact_and_apply(
                        cfg,
                        blob,
                        metrics,
                        cpu_heavy_runtime,
                        req,
                        writer_id,
                        schemas,
                        &mut machine,
                        &gc,
                    )
                    .instrument(compact_span)
                    .await;

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
    ) -> Option<oneshot::Receiver<Result<ApplyMergeResult, anyhow::Error>>> {
        // Run some initial heuristics to ignore some requests for compaction.
        // We don't gain much from e.g. compacting two very small batches that
        // were just written, but it does result in non-trivial blob traffic
        // (especially in aggregate). This heuristic is something we'll need to
        // tune over time.
        let should_compact = req.inputs.len() >= self.cfg.dynamic.compaction_heuristic_min_inputs()
            || req.inputs.iter().map(|x| x.parts.len()).sum::<usize>()
                >= self.cfg.dynamic.compaction_heuristic_min_parts()
            || req.inputs.iter().map(|x| x.len).sum::<usize>()
                >= self.cfg.dynamic.compaction_heuristic_min_updates();
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
        // diffs, which may have a different spine structure less amendable to merging.
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

    async fn compact_and_apply(
        cfg: PersistConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        req: CompactReq<T>,
        writer_id: WriterId,
        schemas: Schemas<K, V>,
        machine: &mut Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
    ) -> Result<ApplyMergeResult, anyhow::Error> {
        metrics.compaction.started.inc();
        let start = Instant::now();

        // pick a timeout for our compaction request proportional to the amount
        // of data that must be read (with a minimum set by PersistConfig)
        let total_input_bytes = req
            .inputs
            .iter()
            .flat_map(|batch| batch.parts.iter())
            .map(|parts| parts.encoded_size_bytes)
            .sum::<usize>();
        let timeout = Duration::max(
            // either our minimum timeout
            cfg.dynamic.compaction_minimum_timeout(),
            // or 1s per MB of input data
            Duration::from_secs(u64::cast_from(total_input_bytes / MB)),
        );

        trace!(
            "compaction request for {}MBs ({} bytes), with timeout of {}s.",
            total_input_bytes / MB,
            total_input_bytes,
            timeout.as_secs_f64()
        );

        let compact_span = debug_span!("compact::consolidate");
        let res = tokio::time::timeout(
            timeout,
            // Compaction is cpu intensive, so be polite and spawn it on the CPU heavy runtime.
            cpu_heavy_runtime
                .spawn_named(
                    || "persist::compact::consolidate",
                    Self::compact(
                        CompactConfig::from(&cfg),
                        Arc::clone(&blob),
                        Arc::clone(&metrics),
                        Arc::clone(&cpu_heavy_runtime),
                        req,
                        writer_id,
                        schemas.clone(),
                    )
                    .instrument(compact_span),
                )
                .map_err(|e| anyhow!(e)),
        )
        .await;

        let res = match res {
            Ok(res) => res,
            Err(err) => {
                metrics.compaction.timed_out.inc();
                Err(anyhow!(err))
            }
        };

        metrics
            .compaction
            .seconds
            .inc_by(start.elapsed().as_secs_f64());

        match res {
            Ok(Ok(res)) => {
                let res = FueledMergeRes { output: res.output };
                let (apply_merge_result, maintenance) = machine.merge_res(&res).await;
                maintenance.start_performing(machine, gc);
                match &apply_merge_result {
                    ApplyMergeResult::AppliedExact => {
                        metrics.compaction.applied.inc();
                        metrics.compaction.applied_exact_match.inc();
                        machine.applier.shard_metrics.compaction_applied.inc();
                        Ok(apply_merge_result)
                    }
                    ApplyMergeResult::AppliedSubset => {
                        metrics.compaction.applied.inc();
                        metrics.compaction.applied_subset_match.inc();
                        machine.applier.shard_metrics.compaction_applied.inc();
                        Ok(apply_merge_result)
                    }
                    ApplyMergeResult::NotAppliedNoMatch
                    | ApplyMergeResult::NotAppliedInvalidSince
                    | ApplyMergeResult::NotAppliedTooManyUpdates => {
                        if let ApplyMergeResult::NotAppliedTooManyUpdates = &apply_merge_result {
                            metrics.compaction.not_applied_too_many_updates.inc();
                        }
                        metrics.compaction.noop.inc();
                        for part in res.output.parts {
                            let key = part.key.complete(&machine.shard_id());
                            retry_external(
                                &metrics.retries.external.compaction_noop_delete,
                                || blob.delete(&key),
                            )
                            .await;
                        }
                        Ok(apply_merge_result)
                    }
                }
            }
            Ok(Err(err)) | Err(err) => {
                metrics.compaction.failed.inc();
                debug!("compaction for {} failed: {:#}", machine.shard_id(), err);
                Err(err)
            }
        }
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
    pub async fn compact(
        cfg: CompactConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        req: CompactReq<T>,
        writer_id: WriterId,
        schemas: Schemas<K, V>,
    ) -> Result<CompactRes<T>, anyhow::Error> {
        let () = Self::validate_req(&req)?;

        // We introduced a fast-path optimization in https://github.com/MaterializeInc/materialize/pull/15363
        // but had to revert it due to a very scary bug. Here we count how many of our compaction reqs
        // could be eligible for the optimization to better understand whether it's worth trying to
        // reintroduce it.
        let mut single_nonempty_batch = None;
        for batch in &req.inputs {
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
            if single_nonempty_batch.runs.len() == 0
                && single_nonempty_batch.desc.since() != &Antichain::from_elem(T::minimum())
            {
                metrics.compaction.fast_path_eligible.inc();
            }
        }

        // compaction needs memory enough for at least 2 runs and 2 in-progress parts
        assert!(cfg.compaction_memory_bound_bytes >= 4 * cfg.batch.blob_target_size);
        // reserve space for the in-progress part to be held in-mem representation and columnar
        let in_progress_part_reserved_memory_bytes = 2 * cfg.batch.blob_target_size;
        // then remaining memory will go towards pulling down as many runs as we can
        let run_reserved_memory_bytes =
            cfg.compaction_memory_bound_bytes - in_progress_part_reserved_memory_bytes;

        let mut all_parts = vec![];
        let mut all_runs = vec![];
        let mut len = 0;

        for (runs, run_chunk_max_memory_usage) in
            Self::chunk_runs(&req, &cfg, metrics.as_ref(), run_reserved_memory_bytes)
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
            let batch = Self::compact_runs(
                &run_cfg,
                &req.shard_id,
                &req.desc,
                runs,
                Arc::clone(&blob),
                Arc::clone(&metrics),
                Arc::clone(&cpu_heavy_runtime),
                writer_id.clone(),
                schemas.clone(),
            )
            .await?;
            let (parts, runs, updates) = (batch.parts, batch.runs, batch.len);
            assert!(
                (updates == 0 && parts.len() == 0) || (updates > 0 && parts.len() > 0),
                "updates={}, parts={}",
                updates,
                parts.len(),
            );

            if updates == 0 {
                continue;
            }
            // merge together parts and runs from each compaction round.
            // parts are appended onto our existing vec, and then we shift
            // the latest run offsets to account for prior parts.
            //
            // e.g. if we currently have 3 parts and 2 runs (including the implicit one from 0):
            //         parts: [k0, k1, k2]
            //         runs:  [    1     ]
            //
            // and we merge in another result with 2 parts and 2 runs:
            //         parts: [k3, k4]
            //         runs:  [    1]
            //
            // we our result will contain 5 parts and 4 runs:
            //         parts: [k0, k1, k2, k3, k4]
            //         runs:  [    1       3   4 ]
            let run_offset = all_parts.len();
            if all_parts.len() > 0 {
                all_runs.push(run_offset);
            }
            all_runs.extend(runs.iter().map(|run_start| run_start + run_offset));
            all_parts.extend(parts);
            len += updates;
        }

        Ok(CompactRes {
            output: HollowBatch {
                desc: req.desc.clone(),
                parts: all_parts,
                runs: all_runs,
                len,
            },
        })
    }

    /// Sorts and groups all runs from the inputs into chunks, each of which has been determined
    /// to consume no more than `run_reserved_memory_bytes` at a time, unless the input parts
    /// were written with a different target size than this build. Uses [Self::order_runs] to
    /// determine the order in which runs are selected.
    fn chunk_runs<'a>(
        req: &'a CompactReq<T>,
        cfg: &CompactConfig,
        metrics: &Metrics,
        run_reserved_memory_bytes: usize,
    ) -> Vec<(Vec<(&'a Description<T>, &'a [HollowBatchPart])>, usize)> {
        let ordered_runs = Self::order_runs(req);
        let mut ordered_runs = ordered_runs.iter().peekable();

        let mut chunks = vec![];
        let mut current_chunk = vec![];
        let mut current_chunk_max_memory_usage = 0;
        while let Some(run) = ordered_runs.next() {
            let run_greatest_part_size = run
                .1
                .iter()
                .map(|x| x.encoded_size_bytes)
                .max()
                .unwrap_or(cfg.batch.blob_target_size);
            current_chunk.push(*run);
            current_chunk_max_memory_usage += run_greatest_part_size;

            if let Some(next_run) = ordered_runs.peek() {
                let next_run_greatest_part_size = next_run
                    .1
                    .iter()
                    .map(|x| x.encoded_size_bytes)
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

        chunks
    }

    /// With bounded memory where we cannot compact all runs/parts together, the groupings
    /// in which we select runs to compact together will affect how much we're able to
    /// consolidate updates.
    ///
    /// This approach orders the input runs by cycling through each batch, selecting the
    /// head element until all are consumed. It assumes that it is generally more effective
    /// to prioritize compacting runs from different batches, rather than runs from within
    /// a single batch.
    ///
    /// ex.
    /// ```text
    ///        inputs                                        output
    ///     b0 runs=[A, B]
    ///     b1 runs=[C]                           output=[A, C, D, B, E, F]
    ///     b2 runs=[D, E, F]
    /// ```
    fn order_runs(req: &CompactReq<T>) -> Vec<(&Description<T>, &[HollowBatchPart])> {
        let total_number_of_runs = req.inputs.iter().map(|x| x.runs.len() + 1).sum::<usize>();

        let mut batch_runs: VecDeque<_> = req
            .inputs
            .iter()
            .map(|batch| (&batch.desc, batch.runs()))
            .collect();

        let mut ordered_runs = Vec::with_capacity(total_number_of_runs);

        while let Some((desc, mut runs)) = batch_runs.pop_front() {
            if let Some(run) = runs.next() {
                ordered_runs.push((desc, run));
                batch_runs.push_back((desc, runs));
            }
        }

        ordered_runs
    }

    /// Compacts runs together. If the input runs are sorted, a single run will be created as output
    ///
    /// Maximum possible memory usage is `(# runs + 2) * [crate::PersistConfig::blob_target_size]`
    async fn compact_runs<'a>(
        // note: 'a cannot be elided due to https://github.com/rust-lang/rust/issues/63033
        cfg: &'a CompactConfig,
        shard_id: &'a ShardId,
        desc: &'a Description<T>,
        runs: Vec<(&'a Description<T>, &'a [HollowBatchPart])>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        writer_id: WriterId,
        real_schemas: Schemas<K, V>,
    ) -> Result<HollowBatch<T>, anyhow::Error> {
        // TODO: Figure out a more principled way to allocate our memory budget.
        // Currently, we give any excess budget to write parallelism. If we had
        // to pick between 100% towards writes vs 100% towards reads, then reads
        // is almost certainly better, but the ideal is probably somewhere in
        // between the two.
        //
        // For now, invent some some extra budget out of thin air for prefetch.
        let prefetch_budget_bytes = 2 * cfg.batch.blob_target_size;

        let mut sorted_updates = BinaryHeap::new();

        let mut remaining_updates_by_run = vec![0; runs.len()];
        let mut runs: Vec<_> = runs
            .into_iter()
            .map(|(part_desc, parts)| {
                (
                    part_desc,
                    parts
                        .into_iter()
                        .map(|x| CompactionPart::Queued(x))
                        .collect::<VecDeque<_>>(),
                )
            })
            .collect();

        let mut timings = Timings::default();

        // Old style compaction operates on the encoded bytes and doesn't need
        // the real schema, so we synthesize one. We use the real schema for
        // stats though (see below).
        let fake_compaction_schema = Schemas {
            key: Arc::new(VecU8Schema),
            val: Arc::new(VecU8Schema),
        };

        let mut batch = BatchBuilderInternal::<Vec<u8>, Vec<u8>, T, D>::new(
            cfg.batch.clone(),
            Arc::clone(&metrics),
            fake_compaction_schema,
            metrics.compaction.batch.clone(),
            desc.lower().clone(),
            Arc::clone(&blob),
            cpu_heavy_runtime,
            shard_id.clone(),
            writer_id,
            desc.since().clone(),
            Some(desc.upper().clone()),
            true,
        );

        start_prefetches(prefetch_budget_bytes, &mut runs, shard_id, &blob, &metrics);

        let all_prefetched = runs
            .iter()
            .all(|(_, x)| x.iter().all(|x| x.is_prefetched()));
        if !all_prefetched {
            metrics.compaction.not_all_prefetched.inc();
        }

        // populate our heap with the updates from the first part of each run
        for (index, (part_desc, parts)) in runs.iter_mut().enumerate() {
            if let Some(part) = parts.pop_front() {
                let start = Instant::now();
                let mut part = part
                    .join(shard_id, blob.as_ref(), &metrics, part_desc)
                    .await?;
                // Ideally we'd hook into start_prefetches here, too, but runs
                // is mutable borrowed. Not the end of the world. Instead do it
                // once after this initial heap population.
                timings.part_fetching += start.elapsed();
                let start = Instant::now();
                while let Some((k, v, mut t, d)) = part.next() {
                    t.advance_by(desc.since().borrow());
                    let d = D::decode(d);
                    let k = k.to_vec();
                    let v = v.to_vec();
                    // default heap ordering is descending
                    sorted_updates.push(Reverse((((k, v), t, d), index)));
                    remaining_updates_by_run[index] += 1;
                }
                timings.heap_population += start.elapsed();
            }
        }

        start_prefetches(prefetch_budget_bytes, &mut runs, shard_id, &blob, &metrics);

        // repeatedly pull off the least element from our heap, refilling from the originating run
        // if needed. the heap will be exhausted only when all parts from all input runs have been
        // consumed.
        while let Some(Reverse((((k, v), t, d), index))) = sorted_updates.pop() {
            remaining_updates_by_run[index] -= 1;
            if remaining_updates_by_run[index] == 0 {
                // repopulate from the originating run, if any parts remain
                let (part_desc, parts) = &mut runs[index];
                if let Some(part) = parts.pop_front() {
                    let start = Instant::now();
                    let mut part = part
                        .join(shard_id, blob.as_ref(), &metrics, part_desc)
                        .await?;
                    // start_prefetches is O(n) so calling it here is O(n^2). N
                    // is the number of things we're about to fetch over the
                    // network, so if it's big enough for N^2 to matter, we've
                    // got bigger problems. It might be possible to do make this
                    // overall linear, but the bookkeeping would be pretty
                    // subtle.
                    start_prefetches(prefetch_budget_bytes, &mut runs, shard_id, &blob, &metrics);
                    timings.part_fetching += start.elapsed();
                    let start = Instant::now();
                    while let Some((k, v, mut t, d)) = part.next() {
                        t.advance_by(desc.since().borrow());
                        let d = D::decode(d);
                        let k = k.to_vec();
                        let v = v.to_vec();
                        // default heap ordering is descending
                        sorted_updates.push(Reverse((((k, v), t, d), index)));
                        remaining_updates_by_run[index] += 1;
                    }
                    timings.heap_population += start.elapsed();
                }
            }

            batch.add(&real_schemas, &k, &v, &t, &d).await?;
        }

        let batch = batch.finish(&real_schemas, desc.upper().clone()).await?;
        let hollow_batch = batch.into_hollow_batch();

        timings.record(&metrics);

        Ok(hollow_batch)
    }

    fn validate_req(req: &CompactReq<T>) -> Result<(), anyhow::Error> {
        let mut frontier = req.desc.lower();
        for input in req.inputs.iter() {
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

#[derive(Debug)]
enum CompactionPart<'a, T> {
    Queued(&'a HollowBatchPart),
    Prefetched(usize, JoinHandle<Result<EncodedPart<T>, anyhow::Error>>),
}

impl<'a, T: Timestamp + Lattice + Codec64> CompactionPart<'a, T> {
    fn is_prefetched(&self) -> bool {
        match self {
            CompactionPart::Queued(_) => false,
            CompactionPart::Prefetched(_, _) => true,
        }
    }

    async fn join(
        self,
        shard_id: &ShardId,
        blob: &(dyn Blob + Send + Sync),
        metrics: &Metrics,
        part_desc: &Description<T>,
    ) -> Result<EncodedPart<T>, anyhow::Error> {
        match self {
            CompactionPart::Prefetched(_, task) => {
                if task.is_finished() {
                    metrics.compaction.parts_prefetched.inc();
                } else {
                    metrics.compaction.parts_waited.inc();
                }
                task.await.map_err(anyhow::Error::new)?
            }
            CompactionPart::Queued(part) => {
                metrics.compaction.parts_waited.inc();
                fetch_batch_part(
                    shard_id,
                    blob,
                    metrics,
                    &metrics.read.compaction,
                    &part.key,
                    part_desc,
                )
                .await
            }
        }
    }
}

fn start_prefetches<T: Timestamp + Lattice + Codec64>(
    mut prefetch_budget_bytes: usize,
    runs: &mut Vec<(&Description<T>, VecDeque<CompactionPart<'_, T>>)>,
    shard_id: &ShardId,
    blob: &Arc<dyn Blob + Send + Sync>,
    metrics: &Arc<Metrics>,
) {
    // First account for how much budget has already been used
    for (_, run) in runs.iter() {
        for part in run.iter() {
            if let CompactionPart::Prefetched(cost_bytes, _) = part {
                prefetch_budget_bytes = prefetch_budget_bytes.saturating_sub(*cost_bytes);
            }
        }
    }

    // Then iterate through parts in a certain order (attempting to match the
    // order in which they'll be fetched), prefetching until we run out of
    // budget.
    //
    // The order used here is the first part of each run, then the second, etc.
    // There's a bunch of heuristics we could use here, but we'd get it exactly
    // correct if we stored on HollowBatchPart the actual kv bounds of data
    // contained in each part and go in sorted order of that. This information
    // would also be useful for pushing MFP down into persist reads, so it seems
    // like we might want to do it at some point. As a result, don't think too
    // hard about this heuristic at first.
    let max_run_len = runs.iter().map(|(_, x)| x.len()).max().unwrap_or_default();
    for idx in 0..max_run_len {
        for (part_desc, run) in runs.iter_mut() {
            let c_part = match run.get_mut(idx) {
                Some(x) => x,
                None => continue,
            };
            let part = match c_part {
                CompactionPart::Queued(x) => x,
                CompactionPart::Prefetched(_, _) => continue,
            };
            let cost_bytes = part.encoded_size_bytes;
            if prefetch_budget_bytes < cost_bytes {
                return;
            }
            prefetch_budget_bytes -= cost_bytes;
            let span = debug_span!("compaction::prefetch");
            let shard_id = *shard_id;
            let blob = Arc::clone(blob);
            let metrics = Arc::clone(metrics);
            let part_key = part.key.clone();
            let part_desc = part_desc.clone();
            let handle = spawn(
                || "persist::compaction::prefetch",
                async move {
                    fetch_batch_part(
                        &shard_id,
                        blob.as_ref(),
                        &metrics,
                        &metrics.read.compaction,
                        &part_key,
                        &part_desc,
                    )
                    .await
                }
                .instrument(span),
            );
            *c_part = CompactionPart::Prefetched(cost_bytes, handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::paths::PartialBatchKey;
    use crate::PersistLocation;
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use timely::progress::Antichain;

    use crate::tests::{all_ok, expect_fetch_part, new_test_client, new_test_client_cache};

    use super::*;

    // A regression test for a bug caught during development of #13160 (never
    // made it to main) where batches written by compaction would always have a
    // since of the minimum timestamp.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
    async fn regression_minimum_since() {
        mz_ore::test::init_logging();

        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("0".to_owned(), "zero".to_owned()), 1, -1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
        ];

        let cache = new_test_client_cache();
        cache.cfg.dynamic.set_blob_target_size(100);
        let (mut write, _) = cache
            .open(PersistLocation {
                blob_uri: "mem://".to_owned(),
                consensus_uri: "mem://".to_owned(),
            })
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
            inputs: vec![b0, b1],
        };
        let schemas = Schemas {
            key: Arc::new(StringSchema),
            val: Arc::new(UnitSchema),
        };
        let res = Compactor::<String, (), u64, i64>::compact(
            CompactConfig::from(&write.cfg),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            Arc::new(CpuHeavyRuntime::new()),
            req.clone(),
            write.writer_id.clone(),
            schemas,
        )
        .await
        .expect("compaction failed");

        assert_eq!(res.output.desc, req.desc);
        assert_eq!(res.output.len, 1);
        assert_eq!(res.output.parts.len(), 1);
        let part = &res.output.parts[0];
        let (part, updates) = expect_fetch_part(
            write.blob.as_ref(),
            &part.key.complete(&write.machine.shard_id()),
        )
        .await;
        assert_eq!(part.desc, res.output.desc);
        assert_eq!(updates, all_ok(&data, 10));
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
    async fn prefetches() {
        let desc = Description::new(
            Antichain::from_elem(0u64),
            Antichain::new(),
            Antichain::new(),
        );
        let input_parts = (0..=9)
            .map(|encoded_size_bytes| HollowBatchPart {
                key: PartialBatchKey("".into()),
                encoded_size_bytes,
                stats: None,
            })
            .collect::<Vec<_>>();
        let parse = |x: &str| {
            x.split('|')
                .map(|run| {
                    let parts = run
                        .split(',')
                        .map(|x| {
                            let encoded_size_bytes = x.get(1..).unwrap().parse::<usize>().unwrap();
                            match x.get(..1).unwrap() {
                                " " => CompactionPart::Queued(&input_parts[encoded_size_bytes]),
                                "f" => CompactionPart::Prefetched(
                                    encoded_size_bytes,
                                    spawn(|| "", async {
                                        tokio::time::sleep(Duration::from_secs(1_000_000)).await;
                                        unreachable!("boom")
                                    }),
                                ),
                                x => panic!("unknown {}", x),
                            }
                        })
                        .collect::<VecDeque<_>>();
                    (&desc, parts)
                })
                .collect::<Vec<_>>()
        };
        let print = |x: &Vec<(&Description<u64>, VecDeque<CompactionPart<'_, u64>>)>| {
            x.iter()
                .map(|(_, x)| {
                    x.iter()
                        .map(|x| match x {
                            CompactionPart::Queued(x) => format!(" {}", x.encoded_size_bytes),
                            CompactionPart::Prefetched(x, _) => format!("f{}", x),
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect::<Vec<_>>()
                .join("|")
        };

        let client = new_test_client().await;

        let shard_id = ShardId::new();
        let blob = &client.blob;
        let metrics = &client.metrics;

        // NB: In the below, parts within a run are separated by `,` and runs
        // are separated by `|`. Example: `r0p0,r0p1|r1p0|r2p0,r2p1,r2p2`

        // Enough budget for none, some, and all parts of a single run
        let mut runs = parse(" 1, 1, 1");
        start_prefetches(0, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), " 1, 1, 1");

        let mut runs = parse(" 1, 1, 1");
        start_prefetches(1, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), "f1, 1, 1");

        let mut runs = parse(" 1, 1, 1");
        start_prefetches(3, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), "f1,f1,f1");

        // Budget partially covers some part (which is then not prefetched)
        let mut runs = parse(" 1| 2| 2");
        start_prefetches(4, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), "f1|f2| 2");

        // Runs of length > 1
        let mut runs = parse(" 1, 1, 1, 1| 1| 1, 1, 1");
        start_prefetches(5, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), "f1,f1, 1, 1|f1|f1,f1, 1");

        // Some budget is already used from a previous call
        let mut runs = parse(" 1| 1|f1,f1");
        start_prefetches(3, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), "f1| 1|f1,f1");

        // Sanity check budget has gone down (no panics)
        let mut runs = parse(" 1| 1|f9");
        start_prefetches(1, &mut runs, &shard_id, blob, metrics);
        assert_eq!(print(&runs), " 1| 1|f9");
    }
}
