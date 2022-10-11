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
use std::iter::Peekable;
use std::marker::PhantomData;
use std::slice::Iter;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist::indexed::columnar::{
    ColumnarRecordsBuilder, ColumnarRecordsVecBuilder, KEY_VAL_DATA_MAX_LEN,
};
use mz_persist::location::Blob;
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use timely::PartialOrder;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, debug_span, info, Instrument, Span};

use crate::async_runtime::CpuHeavyRuntime;
use crate::batch::BatchParts;
use crate::fetch::fetch_batch_part;
use crate::internal::machine::{retry_external, Machine};
use crate::internal::state::{HollowBatch, HollowBatchPart};
use crate::internal::trace::FueledMergeRes;
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

/// A service for performing physical and logical compaction.
///
/// This will possibly be called over RPC in the future. Physical compaction is
/// merging adjacent batches. Logical compaction is advancing timestamps to a
/// new since and consolidating the resulting updates.
#[derive(Debug, Clone)]
pub struct Compactor<T, D> {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    sender: UnboundedSender<(CompactReq<T>, oneshot::Sender<()>)>,
    _phantom: PhantomData<fn() -> D>,
}

impl<T, D> Compactor<T, D>
where
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send,
{
    pub fn new<K, V>(
        machine: Machine<K, V, T, D>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        writer_id: WriterId,
    ) -> Self
    where
        K: Debug + Codec,
        V: Debug + Codec,
    {
        let (compact_req_sender, mut compact_req_receiver) =
            mpsc::unbounded_channel::<(CompactReq<T>, oneshot::Sender<()>)>();
        let metrics = Arc::clone(&machine.metrics);
        let cfg = machine.cfg.clone();

        // spin off a single task responsible for executing compaction requests.
        // work is enqueued into the task through a channel
        let _worker_handle = mz_ore::task::spawn(|| "PersistCompactionWorker", async move {
            while let Some((req, completer)) = compact_req_receiver.recv().await {
                assert_eq!(req.shard_id, machine.shard_id());

                let cfg = machine.cfg.clone();
                let blob = Arc::clone(&machine.state_versions.blob);
                let metrics = Arc::clone(&machine.metrics);
                let cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
                let mut machine = machine.clone();
                let writer_id = writer_id.clone();

                let compact_span =
                    debug_span!(parent: None, "compact::apply", shard_id=%machine.shard_id());
                compact_span.follows_from(&Span::current());
                async move {
                    metrics.compaction.started.inc();
                    let start = Instant::now();

                    // Compaction is cpu intensive, so be polite and spawn it on the CPU heavy runtime.
                    let compact_span = debug_span!("compact::consolidate");
                    let res = cpu_heavy_runtime
                        .spawn_named(
                            || "persist::compact::consolidate",
                            Self::compact(
                                cfg.clone(),
                                Arc::clone(&blob),
                                Arc::clone(&metrics),
                                Arc::clone(&cpu_heavy_runtime),
                                req,
                                writer_id,
                            )
                            .instrument(compact_span),
                        )
                        .await
                        .map_err(|err| anyhow!(err));

                    metrics
                        .compaction
                        .seconds
                        .inc_by(start.elapsed().as_secs_f64());

                    match res {
                        Ok(Ok(res)) => {
                            let res = FueledMergeRes { output: res.output };
                            let applied = machine.merge_res(&res).await;
                            if applied {
                                metrics.compaction.applied.inc();
                            } else {
                                metrics.compaction.noop.inc();
                                for part in res.output.parts {
                                    let key = part.key.complete(&machine.shard_id());
                                    retry_external(
                                        &metrics.retries.external.compaction_noop_delete,
                                        || blob.delete(&key),
                                    )
                                    .await;
                                }
                            }
                        }
                        Ok(Err(err)) | Err(err) => {
                            metrics.compaction.failed.inc();
                            debug!("compaction for {} failed: {:#}", machine.shard_id(), err);
                        }
                    };
                }
                .instrument(compact_span)
                .await;

                // we can safely ignore errors here, it's possible the caller
                // wasn't interested in waiting and dropped their receiver
                let _ = completer.send(());
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
    ) -> Option<oneshot::Receiver<()>> {
        // Run some initial heuristics to ignore some requests for compaction.
        // We don't gain much from e.g. compacting two very small batches that
        // were just written, but it does result in non-trivial blob traffic
        // (especially in aggregate). This heuristic is something we'll need to
        // tune over time.
        let should_compact = req.inputs.len() >= self.cfg.compaction_heuristic_min_inputs
            || req.inputs.iter().map(|x| x.len).sum::<usize>()
                >= self.cfg.compaction_heuristic_min_updates;
        if !should_compact {
            self.metrics.compaction.skipped.inc();
            return None;
        }

        let (compaction_completed_sender, compaction_completed_receiver) = oneshot::channel();
        let new_compaction_sender = self.sender.clone();

        self.metrics.compaction.requested.inc();
        let send = new_compaction_sender.send((req, compaction_completed_sender));
        if let Err(e) = send {
            // In the steady state we expect this to always succeed, but during
            // shutdown it is possible the destination task has already spun down
            info!("compact_and_apply_background failed to send request: {}", e);
            return None;
        }

        Some(compaction_completed_receiver)
    }

    /// Compacts input batches in bounded memory.
    ///
    /// The memory bound is broken into pieces:
    ///     1. in-progress work
    ///     2. fetching parts from runs
    ///     3. additional in-flight requests to Blob
    ///
    /// 1. In-progress work is bounded by 2 * [crate::PersistConfig::blob_target_size]. This
    ///    usage is met at two mutually exclusive moments:
    ///   * When reading in a part, we hold the columnar format in memory while writing its
    ///     contents into a heap.
    ///   * When writing a part, we hold a temporary updates buffer while encoding/writing
    ///     it into a columnar format for Blob.
    ///
    /// 2. When compacting runs, only 1 part from each one is held in memory at a time.
    ///    Compaction will determine an appropriate number of runs to compact together
    ///    given the memory bound and accounting for the reservation in (1). A minimum
    ///    of 2 * [crate::PersistConfig::blob_target_size] of memory is expected, to be
    ///    able to at least have the capacity to compact two runs together at a time,
    ///    and more runs will be compacted together if more memory is available.
    ///
    /// 3. If there is excess memory after accounting for (1) and (2), we increase the
    ///    number of outstanding parts we can keep in-flight to Blob.
    pub async fn compact(
        cfg: PersistConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        req: CompactReq<T>,
        writer_id: WriterId,
    ) -> Result<CompactRes<T>, anyhow::Error> {
        let () = Self::validate_req(&req)?;
        // compaction needs memory enough for at least 2 runs and 2 in-progress parts
        assert!(cfg.compaction_memory_bound_bytes >= 4 * cfg.blob_target_size);
        // reserve space for the in-progress part to be held in-mem representation and columnar
        let in_progress_part_reserved_memory_bytes = 2 * cfg.blob_target_size;
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
                / cfg.blob_target_size;

            let batch_parts = BatchParts::new(
                1 + extra_outstanding_parts,
                Arc::clone(&metrics),
                req.shard_id,
                writer_id.clone(),
                req.desc.lower().clone(),
                Arc::clone(&blob),
                Arc::clone(&cpu_heavy_runtime),
                &metrics.compaction.batch,
            );

            let (parts, runs, updates) = Self::compact_runs(
                &cfg,
                &req.shard_id,
                &req.desc,
                runs,
                batch_parts,
                Arc::clone(&blob),
                Arc::clone(&metrics),
            )
            .await?;
            assert!((updates == 0 && parts.len() == 0) || (updates > 0 && parts.len() > 0));

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
        cfg: &PersistConfig,
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
                .unwrap_or(cfg.blob_target_size);
            current_chunk.push(*run);
            current_chunk_max_memory_usage += run_greatest_part_size;

            if let Some(next_run) = ordered_runs.peek() {
                let next_run_greatest_part_size = next_run
                    .1
                    .iter()
                    .map(|x| x.encoded_size_bytes)
                    .max()
                    .unwrap_or(cfg.blob_target_size);

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

        let mut batch_runs = Vec::with_capacity(req.inputs.len());
        for batch in &req.inputs {
            batch_runs.push((&batch.desc, batch.runs()));
        }

        let mut ordered_runs = Vec::with_capacity(total_number_of_runs);
        while batch_runs.len() > 0 {
            for (desc, runs) in batch_runs.iter_mut() {
                if let Some(run) = runs.next() {
                    ordered_runs.push((*desc, run));
                }
            }
            batch_runs.retain_mut(|(_, iter)| iter.inner.peek().is_some());
        }

        ordered_runs
    }

    /// Compacts runs together. If the input runs are sorted, a single run will be created as output
    ///
    /// Maximum possible memory usage is `(# runs + 2) * [crate::PersistConfig::blob_target_size]`
    async fn compact_runs<'a>(
        // note: 'a cannot be elided due to https://github.com/rust-lang/rust/issues/63033
        cfg: &'a PersistConfig,
        shard_id: &'a ShardId,
        desc: &'a Description<T>,
        runs: Vec<(&'a Description<T>, &'a [HollowBatchPart])>,
        mut batch_parts: BatchParts<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Result<(Vec<HollowBatchPart>, Vec<usize>, usize), anyhow::Error> {
        let mut compaction_runs = vec![];
        let mut compaction_parts_count = 0;
        let mut total_updates = 0;

        let mut sorted_updates = BinaryHeap::new();
        let mut update_buffer: Vec<((Vec<u8>, Vec<u8>), T, D)> = Vec::new();
        let mut update_buffer_size_bytes = 0;
        let mut greatest_kv: Option<(Vec<u8>, Vec<u8>)> = None;

        let mut remaining_updates_by_run = vec![0; runs.len()];
        let mut runs: Vec<_> = runs
            .iter()
            .map(|(part_desc, parts)| (part_desc, parts.into_iter()))
            .collect();

        let mut timings = Timings::default();

        // populate our heap with the updates from the first part of each run.
        // we have the memory to fetch each part in parallel, so we spawn a
        // task to fetch each one.
        let mut pending_initial_parts = VecDeque::with_capacity(usize::min(
            runs.len(),
            cfg.compaction_reads_max_outstanding_parts,
        ));
        let mut fetched_initial_parts = Vec::with_capacity(runs.len());
        let start = Instant::now();
        for (runs_index, (part_desc, parts)) in runs.iter_mut().enumerate() {
            if let Some(part) = parts.next() {
                let shard_id = shard_id.clone();
                let blob = Arc::clone(&blob);
                let metrics = Arc::clone(&metrics);
                let compaction_metrics = metrics.read.compaction.clone();
                let key = part.key.clone();
                let part_desc = (**part_desc).clone();

                pending_initial_parts.push_back((
                    runs_index,
                    mz_ore::task::spawn(|| "compaction::initial_fetch_batch_part", async move {
                        fetch_batch_part(
                            &shard_id,
                            blob.as_ref(),
                            &metrics,
                            &compaction_metrics,
                            &key,
                            &part_desc,
                        )
                        .await
                    }),
                ));

                if pending_initial_parts.len() > cfg.compaction_reads_max_outstanding_parts {
                    let (index, part) = pending_initial_parts
                        .pop_front()
                        .expect("pop failed when len was just > some usize");
                    fetched_initial_parts.push((index, part.await??));
                }
            }
        }

        for (runs_index, part) in pending_initial_parts {
            fetched_initial_parts.push((runs_index, part.await??));
        }
        timings.part_fetching += start.elapsed();
        assert_eq!(fetched_initial_parts.len(), runs.len());

        // serially add each fetched part into our heap. this will momentarily 2x
        // the memory required to store a given part, so this is done one at a time.
        let start = Instant::now();
        for (index, (runs_index, mut part)) in fetched_initial_parts.into_iter().enumerate() {
            // it's crucial that `fetched_initial_parts` maintains the same order as `runs`,
            // as we use the index into runs for a fair bit of bookkeeping
            assert_eq!(index, runs_index);
            while let Some((k, v, mut t, d)) = part.next() {
                t.advance_by(desc.since().borrow());
                let d = D::decode(d);
                let k = k.to_vec();
                let v = v.to_vec();
                // default heap ordering is descending
                sorted_updates.push(Reverse((((k, v), t, d), index)));
                remaining_updates_by_run[index] += 1;
            }
        }
        timings.heap_population += start.elapsed();

        // repeatedly pull off the least element from our heap, refilling from the originating run
        // if needed. the heap will be exhausted only when all parts from all input runs have been
        // consumed.
        while let Some(Reverse((((k, v), t, d), index))) = sorted_updates.pop() {
            remaining_updates_by_run[index] -= 1;
            if remaining_updates_by_run[index] == 0 {
                // repopulate from the originating run, if any parts remain
                let (part_desc, parts) = &mut runs[index];
                if let Some(part) = parts.next() {
                    let start = Instant::now();
                    let mut part = fetch_batch_part(
                        shard_id,
                        blob.as_ref(),
                        &metrics,
                        &metrics.read.compaction,
                        &part.key,
                        part_desc,
                    )
                    .await?;
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

            let update_size_bytes = ColumnarRecordsBuilder::columnar_record_size(&k, &v);

            // flush the buffer if adding this latest update would cause it to exceed our target size
            if update_size_bytes + update_buffer_size_bytes > cfg.blob_target_size {
                total_updates += Self::consolidate_run(
                    &mut update_buffer,
                    &mut compaction_runs,
                    compaction_parts_count,
                    &mut greatest_kv,
                    &mut timings,
                );
                Self::write_run(
                    &mut batch_parts,
                    &mut update_buffer,
                    &mut compaction_parts_count,
                    desc.clone(),
                    &mut timings,
                )
                .await;
                update_buffer_size_bytes = 0;
            }

            update_buffer_size_bytes += update_size_bytes;
            update_buffer.push(((k, v), t, d));
        }

        if update_buffer.len() > 0 {
            total_updates += Self::consolidate_run(
                &mut update_buffer,
                &mut compaction_runs,
                compaction_parts_count,
                &mut greatest_kv,
                &mut timings,
            );
            Self::write_run(
                &mut batch_parts,
                &mut update_buffer,
                &mut compaction_parts_count,
                desc.clone(),
                &mut timings,
            )
            .await;
        }

        let start = Instant::now();
        let compaction_parts = batch_parts.finish().await;
        timings.part_writing += start.elapsed();
        assert_eq!(compaction_parts.len(), compaction_parts_count);

        timings.record(&metrics);

        Ok((compaction_parts, compaction_runs, total_updates))
    }

    /// Consolidates `updates`, and determines whether the updates should extend the
    /// current run (if any).  A new run will be created if `updates` contains a key
    /// that overlaps with the current or any previous run.
    fn consolidate_run(
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        compaction_runs: &mut Vec<usize>,
        number_of_compacted_runs: usize,
        greatest_kv: &mut Option<(Vec<u8>, Vec<u8>)>,
        timings: &mut Timings,
    ) -> usize {
        let start = Instant::now();
        consolidate_updates(updates);

        match (&greatest_kv, updates.last()) {
            // our updates contain a key that exists within the range of a run we've
            // already created, we should start a new run, as this part is no longer
            // contiguous with the previous run/part
            (Some(greatest_kv_seen), Some(greatest_kv_in_batch))
                if *greatest_kv_seen > greatest_kv_in_batch.0 =>
            {
                compaction_runs.push(number_of_compacted_runs);
            }
            (_, Some(greatest_kv_in_batch)) => *greatest_kv = Some(greatest_kv_in_batch.0.clone()),
            (Some(_), None) | (None, None) => {}
        };

        timings.consolidation += start.elapsed();
        updates.len()
    }

    /// Encodes `updates` into columnar format and writes them as a single part to blob. It is the
    /// caller's responsibility to chunk `updates` into a batch no greater than [crate::PersistConfig::blob_target_size]
    /// and must absolutely be less than [mz_persist::indexed::columnar::KEY_VAL_DATA_MAX_LEN]
    async fn write_run(
        batch_parts: &mut BatchParts<T>,
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        compaction_parts_count: &mut usize,
        desc: Description<T>,
        timings: &mut Timings,
    ) {
        if updates.is_empty() {
            return;
        }
        *compaction_parts_count += 1;

        let mut builder = ColumnarRecordsVecBuilder::new_with_len(KEY_VAL_DATA_MAX_LEN);
        let start = Instant::now();
        for ((k, v), t, d) in updates.drain(..) {
            builder.push(((&k, &v), T::encode(&t), D::encode(&d)));
        }
        let chunks = builder.finish();
        timings.part_columnar_encoding += start.elapsed();
        debug_assert_eq!(chunks.len(), 1);

        let start = Instant::now();
        for chunk in chunks {
            batch_parts
                .write(chunk, desc.upper().clone(), desc.since().clone())
                .await;
        }
        timings.part_writing += start.elapsed();
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
    consolidation: Duration,
    part_columnar_encoding: Duration,
    part_writing: Duration,
}

impl Timings {
    fn record(self, metrics: &Metrics) {
        // intentionally deconstruct so we don't forget to consider each field
        let Timings {
            part_fetching,
            heap_population,
            consolidation,
            part_columnar_encoding,
            part_writing,
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
        metrics
            .compaction
            .steps
            .consolidation_seconds
            .inc_by(consolidation.as_secs_f64());
        metrics
            .compaction
            .steps
            .part_columnar_encoding_seconds
            .inc_by(part_columnar_encoding.as_secs_f64());
        metrics
            .compaction
            .steps
            .part_write_seconds
            .inc_by(part_writing.as_secs_f64());
    }
}

impl<T> HollowBatch<T> {
    pub(crate) fn runs(&self) -> HollowBatchRunIter<T> {
        HollowBatchRunIter {
            batch: self,
            inner: self.runs.iter().peekable(),
            emitted_implicit: false,
        }
    }
}

pub(crate) struct HollowBatchRunIter<'a, T> {
    batch: &'a HollowBatch<T>,
    inner: Peekable<Iter<'a, usize>>,
    emitted_implicit: bool,
}

impl<'a, T> Iterator for HollowBatchRunIter<'a, T> {
    type Item = &'a [HollowBatchPart];

    fn next(&mut self) -> Option<Self::Item> {
        if self.batch.parts.is_empty() {
            return None;
        }

        if !self.emitted_implicit {
            self.emitted_implicit = true;
            return Some(match self.inner.peek() {
                None => &self.batch.parts,
                Some(run_end) => &self.batch.parts[0..**run_end],
            });
        }

        if let Some(run_start) = self.inner.next() {
            return Some(match self.inner.peek() {
                Some(run_end) => &self.batch.parts[*run_start..**run_end],
                None => &self.batch.parts[*run_start..],
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::PersistLocation;
    use timely::progress::Antichain;

    use crate::tests::{all_ok, expect_fetch_part, new_test_client_cache};

    use super::*;

    // A regression test for a bug caught during development of #13160 (never
    // made it to main) where batches written by compaction would always have a
    // since of the minimum timestamp.
    #[tokio::test]
    async fn regression_minimum_since() {
        mz_ore::test::init_logging();

        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("0".to_owned(), "zero".to_owned()), 1, -1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
        ];

        let mut cache = new_test_client_cache();
        cache.cfg.blob_target_size = 100;
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
        let res = Compactor::<u64, i64>::compact(
            write.cfg.clone(),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            Arc::new(CpuHeavyRuntime::new()),
            req.clone(),
            write.writer_id.clone(),
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
}
