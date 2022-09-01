// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::indexed::columnar::ColumnarRecordsVecBuilder;
use mz_persist::location::Blob;
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use timely::PartialOrder;
use tokio::task::JoinHandle;
use tracing::{debug_span, warn, Instrument, Span};

use crate::async_runtime::CpuHeavyRuntime;
use crate::batch::BatchParts;
use crate::fetch::fetch_batch_part;
use crate::internal::machine::{retry_external, Machine};
use crate::internal::paths::PartialBatchKey;
use crate::internal::state::HollowBatch;
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
pub struct Compactor {
    cfg: PersistConfig,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
    writer_id: WriterId,
}

impl Compactor {
    pub fn new(
        cfg: PersistConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        writer_id: WriterId,
    ) -> Self {
        Compactor {
            cfg,
            blob,
            metrics,
            cpu_heavy_runtime,
            writer_id,
        }
    }

    pub fn compact_and_apply_background<K, V, T, D>(
        &self,
        machine: &Machine<K, V, T, D>,
        req: CompactReq<T>,
        memory_budget_bytes: usize,
    ) -> Option<JoinHandle<()>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        assert_eq!(req.shard_id, machine.shard_id());

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

        let cfg = self.cfg.clone();
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        let cpu_heavy_runtime = Arc::clone(&self.cpu_heavy_runtime);
        let mut machine = machine.clone();
        let writer_id = self.writer_id.clone();

        // Spawn compaction in a background task, so the write that triggered it
        // isn't blocked on it.
        let compact_span =
            debug_span!(parent: None, "compact::apply", shard_id=%machine.shard_id());
        compact_span.follows_from(&Span::current());

        Some(mz_ore::task::spawn(
            || "persist::compact::apply",
            async move {
                metrics.compaction.started.inc();
                let start = Instant::now();

                let res = Compactor::compact::<T, D>(
                    cfg.clone(),
                    Arc::clone(&blob),
                    Arc::clone(&metrics),
                    Arc::clone(&cpu_heavy_runtime),
                    req,
                    memory_budget_bytes,
                    writer_id.clone(),
                )
                .await;

                metrics
                    .compaction
                    .seconds
                    .inc_by(start.elapsed().as_secs_f64());

                let res = match res {
                    Ok(res) => res,
                    Err(err) => {
                        metrics.compaction.failed.inc();
                        warn!("compaction for {} failed: {:#}", machine.shard_id(), err);
                        return;
                    }
                };
                let res = FueledMergeRes { output: res.output };
                let applied = machine.merge_res(&res).await;
                if applied {
                    metrics.compaction.applied.inc();
                } else {
                    metrics.compaction.noop.inc();
                    for part in res.output.parts {
                        let key = part.key.complete(&machine.shard_id());
                        retry_external(&metrics.retries.external.compaction_noop_delete, || {
                            blob.delete(&key)
                        })
                        .await;
                    }
                }
            }
            .instrument(compact_span),
        ))
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
    pub async fn compact<T, D>(
        cfg: PersistConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        req: CompactReq<T>,
        memory_bound_bytes: usize,
        writer_id: WriterId,
    ) -> Result<CompactRes<T>, anyhow::Error>
    where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let parts_cpu_heavy_runtime = Arc::clone(&cpu_heavy_runtime);
        let compact_blocking = async move {
            let () = Compactor::validate_req(&req)?;

            // compaction needs memory enough for at least 2 runs and 2 in-progress parts
            assert!(memory_bound_bytes >= 4 * cfg.blob_target_size);
            // reserve space for an in-progress part to be held in-mem representation and columnar
            let in_progress_part_reserved_memory_bytes = 2 * cfg.blob_target_size;
            // then remaining memory will go towards pulling down as many runs as we can
            let run_reserved_memory_bytes =
                memory_bound_bytes - in_progress_part_reserved_memory_bytes;

            let mut all_parts = vec![];
            let mut all_runs = vec![];
            let mut len = 0;

            let mut ordered_runs = Self::order_runs::<T, D>(&req);
            // for now: assume each part is always blob_target_size. in the future, we'll
            // have a smarter way of deciding how many runs we can pull down at once
            let run_chunk_size = run_reserved_memory_bytes / usize::max(cfg.blob_target_size, 1);
            for runs in ordered_runs.chunks_mut(run_chunk_size) {
                // given the runs we actually have in our chunk, we might have extra memory
                // available. we reserved enough space to always have 1 in-progress part in
                // flight, but if we have excess, we can use it to increase our write parallelism
                let actual_run_memory_requirement = runs.len() * cfg.blob_target_size;
                let extra_outstanding_parts = (run_reserved_memory_bytes
                    - actual_run_memory_requirement)
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

                let (parts, runs, updates) = Self::compact_runs::<T, D>(
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
                    keys: all_parts,
                    runs: all_runs,
                    len,
                },
            })
        };

        // Compaction is cpu intensive, so be polite and spawn it on the
        // CPU heavy runtime.
        let compact_span = debug_span!("compact::consolidate");
        parts_cpu_heavy_runtime
            .spawn_named(
                || "persist::compact::consolidate",
                compact_blocking.instrument(compact_span),
            )
            .await?
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
    /// ex.   inputs                                        output
    ///     b0 runs=[A, B]
    ///     b1 runs=[C]                           output=[A, C, D, B, E, F]
    ///     b2 runs=[D, E, F]
    fn order_runs<T, D>(req: &CompactReq<T>) -> Vec<(&HollowBatch<T>, &[PartialBatchKey])>
    where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let total_number_of_runs = req.inputs.iter().map(|x| x.runs.len() + 1).sum::<usize>();

        let mut batch_runs = Vec::with_capacity(req.inputs.len());
        for batch in &req.inputs {
            batch_runs.push((batch, batch.runs()));
        }

        let mut ordered_runs = Vec::with_capacity(total_number_of_runs);
        while batch_runs.len() > 0 {
            for (batch, runs) in batch_runs.iter_mut() {
                if let Some(run) = runs.next() {
                    ordered_runs.push((*batch, run));
                }
            }
            batch_runs.retain_mut(|(_, iter)| iter.inner.peek().is_some());
        }

        assert_eq!(total_number_of_runs, ordered_runs.len());
        ordered_runs
    }

    /// Compacts runs together. If the input runs are sorted, a single run will be created as output
    ///
    /// Maximum possible memory usage is `(# runs + 2) * [crate::PersistConfig::blob_target_size]`
    async fn compact_runs<'a, T, D>(
        cfg: &'a PersistConfig,
        shard_id: &'a ShardId,
        desc: &'a Description<T>,
        runs: &'a mut [(&'a HollowBatch<T>, &'a [PartialBatchKey])],
        mut batch_parts: BatchParts<T>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Result<(Vec<PartialBatchKey>, Vec<usize>, usize), anyhow::Error>
    where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut compaction_runs = vec![];
        let mut compaction_parts = vec![];
        let mut total_updates = 0;

        let mut heap = BinaryHeap::new();
        let mut update_buffer: Vec<((Vec<u8>, Vec<u8>), T, D)> = Vec::new();
        let mut update_buffer_size_bytes = 0;
        let mut greatest_kv: Option<(Vec<u8>, Vec<u8>)> = None;

        let mut remaining_updates_by_run = vec![0; runs.len()];
        let mut runs: Vec<_> = runs
            .iter()
            .map(|(batch, parts)| (batch, parts.into_iter()))
            .collect();

        // populate our heap with the updates from the first part of each run
        for (index, (batch, parts)) in runs.iter_mut().enumerate() {
            if let Some(key) = parts.next() {
                fetch_batch_part(
                    &shard_id,
                    blob.as_ref(),
                    &metrics,
                    key,
                    &batch.desc,
                    |k, v, mut t, d| {
                        t.advance_by(desc.since().borrow());
                        let d = D::decode(d);
                        let k = k.to_vec();
                        let v = v.to_vec();
                        // default heap ordering is descending
                        heap.push(Reverse((((k, v), t, d), index)));
                        remaining_updates_by_run[index] += 1;
                    },
                )
                .await?;
            }
        }

        // repeatedly pull off the least element from our heap, refilling from the originating run
        // if needed. the heap will be exhausted only when all parts from all input runs have been
        // consumed.
        while let Some(Reverse((((k, v), t, d), index))) = heap.pop() {
            remaining_updates_by_run[index] -= 1;
            if remaining_updates_by_run[index] == 0 {
                // repopulate from the originating run, if any parts remain
                let (batch, parts) = &mut runs[index];
                if let Some(key) = parts.next() {
                    fetch_batch_part(
                        &shard_id,
                        blob.as_ref(),
                        &metrics,
                        key,
                        &batch.desc,
                        |k, v, mut t, d| {
                            t.advance_by(desc.since().borrow());
                            let d = D::decode(d);
                            let k = k.to_vec();
                            let v = v.to_vec();
                            heap.push(Reverse((((k, v), t, d), index)));
                            remaining_updates_by_run[index] += 1;
                        },
                    )
                    .await?;
                }
            }

            // both T and D are Codec64, ergo 8 bytes a piece
            update_buffer_size_bytes += k.len() + v.len() + 16;
            update_buffer.push(((k, v), t, d));

            // if we're more than 90% into our target size, write out our part. this is an
            // attempt to avoid writing blobs that are just at the threshold of `blob_target_size`
            // where they could potentially spill over into two parts, a full one and a very
            // small overflow
            if update_buffer_size_bytes >= cfg.blob_target_size * 9 / 10 {
                Self::consolidate_run(
                    &mut update_buffer,
                    &mut compaction_runs,
                    &mut compaction_parts,
                    &mut greatest_kv,
                    &mut total_updates,
                );
                Self::write_run(
                    cfg,
                    &mut batch_parts,
                    &mut update_buffer,
                    &mut compaction_parts,
                    desc.clone(),
                )
                .await;
                update_buffer_size_bytes = 0;
            }
        }

        if update_buffer.len() > 0 {
            Self::consolidate_run(
                &mut update_buffer,
                &mut compaction_runs,
                &mut compaction_parts,
                &mut greatest_kv,
                &mut total_updates,
            );
            Self::write_run(
                cfg,
                &mut batch_parts,
                &mut update_buffer,
                &mut compaction_parts,
                desc.clone(),
            )
            .await;
        }

        Ok((compaction_parts, compaction_runs, total_updates))
    }

    /// Consolidates `updates`, and determines whether the updates should extend the
    /// current run (if any).  A new run will be created if `updates` contains a key
    /// that overlaps with the current or any previous run.
    fn consolidate_run<T, D>(
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        compaction_runs: &mut Vec<usize>,
        compaction_keys: &mut Vec<PartialBatchKey>,
        greatest_kv: &mut Option<(Vec<u8>, Vec<u8>)>,
        total_updates: &mut usize,
    ) where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        consolidate_updates(updates);
        *total_updates += updates.len();

        match (&greatest_kv, updates.last()) {
            // our updates contain a key that exists within the range of a run we've
            // already created, we should start a new run, as this part is no longer
            // contiguous with the previous run/part
            (Some(greatest_kv_seen), Some(greatest_kv_in_batch))
                if *greatest_kv_seen > greatest_kv_in_batch.0 =>
            {
                compaction_runs.push(compaction_keys.len());
            }
            (_, Some(greatest_kv_in_batch)) => *greatest_kv = Some(greatest_kv_in_batch.0.clone()),
            (Some(_), None) | (None, None) => {}
        }
    }

    /// Encodes `updates` into columnar format and writes its parts out to blob
    async fn write_run<T, D>(
        cfg: &PersistConfig,
        batch_parts: &mut BatchParts<T>,
        updates: &mut Vec<((Vec<u8>, Vec<u8>), T, D)>,
        compaction_keys: &mut Vec<PartialBatchKey>,
        desc: Description<T>,
    ) where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut builder = ColumnarRecordsVecBuilder::new_with_len(cfg.blob_target_size);
        for ((k, v), t, d) in updates.drain(..) {
            builder.push(((&k, &v), T::encode(&t), D::encode(&d)));

            // Flush out filled parts as we go to keep bounded memory use.
            for chunk in builder.take_filled() {
                batch_parts
                    .write(chunk, desc.upper().clone(), desc.since().clone())
                    .await;
            }
        }
        for chunk in builder.finish() {
            batch_parts
                .write(chunk, desc.upper().clone(), desc.since().clone())
                .await;
        }
        compaction_keys.extend_from_slice(&batch_parts.flush().await);
    }

    fn validate_req<T: Timestamp>(req: &CompactReq<T>) -> Result<(), anyhow::Error> {
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
    type Item = &'a [PartialBatchKey];

    fn next(&mut self) -> Option<Self::Item> {
        if !self.emitted_implicit {
            self.emitted_implicit = true;
            return Some(match self.inner.peek() {
                None => &self.batch.keys,
                Some(run_end) => &self.batch.keys[0..**run_end],
            });
        }

        if let Some(run_start) = self.inner.next() {
            return Some(match self.inner.peek() {
                Some(run_end) => &self.batch.keys[*run_start..**run_end],
                None => &self.batch.keys[*run_start..],
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use timely::progress::Antichain;

    use crate::tests::{all_ok, expect_fetch_part, new_test_client};

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

        let (mut write, _) = new_test_client()
            .await
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
        let res = Compactor::compact::<u64, i64>(
            write.cfg.clone(),
            Arc::clone(&write.blob),
            Arc::clone(&write.metrics),
            Arc::new(CpuHeavyRuntime::new()),
            req.clone(),
            usize::MAX,
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
