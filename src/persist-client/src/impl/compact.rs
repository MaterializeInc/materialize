// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
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
use crate::r#impl::machine::{retry_external, Machine};
use crate::r#impl::state::HollowBatch;
use crate::r#impl::trace::FueledMergeRes;
use crate::read::fetch_batch_part;
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
                let res = Self::compact::<T, D>(
                    cfg,
                    Arc::clone(&blob),
                    Arc::clone(&metrics),
                    Arc::clone(&cpu_heavy_runtime),
                    req,
                    writer_id,
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
                    for key in res.output.keys {
                        let key = key.complete(&machine.shard_id());
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

    pub async fn compact<T, D>(
        cfg: PersistConfig,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
        req: CompactReq<T>,
        writer_id: WriterId,
    ) -> Result<CompactRes<T>, anyhow::Error>
    where
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let compact_blocking = async move {
            let () = Self::validate_req(&req)?;

            let mut parts = BatchParts::new(
                cfg.batch_builder_max_outstanding_parts,
                Arc::clone(&metrics),
                req.shard_id,
                writer_id,
                req.desc.lower().clone(),
                Arc::clone(&blob),
                &metrics.compaction.batch,
            );

            // TODO: Do this in a bounded amount of memory. The pattern for how
            // to do this has already been established in old persist (see
            // background.rs). We'll have to generalize that from 2 to N batches
            // and also resolve the (much much harder) issue of new persist
            // batch sorted-ness.
            let mut updates = Vec::new();
            for part in req.inputs.iter() {
                for key in part.keys.iter() {
                    fetch_batch_part(
                        &req.shard_id,
                        blob.as_ref(),
                        &metrics,
                        key,
                        &part.desc,
                        |k, v, mut t, d| {
                            t.advance_by(req.desc.since().borrow());
                            let d = D::decode(d);
                            updates.push(((k.to_vec(), v.to_vec()), t, d));
                        },
                    )
                    .await;
                }
            }
            consolidate_updates(&mut updates);

            let len = updates.len();
            let mut builder = ColumnarRecordsVecBuilder::new_with_len(cfg.blob_target_size);
            for ((k, v), t, d) in updates.iter() {
                builder.push(((k, v), T::encode(t), D::encode(d)));

                // Flush out filled parts as we go to keep bounded memory use.
                for chunk in builder.take_filled() {
                    parts
                        .write(chunk, req.desc.upper().clone(), req.desc.since().clone())
                        .await;
                }
            }
            for chunk in builder.finish() {
                parts
                    .write(chunk, req.desc.upper().clone(), req.desc.since().clone())
                    .await;
            }
            let keys = parts.finish().await;

            Ok(CompactRes {
                output: HollowBatch {
                    desc: req.desc,
                    keys,
                    len,
                },
            })
        };

        // Compaction is cpu intensive, so be polite and spawn it on the
        // CPU heavy runtime.
        let compact_span = debug_span!("compact::consolidate");
        cpu_heavy_runtime
            .spawn_named(
                || "persist::compact::consolidate",
                compact_blocking.instrument(compact_span),
            )
            .await?
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
            write.writer_id.clone(),
        )
        .await
        .expect("compaction failed");

        assert_eq!(res.output.desc, req.desc);
        assert_eq!(res.output.len, 1);
        assert_eq!(res.output.keys.len(), 1);
        let key = &res.output.keys[0];
        let (part, updates) = expect_fetch_part(
            write.blob.as_ref(),
            &key.complete(&write.machine.shard_id()),
        )
        .await;
        assert_eq!(part.desc, res.output.desc);
        assert_eq!(updates, all_ok(&data, 10));
    }
}
