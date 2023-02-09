// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use mz_ore::cast::CastFrom;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};
use prometheus::Counter;
use timely::progress::Timestamp;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, debug_span, warn, Instrument, Span};

use crate::internal::machine::{retry_external, Machine};
use crate::internal::metrics::RetryMetrics;
use crate::internal::paths::{BlobKey, PartialRollupKey, RollupId};
use crate::ShardId;

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct GcReq {
    pub shard_id: ShardId,
    pub new_seqno_since: SeqNo,
}

#[derive(Debug)]
pub struct GarbageCollector<K, V, T, D> {
    sender: UnboundedSender<(GcReq, oneshot::Sender<()>)>,
    _phantom: PhantomData<fn() -> (K, V, T, D)>,
}

impl<K, V, T, D> Clone for GarbageCollector<K, V, T, D> {
    fn clone(&self) -> Self {
        GarbageCollector {
            sender: self.sender.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Cleanup for no longer necessary blobs and consensus versions.
///
/// - Every read handle, snapshot, and listener is given a capability on seqno
///   with a very long lease (allowing for infrequent heartbeats). This is a
///   guarantee that no blobs referenced by the state at that version will be
///   deleted (even if they've been compacted in some newer version of the
///   state). This is called a seqno_since in the code as it has obvious
///   parallels to how sinces work at the shard/collection level. (Is reusing
///   "since" here a good idea? We could also call it a "seqno capability" or
///   something else instead.)
/// - Every state transition via apply_unbatched_cmd has the opportunity to
///   determine that the overall seqno_since for the shard has changed. In the
///   common case in production, this will be in response to a snapshot
///   finishing or a listener emitting some batch.
/// - It would be nice if this only ever happened in response to read-y things
///   (there'd be a nice parallel to how compaction background work is only
///   spawned by write activity), but if there are no readers, we still very
///   much want to continue to garbage collect. Notably, if there are no
///   readers, we naturally only need to hold a capability on the current
///   version of state. This means that if there are only writers, a write
///   commands will result in the seqno_since advancing immediately from the
///   previous version of the state to the new one.
/// - Like Compacter, GarbageCollector uses a heuristic to ignore some requests
///   to save work. In this case, the tradeoff is between consensus traffic
///   (plus a bit of cpu) and keeping blobs around longer than strictly
///   necessary. This is correct because a process could always die while
///   executing one of these requests (or be slow and still working on it when
///   the next request is generated), so we anyway need to handle them being
///   dropped.
/// - GarbageCollector works by `Consensus::scan`-ing for every live version of
///   state (ignoring what the request things the prev_state_seqno was for the
///   reasons mentioned immediately above). It then walks through them in a
///   loop, accumulating a BTreeSet of every referenced blob key. When it finds
///   the version corresponding to the new_seqno_since, it removes every blob in
///   that version of the state from the BTreeSet and exits the loop. This
///   results in the BTreeSet containing every blob eligible for deletion. It
///   deletes those blobs and then truncates the state to the new_seqno_since to
///   indicate that this work doesn't need to be done again.
/// - Note that these requests are being processed concurrently, so it's always
///   possible that some future request has already deleted the blobs and
///   truncated consensus. It's also possible that this is the future request.
///   As a result, the only guarantee that we get is that the current version of
///   head is >= new_seqno_since.
/// - (Aside: The above also means that if Blob is not linearizable, there is a
///   possible race where a blob gets deleted before it written and thus is
///   leaked. We anyway always have the possibility of a write process being
///   killed between when it writes a blob and links it into state, so this is
///   fine; it'll be caught and fixed by the same mechanism.)
impl<K, V, T, D> GarbageCollector<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub fn new(mut machine: Machine<K, V, T, D>) -> Self {
        let (gc_req_sender, mut gc_req_recv) =
            mpsc::unbounded_channel::<(GcReq, oneshot::Sender<()>)>();

        // spin off a single task responsible for executing GC requests.
        // work is enqueued into the task through a channel
        let _worker_handle = mz_ore::task::spawn(|| "PersistGcWorker", async move {
            while let Some((req, completer)) = gc_req_recv.recv().await {
                let mut consolidated_req = req;
                let mut gc_completed_senders = vec![completer];

                // check if any further gc requests have built up. we'll merge their requests
                // together and run a single GC pass to satisfy all of them
                while let Ok((req, completer)) = gc_req_recv.try_recv() {
                    assert_eq!(req.shard_id, consolidated_req.shard_id);
                    gc_completed_senders.push(completer);
                    consolidated_req.new_seqno_since =
                        std::cmp::max(req.new_seqno_since, consolidated_req.new_seqno_since);
                }

                let merged_requests = gc_completed_senders.len() - 1;
                if merged_requests > 0 {
                    machine
                        .applier
                        .metrics
                        .gc
                        .merged
                        .inc_by(u64::cast_from(merged_requests));
                    debug!(
                        "Merged {} gc requests together for shard {}",
                        merged_requests, consolidated_req.shard_id
                    );
                }

                let gc_span = debug_span!(parent: None, "gc_and_truncate", shard_id=%consolidated_req.shard_id);
                gc_span.follows_from(&Span::current());

                let start = Instant::now();
                machine.applier.metrics.gc.started.inc();
                Self::gc_and_truncate(&mut machine, consolidated_req)
                    .instrument(gc_span)
                    .await;
                machine.applier.metrics.gc.finished.inc();
                machine.applier.shard_metrics.gc_finished.inc();
                machine
                    .applier
                    .metrics
                    .gc
                    .seconds
                    .inc_by(start.elapsed().as_secs_f64());

                // inform all callers who enqueued GC reqs that their work is complete
                for sender in gc_completed_senders {
                    // we can safely ignore errors here, it's possible the caller
                    // wasn't interested in waiting and dropped their receiver
                    let _ = sender.send(());
                }
            }
        });

        GarbageCollector {
            sender: gc_req_sender,
            _phantom: PhantomData,
        }
    }

    /// Enqueues a [GcReq] to be consumed by the GC background task when available.
    ///
    /// Returns a future that indicates when GC has cleaned up to at least [GcReq::new_seqno_since]
    pub fn gc_and_truncate_background(&self, req: GcReq) -> Option<oneshot::Receiver<()>> {
        let (gc_completed_sender, gc_completed_receiver) = oneshot::channel();
        let new_gc_sender = self.sender.clone();
        let send = new_gc_sender.send((req, gc_completed_sender));

        if let Err(e) = send {
            // In the steady state we expect this to always succeed, but during
            // shutdown it is possible the destination task has already spun down
            warn!(
                "gc_and_truncate_background failed to send gc request: {}",
                e
            );
            return None;
        }

        Some(gc_completed_receiver)
    }

    pub async fn gc_and_truncate(machine: &mut Machine<K, V, T, D>, req: GcReq) {
        // There's also a bulk delete API in s3 if the performance of this
        // becomes an issue. Maybe make Blob::delete take a list of keys?
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        async fn delete_all(
            blob: &(dyn Blob + Send + Sync),
            keys: impl Iterator<Item = BlobKey>,
            metrics: &RetryMetrics,
            span: Span,
            semaphore: &Semaphore,
        ) {
            let futures = FuturesUnordered::new();
            for key in keys {
                futures.push(
                    retry_external(metrics, move || {
                        let key = key.clone();
                        async move {
                            let _permit = semaphore
                                .acquire()
                                .await
                                .expect("acquiring permit from open semaphore");
                            blob.delete(&key).await.map(|_| ())
                        }
                    })
                    .instrument(span.clone()),
                )
            }

            futures.collect().await
        }

        let delete_semaphore = Semaphore::new(
            machine
                .applier
                .cfg
                .dynamic
                .gc_blob_delete_concurrency_limit(),
        );

        let mut step_start = Instant::now();
        let mut report_step_timing = |counter: &Counter| {
            let now = Instant::now();
            counter.inc_by(now.duration_since(step_start).as_secs_f64());
            step_start = now;
        };

        assert_eq!(req.shard_id, machine.shard_id());
        // NB: Because these requests can be processed concurrently (and in
        // arbitrary order), all of the logic below has to work even if we've
        // already gc'd and truncated past new_seqno_since.

        let mut states = machine
            .applier
            .state_versions
            .fetch_all_live_states::<T>(req.shard_id)
            .await
            // TODO: Consider pulling the K, V, D params off of GC. If we do,
            // then we should be able to delete TypedStateVersionsIter (and
            // probably merge UntypedStateVersionsIter into StateVersionsIter).
            .check_codecs::<K, V, D>()
            .expect("shard codecs should not change");

        debug!(
            "gc {} for {} got {} versions from scan",
            req.shard_id,
            req.new_seqno_since,
            states.len()
        );
        report_step_timing(&machine.applier.metrics.gc.steps.fetch_seconds);

        let earliest_live_seqno = match states.peek_seqno() {
            Some(x) => x,
            None => panic!(
                "initialized shard {} should have at least one live state",
                req.shard_id
            ),
        };
        // Fast-path: Someone already GC'd past `req.new_seqno_since`, don't
        // bother running any of the below logic.
        //
        // Also a fix for #14580.
        if earliest_live_seqno > req.new_seqno_since {
            machine.applier.metrics.gc.noop.inc();
            debug!(
                "gc {} early returning, already GC'd past {}",
                req.shard_id, req.new_seqno_since,
            );
            return;
        }

        let mut deleteable_batch_blobs = BTreeSet::new();
        let mut deleteable_rollup_blobs = Vec::new();
        let mut live_diffs = 0;
        let mut seqno_held_parts = BTreeSet::new();

        let mut state_count = 0;

        while let Some(state) = states.next() {
            if state_count % 1000 == 0 {
                let batch_count = state.collections.trace.batches().into_iter().count();
                debug!(
                    "gc {} state includes {batch_count} batches after applying {state_count} diffs (seqno {:?})",
                    req.shard_id, state.seqno
                );
            }
            state_count += 1;

            match state.seqno.cmp(&req.new_seqno_since) {
                Ordering::Less => {
                    state.collections.trace.map_batches(|b| {
                        for part in b.parts.iter() {
                            // It's okay (expected) if the key already exists in
                            // deleteable_batch_blobs, it may have been present in
                            // previous versions of state.
                            deleteable_batch_blobs.insert(part.key.to_owned());
                        }
                    });
                }
                Ordering::Equal => {
                    live_diffs += 1;
                    state.collections.trace.map_batches(|b| {
                        for part in b.parts.iter() {
                            // It's okay (expected) if the key doesn't exist in
                            // deleteable_batch_blobs, it may have been added in
                            // this version of state.
                            let _ = deleteable_batch_blobs.remove(&part.key);
                            seqno_held_parts.insert(part.key.to_owned());
                        }
                    });
                    // We only need to detect deletable rollups in the last iter
                    // through the live_diffs loop because they accumulate in state.
                    for (seqno, rollup) in state.collections.rollups.iter() {
                        // SUBTLE: We only guarantee that a rollup exists for the
                        // first live state. Anything before that is not allowed to
                        // be used and so is free to be deleted and removed from
                        // state.
                        if seqno < &earliest_live_seqno {
                            deleteable_rollup_blobs.push((*seqno, rollup.key.clone()));
                        } else {
                            // We iterate in order, may as well short circuit the
                            // rollup loop.
                            break;
                        }
                    }
                    break;
                }
                Ordering::Greater => {
                    break;
                }
            }
        }

        debug!(
            "gc {} collected {} deleteable batch blobs, {} deleteable rollup blobs",
            req.shard_id,
            deleteable_batch_blobs.len(),
            deleteable_rollup_blobs.len()
        );
        report_step_timing(&machine.applier.metrics.gc.steps.apply_diff_seconds);

        // Delete the rollup blobs before removing them from state.
        delete_all(
            machine.applier.state_versions.blob.borrow(),
            deleteable_rollup_blobs
                .iter()
                .map(|(_, k)| k.complete(&req.shard_id)),
            &machine.applier.metrics.retries.external.rollup_delete,
            debug_span!("rollup::delete"),
            &delete_semaphore,
        )
        .await;
        debug!("gc {} deleted rollup blobs", req.shard_id);
        report_step_timing(&machine.applier.metrics.gc.steps.delete_rollup_seconds);

        // As described in the big rustdoc comment on [StateVersions], we
        // maintain the invariant that there is always a rollup corresponding to
        // the seqno of the first live version in consensus. So, write a new
        // rollup at exactly req.new_seqno_since so we're free to truncate
        // anything before it.
        //
        // NB: We write rollups periodically (via maintenance) to cover the case
        // when GC is being held up by a long seqno hold (such as the 15m read
        // lease timeouts whenever environmentd restarts).
        let state = states.state();
        assert_eq!(state.seqno, req.new_seqno_since);
        let rollup = machine.applier.state_versions.encode_rollup_blob(
            &machine.applier.shard_metrics,
            state,
            PartialRollupKey::new(state.seqno, &RollupId::new()),
        );
        let () = machine
            .applier
            .state_versions
            .write_rollup_blob(&rollup)
            .await;
        let applied = machine
            .add_and_remove_rollups(
                (rollup.seqno, &rollup.to_hollow()),
                &deleteable_rollup_blobs,
            )
            .await;
        // We raced with some other GC process to write this rollup out. Ours
        // wasn't registered, so delete it.
        if !applied {
            machine
                .applier
                .state_versions
                .delete_rollup(&rollup.shard_id, &rollup.key)
                .await;
        }
        debug!(
            "gc {} wrote rollup at seqno {}. applied={}",
            req.shard_id, rollup.seqno, applied
        );
        report_step_timing(&machine.applier.metrics.gc.steps.write_rollup_seconds);

        delete_all(
            machine.applier.state_versions.blob.borrow(),
            deleteable_batch_blobs
                .into_iter()
                .map(|k| k.complete(&req.shard_id)),
            &machine.applier.metrics.retries.external.batch_delete,
            debug_span!("batch::delete"),
            &delete_semaphore,
        )
        .await;
        debug!("gc {} deleted batch blobs", req.shard_id);
        report_step_timing(&machine.applier.metrics.gc.steps.delete_batch_part_seconds);

        // Now that we've deleted the eligible blobs, "commit" this info by
        // truncating the state versions that referenced them.
        machine
            .applier
            .state_versions
            .truncate_diffs(&req.shard_id, req.new_seqno_since)
            .await;
        debug!(
            "gc {} truncated diffs through seqno {}",
            req.shard_id, req.new_seqno_since
        );
        report_step_timing(&machine.applier.metrics.gc.steps.truncate_diff_seconds);

        // Finally, apply the remaining diffs to calculate metrics.
        while let Some(state) = states.next() {
            live_diffs += 1;
            state.collections.trace.map_batches(|b| {
                for part in b.parts.iter() {
                    seqno_held_parts.insert(part.key.to_owned());
                }
            });
        }

        // Remove the current batch's parts from the set; we only count parts
        // that are in some live diff but not the current state.
        let state = states.state();
        state.collections.trace.map_batches(|b| {
            for part in b.parts.iter() {
                let _ = seqno_held_parts.remove(&part.key);
            }
        });

        let shard_metrics = machine.applier.metrics.shards.shard(&req.shard_id);
        shard_metrics.set_gc_seqno_held_parts(seqno_held_parts.len());
        shard_metrics.gc_live_diffs.set(live_diffs);
        report_step_timing(&machine.applier.metrics.gc.steps.finish_seconds);
    }
}
