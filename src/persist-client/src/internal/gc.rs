// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use prometheus::Counter;
use timely::progress::Timestamp;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, debug_span, info, warn, Instrument, Span};

use mz_ore::cast::CastFrom;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::internal::machine::{retry_external, Machine};
use crate::internal::maintenance;
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::{GcStepTimings, RetryMetrics};
use crate::internal::paths::{BlobKey, PartialBatchKey, PartialRollupKey};
use crate::internal::state::HollowBlobRef;
use crate::internal::state_versions::{InspectDiff, StateVersionsIter};
use crate::ShardId;

#[derive(Debug, Clone, PartialEq)]
pub struct GcReq {
    pub shard_id: ShardId,
    pub new_seqno_since: SeqNo,
}

#[derive(Debug)]
pub struct GarbageCollector<K, V, T, D> {
    sender: UnboundedSender<(GcReq, oneshot::Sender<RoutineMaintenance>)>,
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
            mpsc::unbounded_channel::<(GcReq, oneshot::Sender<RoutineMaintenance>)>();

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
                let mut maintenance = Self::gc_and_truncate(&mut machine, consolidated_req)
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
                    // wasn't interested in waiting and dropped their receiver.
                    // maintenance will be somewhat-arbitrarily assigned to the first oneshot.
                    let _ = sender.send(mem::take(&mut maintenance));
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
    pub fn gc_and_truncate_background(
        &self,
        req: GcReq,
    ) -> Option<oneshot::Receiver<RoutineMaintenance>> {
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

    pub async fn gc_and_truncate(
        machine: &mut Machine<K, V, T, D>,
        req: GcReq,
    ) -> RoutineMaintenance {
        let mut step_start = Instant::now();
        let mut report_step_timing = |counter: &Counter| {
            let now = Instant::now();
            counter.inc_by(now.duration_since(step_start).as_secs_f64());
            step_start = now;
        };
        assert_eq!(req.shard_id, machine.shard_id());

        let mut states = machine
            .applier
            .state_versions
            .fetch_all_live_states(req.shard_id)
            .await
            .expect("state is initialized")
            .check_ts_codec()
            .expect("ts codec has not changed");
        report_step_timing(&machine.applier.metrics.gc.steps.fetch_seconds);

        machine
            .applier
            .shard_metrics
            .gc_live_diffs
            .set(u64::cast_from(states.len()));

        debug!(
            "gc seqno_since: ({}) got {} versions from scan",
            req.new_seqno_since,
            states.len()
        );

        let maintenance = Self::incrementally_gc(
            &req.shard_id,
            &mut states,
            req.new_seqno_since,
            machine,
            &mut report_step_timing,
        )
        .await;

        // WIP: do we want to calculate the seqno_parts_held metric?
        while let Some(_) = states.next(|_| {}) {}

        maintenance
    }

    /// Physically deletes all blobs from Blob and live diffs from Consensus that
    /// are safe to delete, given the `seqno_since`, ensuring that the earliest
    /// live diff in Consensus has a rollup of seqno `<= seqno_since`.
    ///
    /// Internally, performs deletions for each rollup encountered, ensuring that
    /// incremental progress is made even if the process is interrupted before
    /// completing all gc work.
    async fn incrementally_gc<F>(
        shard_id: &ShardId,
        states: &mut StateVersionsIter<T>,
        seqno_since: SeqNo,
        machine: &mut Machine<K, V, T, D>,
        timer: &mut F,
    ) -> RoutineMaintenance
    where
        F: FnMut(&Counter),
    {
        let delete_semaphore = Semaphore::new(
            machine
                .applier
                .cfg
                .dynamic
                .gc_blob_delete_concurrency_limit(),
        );
        let mut batch_parts_to_delete: Vec<PartialBatchKey> = vec![];
        let mut rollups_to_delete: Vec<PartialRollupKey> = vec![];

        // we determine which rollups are removable from the *latest* state known
        // to this process, which may be arbitrarily far ahead of `states`. we do
        // this for several reasons, but most importantly because we truncate live
        // diffs in Consensus before we remove rollups from state, WIP explain more plz
        let mut removable_rollups: Vec<_> = machine
            .applier
            .removable_rollups()
            .into_iter()
            .filter(|(seqno, _rollup)| *seqno <= seqno_since)
            .collect();

        info!(
            "Removing to ({}). Eligible rollups: ({:?})",
            seqno_since, removable_rollups
        );

        for (truncate_lt, _rollup) in &removable_rollups {
            assert!(*truncate_lt <= seqno_since);
            Self::find_removable_blobs(
                states,
                *truncate_lt,
                &machine.applier.metrics.gc.steps,
                timer,
                &mut batch_parts_to_delete,
                &mut rollups_to_delete,
            );

            info!(
                "While truncating lt ({}), removing: {:?}",
                truncate_lt, rollups_to_delete
            );

            Self::delete_and_truncate(
                shard_id,
                *truncate_lt,
                &mut batch_parts_to_delete,
                &mut rollups_to_delete,
                machine,
                timer,
                &delete_semaphore,
            )
            .await;

            assert!(batch_parts_to_delete.is_empty());
            assert!(rollups_to_delete.is_empty());
        }

        removable_rollups.pop();

        let (removed_rollups, maintenance) = machine.remove_rollups(&removable_rollups).await;
        info!("removed rollups: {:?}", removed_rollups);
        maintenance
    }

    /// Iterates through `states`, accumulating all deleted blobs (both batch parts
    /// and rollups) until `truncate_lt` (exclusive).
    fn find_removable_blobs<F>(
        states: &mut StateVersionsIter<T>,
        truncate_lt: SeqNo,
        metrics: &GcStepTimings,
        timer: &mut F,
        batch_parts_to_delete: &mut Vec<PartialBatchKey>,
        rollups_to_delete: &mut Vec<PartialRollupKey>,
    ) where
        F: FnMut(&Counter),
    {
        // if our state is already past the truncation point, there's nothing to do
        if states.state().seqno().next() >= truncate_lt {
            return;
        }

        assert!(states.state().seqno.next() <= truncate_lt);
        while let Some(state) = states.next(|diff| match diff {
            InspectDiff::FromInitial(_) => {}
            InspectDiff::Diff(diff) => {
                diff.map_blob_deletes(|blob| match blob {
                    HollowBlobRef::Batch(batch) => {
                        for part in &batch.parts {
                            batch_parts_to_delete.push(part.key.to_owned());
                        }
                    }
                    HollowBlobRef::Rollup(rollup) => {
                        rollups_to_delete.push(rollup.key.to_owned());
                    }
                });
            }
        }) {
            if state.seqno.next() == truncate_lt {
                break;
            }
        }

        // there should always be enough live diffs to reach `truncate_lt`
        // if our initial seqno.next() was <= `truncate_lt`.
        //
        // WIP: there's a wonderful proof but doesn't fit in this comment...
        assert_eq!(states.state().seqno.next(), truncate_lt);

        timer(&metrics.find_deletable_blobs_seconds);
    }

    /// Deletes all batch parts and rollups from Blob.
    /// Truncates Consensus to `truncate_lt`.
    async fn delete_and_truncate<F>(
        shard_id: &ShardId,
        truncate_lt: SeqNo,
        batch_parts: &mut Vec<PartialBatchKey>,
        rollups: &mut Vec<PartialRollupKey>,
        machine: &mut Machine<K, V, T, D>,
        timer: &mut F,
        delete_semaphore: &Semaphore,
    ) where
        F: FnMut(&Counter),
    {
        Self::delete_all(
            machine.applier.state_versions.blob.borrow(),
            batch_parts.drain(..).map(|k| k.complete(shard_id)),
            &machine.applier.metrics.retries.external.rollup_delete,
            debug_span!("rollup::delete"),
            delete_semaphore,
        )
        .await;
        timer(&machine.applier.metrics.gc.steps.delete_rollup_seconds);

        Self::delete_all(
            machine.applier.state_versions.blob.borrow(),
            rollups.drain(..).map(|k| k.complete(shard_id)),
            &machine.applier.metrics.retries.external.batch_delete,
            debug_span!("batch::delete"),
            delete_semaphore,
        )
        .await;
        timer(&machine.applier.metrics.gc.steps.delete_batch_part_seconds);

        // WIP: not a bad spot for a failpoint if we want to try that

        machine
            .applier
            .state_versions
            .truncate_diffs(shard_id, truncate_lt)
            .await;
        timer(&machine.applier.metrics.gc.steps.truncate_diff_seconds);
    }

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
}
