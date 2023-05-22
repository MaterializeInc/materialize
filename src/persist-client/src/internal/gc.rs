// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use prometheus::Counter;
use timely::progress::Timestamp;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, debug_span, warn, Instrument, Span};

use mz_ore::cast::CastFrom;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::internal::machine::{retry_external, Machine};
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::RetryMetrics;
use crate::internal::paths::{BlobKey, PartialBatchKey, PartialBlobKey, PartialRollupKey};
use crate::internal::state::HollowBlobRef;
use crate::internal::state_diff::StateDiff;
use crate::internal::state_versions::InspectDiff;
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
        // NB: Because these requests can be processed concurrently (and in
        // arbitrary order), all of the logic below has to work even if we've
        // already gc'd and truncated past new_seqno_since.

        let mut state = machine
            .applier
            .state_versions
            .fetch_all_live_states(req.shard_id)
            .await
            .expect("state is initialized")
            .check_ts_codec()
            .expect("ts codec has not changed");

        // WIP: should just be earliest rollup
        while let Some((truncate_to, _rollup)) = machine
            .applier
            .earliest_rollup_lte_seqno(req.new_seqno_since)
        {
            while let Some(s) = state.next(|diff| match diff {
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
                if s.seqno == truncate_to {
                    break;
                }
            }

            Self::truncate(
                shard_id,
                truncate_to_rollup,
                batch_parts_to_delete,
                rollups_to_delete,
                machine,
                timer,
                &delete_semaphore,
            )
            .await;
        }
        let Some((truncate_to, removable_rollups)) = Self::determine_truncation_range(&req, machine) else {
            // Fast-path: Someone already GC'd past `req.new_seqno_since`, don't
            // bother running any of the below logic.
            //
            // Also a fix for #14580.
            machine.applier.metrics.gc.noop.inc();
            debug!(
                "gc {} early returning, already GC'd past {}",
                req.shard_id, req.new_seqno_since,
            );
            return RoutineMaintenance::default();
        };

        // TODO: we could narrow this and only fetch from [SeqNo::minimum, truncate_to)
        let diffs = machine
            .applier
            .state_versions
            .fetch_all_live_diffs(&req.shard_id)
            .await;
        report_step_timing(&machine.applier.metrics.gc.steps.fetch_seconds);
        machine
            .applier
            .shard_metrics
            .gc_live_diffs
            .set(u64::cast_from(diffs.len()));

        debug!(
            "gc seqno_since: ({}); truncate_to: ({}) got {} versions from scan",
            req.new_seqno_since,
            truncate_to,
            diffs.len()
        );

        let metrics = Arc::clone(&machine.applier.metrics);
        let build_version = machine.applier.cfg.build_version.clone();
        let did_truncate_to = Self::incrementally_truncate(
            &req.shard_id,
            // TODO: in theory this could be an iterator to avoid the `.collect()` but doing
            // so ran into an annoying rustc higher-ranked lifetime bug.
            diffs
                .0
                .iter()
                .filter(|x| x.seqno < truncate_to)
                .map(|x| {
                    metrics
                        .codecs
                        .state_diff
                        .decode(|| StateDiff::<T>::decode(&build_version, x.data.clone()))
                })
                .collect(),
            machine,
            &mut report_step_timing,
        )
        .instrument(debug_span!("incrementally_truncate", truncate_to = %truncate_to))
        .await;

        if let Some(did_truncate_to) = did_truncate_to {
            assert_eq!(truncate_to, did_truncate_to);
        }

        // one final bit of work: remove any references to now-deleted rollups from State.
        // we do this work after the physical deletion of the rollup blobs to ensure we
        // have references to them if GC fails partway through.
        debug!("removing rollups: {:?}", removable_rollups);

        let (removed_rollups, maintenance) = machine.remove_rollups(&removable_rollups).await;
        debug!("removed rollups: {:?}", removed_rollups);
        report_step_timing(&machine.applier.metrics.gc.steps.remove_rollup_seconds);
        maintenance
    }

    /// Determines a safe range of seqnos to delete given the GcReq.
    ///
    /// `GcReq::new_seqno_since` informs us which seqnos are safe to GC based on reader holds, but
    /// GC must also preserve the invariant that the earliest diff in Consensus has a rollup. This
    /// means GC can safely truncate up to the latest rollup <= new_seqno_since.
    ///
    /// Returns None if there are no rollups with SeqNo <= new_seqno_since, otherwise returns the
    /// SeqNo of the latest rollup <= new_seqno_since and a Vec of all earlier rollups (if any).
    fn determine_truncation_range(
        seqno_since: SeqNo,
        machine: &mut Machine<K, V, T, D>,
    ) -> Option<(SeqNo, Vec<(SeqNo, PartialRollupKey)>)> {
        let mut rollups_lte_seqno_since = machine.applier.earliest_rollup_lte_seqno(seqno_since);

        // TODO: we could, if we wanted, leave a configurable N rollups in state to avoid state slow paths.
        // so fetch the Nth latest rollup <= new_seqno_since rather than just the latest. this knob might
        // be less relevant with pubsub, but might be worth having?

        // we will truncate up to the latest rollup <= seqno_since
        let Some((truncate_to, _rollup_to_keep)) = rollups_lte_seqno_since.pop() else {
            return None;
        };
        // all rollups earlier than the latest rollup <= seqno_since are eligible for removal
        let removable_rollups = rollups_lte_seqno_since;

        Some((truncate_to, removable_rollups))
    }

    /// Physically deletes all blobs removed in `diffs` and truncates Consensus
    /// to remove all reference to `diffs`.
    ///
    /// **It is the caller's responsibility to ensure that the deletion of `diffs`
    /// maintains the invariant that the earliest live diff in Consensus has a
    /// rollup.**
    ///
    /// Internally, performs deletions for each rollup encountered, ensuring that
    /// incremental progress is made even if the process is interrupted.
    async fn incrementally_truncate<F>(
        shard_id: &ShardId,
        mut diffs: VecDeque<StateDiff<T>>,
        machine: &mut Machine<K, V, T, D>,
        mut timer: &mut F,
    ) -> Option<SeqNo>
    where
        F: FnMut(&Counter),
    {
        if diffs.is_empty() {
            return None;
        }

        let delete_semaphore = Semaphore::new(
            machine
                .applier
                .cfg
                .dynamic
                .gc_blob_delete_concurrency_limit(),
        );
        let mut batch_parts_to_delete: Vec<PartialBatchKey> = vec![];
        let mut rollups_to_delete: Vec<PartialRollupKey> = vec![];
        let mut truncated_to = None;

        while let Some(seqno) = Self::truncate_to_next_rollup_or_end(
            shard_id,
            &mut diffs,
            machine,
            timer,
            &mut batch_parts_to_delete,
            &mut rollups_to_delete,
            &delete_semaphore,
        )
        .await
        {
            if let Some(truncated_to) = &truncated_to {
                assert!(seqno > *truncated_to);
            }
            truncated_to = Some(seqno);
        }

        truncated_to
    }

    async fn truncate_to_next_rollup_or_end<F>(
        shard_id: &ShardId,
        diffs: &mut VecDeque<StateDiff<T>>,
        machine: &mut Machine<K, V, T, D>,
        mut timer: &mut F,
        batch_parts_to_delete: &mut Vec<PartialBatchKey>,
        rollups_to_delete: &mut Vec<PartialRollupKey>,
        delete_semaphore: &Semaphore,
    ) -> Option<SeqNo>
    where
        F: FnMut(&Counter),
    {
        if diffs.is_empty() {
            return None;
        }

        let mut latest_seqno_seen = SeqNo::minimum();
        // move all of this into Iterator closure
        while let Some(diff) = diffs.pop_front() {
            assert!(latest_seqno_seen < diff.seqno_to);
            latest_seqno_seen = diff.seqno_to;

            let mut truncate_to_rollup = None;
            diff.map_blob_inserts(|blob| match blob {
                HollowBlobRef::Rollup(x) => {
                    // NB: a rollup of seqno X may be written at any seqno >= X, so we must parse
                    // the rollup key to know how far we're allowed to truncate. it is NOT safe
                    // to use `diff.seqno_to` to determine the truncation point!
                    match BlobKey::parse_ids(&x.key.complete(shard_id)).expect("valid rollup key") {
                        (_shard, PartialBlobKey::Rollup(rollup_seqno, _rollup_id)) => {
                            assert!(rollup_seqno >= diff.seqno_to);
                            truncate_to_rollup = Some(rollup_seqno);
                        }
                        (_, _) => {
                            panic!("invalid rollup blob: {:?}", x);
                        }
                    }
                }
                _ => {}
            });

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

            // each time we discover a new rollup, we use it as a checkpoint
            // to delete blobs in batches / truncate state. this makes the
            // overall GC process incremental, operating on the ranges
            // between rollups.
            if let Some(truncate_to_rollup) = truncate_to_rollup {
                debug!("truncating up to rollup: {}", truncate_to_rollup);

                Self::truncate(
                    shard_id,
                    truncate_to_rollup,
                    batch_parts_to_delete,
                    rollups_to_delete,
                    machine,
                    timer,
                    &delete_semaphore,
                )
                .await;

                return Some(truncate_to_rollup);
            }
        }

        assert!(latest_seqno_seen > SeqNo::minimum());

        // `truncate` deletes all diffs < seqno, so in order to fully
        // delete all of the diffs passed in we must truncate to the
        // latest seqno we observed + 1
        let truncate_to = latest_seqno_seen.next();
        debug!("truncating up to truncation point: {}", truncate_to);
        Self::truncate(
            shard_id,
            truncate_to,
            &mut pending_batch_parts_to_delete,
            &mut pending_rollups_to_delete,
            machine,
            &mut timer,
            &delete_semaphore,
        )
        .await;

        Some(truncate_to)
    }

    /// Deletes all batch parts and rollups. Truncates all Consensus diffs < `truncate_to`.
    async fn truncate<F>(
        shard_id: &ShardId,
        truncate_to: SeqNo,
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

        machine
            .applier
            .state_versions
            .truncate_diffs(shard_id, truncate_to)
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
