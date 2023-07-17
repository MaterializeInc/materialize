// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::collections::BTreeSet;
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
use tracing::{debug, debug_span, error, warn, Instrument, Span};

use crate::async_runtime::IsolatedRuntime;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::internal::machine::{retry_external, Machine};
use crate::internal::maintenance::RoutineMaintenance;
use crate::internal::metrics::{GcStepTimings, RetryMetrics};
use crate::internal::paths::{BlobKey, PartialBatchKey, PartialBlobKey, PartialRollupKey};
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
    pub fn new(machine: Machine<K, V, T, D>, isolated_runtime: Arc<IsolatedRuntime>) -> Self {
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
                let (mut maintenance, _stats) = {
                    let name = format!("gc_and_truncate ({})", &consolidated_req.shard_id);
                    let mut machine = machine.clone();
                    isolated_runtime
                        .spawn_named(|| name, async move {
                            Self::gc_and_truncate(&mut machine, consolidated_req)
                                .instrument(gc_span)
                                .await
                        })
                        .await
                        .expect("gc_and_truncate failed")
                };
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

    pub(crate) async fn gc_and_truncate(
        machine: &mut Machine<K, V, T, D>,
        req: GcReq,
    ) -> (RoutineMaintenance, GcResults) {
        let mut step_start = Instant::now();
        let mut report_step_timing = |counter: &Counter| {
            let now = Instant::now();
            counter.inc_by(now.duration_since(step_start).as_secs_f64());
            step_start = now;
        };
        assert_eq!(req.shard_id, machine.shard_id());

        // Double check our GC req: seqno_since will never regress
        // so we can verify it's not somehow greater than the last-
        // known seqno_since
        if req.new_seqno_since > machine.applier.seqno_since() {
            machine
                .applier
                .fetch_and_update_state(Some(req.new_seqno_since))
                .await;
            let current_seqno_since = machine.applier.seqno_since();
            assert!(
                req.new_seqno_since <= current_seqno_since,
                "invalid gc req: {:?} vs machine seqno_since {}",
                req,
                current_seqno_since
            );
        }

        // First, check the latest known state to this process to see
        // if there's relevant GC work for this seqno_since
        let gc_rollups =
            GcRollups::new(machine.applier.rollups_lte_seqno(req.new_seqno_since), &req);
        let rollups_to_remove_from_state = gc_rollups.rollups_to_remove_from_state();
        report_step_timing(&machine.applier.metrics.gc.steps.find_removable_rollups);

        let mut gc_results = GcResults::default();

        if rollups_to_remove_from_state.is_empty() {
            // If there are no rollups to remove from state (either the work has already
            // been done, or the there aren't enough rollups <= seqno_since to have any
            // to delete), we can safely exit.
            machine.applier.metrics.gc.noop.inc();
            return (RoutineMaintenance::default(), gc_results);
        }

        debug!(
            "Finding all rollups <= ({}). Will truncate: {:?}. Will remove rollups from state: {:?}",
            req.new_seqno_since,
            gc_rollups.truncate_seqnos().collect::<Vec<_>>(),
            rollups_to_remove_from_state,
        );

        let mut states = machine
            .applier
            .state_versions
            .fetch_all_live_states(req.shard_id)
            .await
            .expect("state is initialized")
            .check_ts_codec()
            .expect("ts codec has not changed");
        let initial_seqno = states.state().seqno;
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

        Self::incrementally_delete_and_truncate(
            &mut states,
            &gc_rollups,
            machine,
            &mut report_step_timing,
            &mut gc_results,
        )
        .await;

        // Now that the blobs are deleted / Consensus is truncated, remove
        // the rollups from state. Doing this at the end ensures that our
        // invariant is maintained that the current state contains a rollup
        // to the earliest state in Consensus, and ensures that if GC crashes
        // part-way through, we still have a reference to these rollups to
        // resume their deletion.
        //
        // This does mean that if GC crashes part-way through we would
        // repeat work when it resumes. However the redundant work should
        // be minimal as Consensus is incrementally truncated, allowing
        // the next run of GC to skip any work needed for rollups less
        // than the last truncation.
        //
        // In short, while this step is not incremental, it does not need
        // to be for GC to efficiently resume. And in fact, making it
        // incremental could be quite expensive (e.g. more CaS operations).
        let (removed_rollups, maintenance) =
            machine.remove_rollups(rollups_to_remove_from_state).await;
        report_step_timing(&machine.applier.metrics.gc.steps.remove_rollups_from_state);
        debug!("CaS removed rollups from state: {:?}", removed_rollups);
        gc_results.rollups_removed_from_state = removed_rollups;

        // Everything here and below is not strictly needed for GC to complete,
        // but it's a good opportunity, while we have all live states in hand,
        // to run some metrics and assertions.

        // Apply all remaining live states to rollup some metrics, like how many
        // parts are being held (in Blob) that are not part of the latest state.
        let mut seqno_held_parts = 0;
        while let Some(_) = states.next(|diff| match diff {
            InspectDiff::FromInitial(_) => {}
            InspectDiff::Diff(diff) => {
                diff.map_blob_deletes(|blob| match blob {
                    HollowBlobRef::Batch(batch) => {
                        seqno_held_parts += batch.parts.len();
                    }
                    HollowBlobRef::Rollup(_) => {}
                });
            }
        }) {}

        machine
            .applier
            .shard_metrics
            .gc_seqno_held_parts
            .set(u64::cast_from(seqno_held_parts));

        // verify that the "current" state (as of `fetch_all_live_states`) contains
        // a rollup to the earliest state we fetched. this invariant isn't affected
        // by the GC work we just performed, but it is a property of GC correctness
        // overall / is a convenient place to run the assertion.
        let valid_pre_gc_state = states
            .state()
            .collections
            .rollups
            .contains_key(&initial_seqno);

        debug_assert!(
            valid_pre_gc_state,
            "rollups = {:?}, state seqno = {}",
            states.state().collections.rollups,
            initial_seqno
        );

        if !valid_pre_gc_state {
            // this should never be true in the steady-state, but may be true the
            // first time GC runs after fixing any correctness bugs related to our
            // state version invariants. we'll make it an error so we can track
            // any violations in Sentry, but opt not to panic because the root
            // cause of the violation cannot be from this GC run (in fact, this
            // GC run, assuming it's correct, should have fixed the violation!)
            error!("earliest state fetched during GC did not have corresponding rollup: rollups = {:?}, state seqno = {}",
                states.state().collections.rollups,
                initial_seqno
            );
        }

        report_step_timing(
            &machine
                .applier
                .metrics
                .gc
                .steps
                .post_gc_calculations_seconds,
        );

        (maintenance, gc_results)
    }

    /// Physically deletes all blobs from Blob and live diffs from Consensus that
    /// are safe to delete, given the `seqno_since`, ensuring that the earliest
    /// live diff in Consensus has a rollup of seqno `<= seqno_since`.
    ///
    /// Internally, performs deletions for each rollup encountered, ensuring that
    /// incremental progress is made even if the process is interrupted before
    /// completing all gc work.
    async fn incrementally_delete_and_truncate<F>(
        states: &mut StateVersionsIter<T>,
        gc_rollups: &GcRollups,
        machine: &mut Machine<K, V, T, D>,
        timer: &mut F,
        gc_results: &mut GcResults,
    ) where
        F: FnMut(&Counter),
    {
        assert_eq!(states.state().shard_id, machine.shard_id());
        let shard_id = states.state().shard_id;
        let mut batch_parts_to_delete: BTreeSet<PartialBatchKey> = BTreeSet::new();
        let mut rollups_to_delete: BTreeSet<PartialRollupKey> = BTreeSet::new();

        for truncate_lt in gc_rollups.truncate_seqnos() {
            assert!(batch_parts_to_delete.is_empty());
            assert!(rollups_to_delete.is_empty());

            // our state is already past the truncation point. there's no work to do --
            // some process already truncated this far
            if states.state().seqno >= truncate_lt {
                continue;
            }

            // By our invariant, `states` should always begin on a rollup.
            assert!(
                gc_rollups.contains_seqno(&states.state().seqno),
                "rollups = {:?}, state seqno = {}",
                gc_rollups,
                states.state().seqno
            );

            Self::find_removable_blobs(
                states,
                truncate_lt,
                &machine.applier.metrics.gc.steps,
                timer,
                &mut batch_parts_to_delete,
                &mut rollups_to_delete,
            );

            // After finding removable blobs, our state should be exactly `truncate_lt`,
            // to ensure we've seen all blob deletions in the diffs needed to reach
            // this seqno.
            //
            // That we can always reach `truncate_lt` given the live diffs we fetched
            // earlier is a little subtle:
            // * Our GC request was generated after `seqno_since` was written.
            // * If our initial seqno on this loop was < `truncate_lt`, then our read
            //   to `fetch_all_live_states` must have seen live diffs through at least
            //   `seqno_since`, because the diffs were not yet truncated.
            // * `seqno_since` >= `truncate_lt`, therefore we must have enough live
            //   diffs to reach `truncate_lt`.
            assert_eq!(states.state().seqno, truncate_lt);
            // `truncate_lt` _is_ the seqno of a rollup, but let's very explicitly
            // assert that we're about to truncate everything less than a rollup
            // to maintain our invariant.
            assert!(
                gc_rollups.contains_seqno(&states.state().seqno),
                "rollups = {:?}, state seqno = {}",
                gc_rollups,
                states.state().seqno
            );

            // Extra paranoia: verify that none of the blobs we're about to delete
            // are in our current state (we should only be truncating blobs from
            // before this state!)
            states.state().map_blobs(|blob| match blob {
                HollowBlobRef::Batch(batch) => {
                    for live_part in &batch.parts {
                        assert_eq!(batch_parts_to_delete.get(&live_part.key), None);
                    }
                }
                HollowBlobRef::Rollup(live_rollup) => {
                    assert_eq!(rollups_to_delete.get(&live_rollup.key), None);
                    // And double check that the rollups we're about to delete are
                    // earlier than our truncation point:
                    match BlobKey::parse_ids(&live_rollup.key.complete(&shard_id)) {
                        Ok((_shard, PartialBlobKey::Rollup(rollup_seqno, _rollup))) => {
                            assert!(rollup_seqno < truncate_lt);
                        }
                        _ => {
                            panic!("invalid rollup during deletion: {:?}", live_rollup);
                        }
                    }
                }
            });

            gc_results.truncated_consensus_to.push(truncate_lt);
            gc_results.batch_parts_deleted_from_blob += batch_parts_to_delete.len();
            gc_results.rollups_deleted_from_blob += rollups_to_delete.len();

            Self::delete_and_truncate(
                truncate_lt,
                &mut batch_parts_to_delete,
                &mut rollups_to_delete,
                machine,
                timer,
            )
            .await;
        }
    }

    /// Iterates through `states`, accumulating all deleted blobs (both batch parts
    /// and rollups) until reaching the seqno `truncate_lt`.
    ///
    /// * The initial seqno of `states` MUST be less than `truncate_lt`.
    /// * The seqno of `states` after this fn will be exactly `truncate_lt`.
    fn find_removable_blobs<F>(
        states: &mut StateVersionsIter<T>,
        truncate_lt: SeqNo,
        metrics: &GcStepTimings,
        timer: &mut F,
        batch_parts_to_delete: &mut BTreeSet<PartialBatchKey>,
        rollups_to_delete: &mut BTreeSet<PartialRollupKey>,
    ) where
        F: FnMut(&Counter),
    {
        assert!(states.state().seqno < truncate_lt);
        while let Some(state) = states.next(|diff| match diff {
            InspectDiff::FromInitial(_) => {}
            InspectDiff::Diff(diff) => {
                diff.map_blob_deletes(|blob| match blob {
                    HollowBlobRef::Batch(batch) => {
                        for part in &batch.parts {
                            // we use BTreeSets for fast lookups elsewhere, but we should never
                            // see repeat blob insertions within a single GC run, otherwise we
                            // have a logic error or our diffs are incorrect (!)
                            assert!(batch_parts_to_delete.insert(part.key.to_owned()));
                        }
                    }
                    HollowBlobRef::Rollup(rollup) => {
                        assert!(rollups_to_delete.insert(rollup.key.to_owned()));
                    }
                });
            }
        }) {
            if state.seqno == truncate_lt {
                break;
            }
        }
        timer(&metrics.find_deletable_blobs_seconds);
    }

    /// Deletes `batch_parts` and `rollups` from Blob.
    /// Truncates Consensus to `truncate_lt`.
    async fn delete_and_truncate<F>(
        truncate_lt: SeqNo,
        batch_parts: &mut BTreeSet<PartialBatchKey>,
        rollups: &mut BTreeSet<PartialRollupKey>,
        machine: &mut Machine<K, V, T, D>,
        timer: &mut F,
    ) where
        F: FnMut(&Counter),
    {
        let shard_id = machine.shard_id();
        let delete_semaphore = Semaphore::new(
            machine
                .applier
                .cfg
                .dynamic
                .gc_blob_delete_concurrency_limit(),
        );

        Self::delete_all(
            machine.applier.state_versions.blob.borrow(),
            batch_parts.iter().map(|k| k.complete(&shard_id)),
            &machine.applier.metrics.retries.external.rollup_delete,
            debug_span!("rollup::delete"),
            &delete_semaphore,
        )
        .await;
        batch_parts.clear();
        timer(&machine.applier.metrics.gc.steps.delete_rollup_seconds);

        Self::delete_all(
            machine.applier.state_versions.blob.borrow(),
            rollups.iter().map(|k| k.complete(&shard_id)),
            &machine.applier.metrics.retries.external.batch_delete,
            debug_span!("batch::delete"),
            &delete_semaphore,
        )
        .await;
        rollups.clear();
        timer(&machine.applier.metrics.gc.steps.delete_batch_part_seconds);

        machine
            .applier
            .state_versions
            .truncate_diffs(&shard_id, truncate_lt)
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

#[derive(Debug, Default)]
pub(crate) struct GcResults {
    pub(crate) batch_parts_deleted_from_blob: usize,
    pub(crate) rollups_deleted_from_blob: usize,
    pub(crate) truncated_consensus_to: Vec<SeqNo>,
    pub(crate) rollups_removed_from_state: Vec<SeqNo>,
}

#[derive(Debug)]
struct GcRollups {
    rollups_lte_seqno_since: Vec<(SeqNo, PartialRollupKey)>,
    rollup_seqnos: HashSet<SeqNo>,
}

impl GcRollups {
    fn new(rollups_lte_seqno_since: Vec<(SeqNo, PartialRollupKey)>, gc_req: &GcReq) -> Self {
        assert!(rollups_lte_seqno_since
            .iter()
            .all(|(seqno, _rollup)| *seqno <= gc_req.new_seqno_since));
        let rollup_seqnos = rollups_lte_seqno_since.iter().map(|(x, _)| *x).collect();
        Self {
            rollups_lte_seqno_since,
            rollup_seqnos,
        }
    }

    fn contains_seqno(&self, seqno: &SeqNo) -> bool {
        self.rollup_seqnos.contains(seqno)
    }

    /// Returns the seqnos we can safely truncate state to when performing
    /// incremental GC (all rollups with seqnos <= seqno_since).
    fn truncate_seqnos(&self) -> impl Iterator<Item = SeqNo> + '_ {
        self.rollups_lte_seqno_since
            .iter()
            .map(|(seqno, _rollup)| *seqno)
    }

    /// Returns the rollups we can safely remove from state (all rollups
    /// `<` than the latest rollup `<=` seqno_since).
    ///
    /// See the full explanation in [crate::internal::state_versions::StateVersions]
    /// for how this is derived.
    fn rollups_to_remove_from_state(&self) -> &[(SeqNo, PartialRollupKey)] {
        match self.rollups_lte_seqno_since.split_last() {
            None => &[],
            Some((_rollup_to_keep, rollups_to_remove_from_state)) => rollups_to_remove_from_state,
        }
    }
}
