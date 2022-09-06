// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, debug_span, warn, Instrument, Span};

use crate::internal::machine::{retry_external, Machine};
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::ShardId;

#[derive(Debug, Clone)]
pub struct GcReq {
    pub shard_id: ShardId,
    pub old_seqno_since: SeqNo,
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
///   loop, accumulating a HashSet of every referenced blob key. When it finds
///   the version corresponding to the new_seqno_since, it removes every blob in
///   that version of the state from the HashSet and exits the loop. This
///   results in the HashSet containing every blob eligible for deletion. It
///   deletes those blobs and then truncates the state to the new_seqno_since to
///   indicate that this work doesn't need to be done again.
/// - Note that these requests are being processed concurrently, so it's always
///   possible that some future request has already deleted the blobs and
///   truncated consensus. It's also possible that this is the future request.
///   As a result, the only guarantee that we get is that the current version of
///   head is >= new_seqno_since >= old_seqno_since.
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
                    consolidated_req.old_seqno_since =
                        std::cmp::min(req.old_seqno_since, consolidated_req.old_seqno_since);
                    consolidated_req.new_seqno_since =
                        std::cmp::max(req.new_seqno_since, consolidated_req.new_seqno_since);
                }

                let merged_requests = gc_completed_senders.len() - 1;
                if merged_requests > 0 {
                    debug!(
                        "Merged {} gc requests together for shard {}",
                        merged_requests, consolidated_req.shard_id
                    );
                }

                let gc_span = debug_span!(parent: None, "gc_and_truncate", shard_id=%consolidated_req.shard_id);
                gc_span.follows_from(&Span::current());

                let start = Instant::now();
                machine.metrics.gc.started.inc();
                Self::gc_and_truncate(&mut machine, consolidated_req)
                    .instrument(gc_span)
                    .await;
                machine.metrics.gc.finished.inc();
                machine
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
        assert_eq!(req.shard_id, machine.shard_id());
        // NB: Because these requests can be processed concurrently (and in
        // arbitrary order), all of the logic below has to work even if we've
        // already gc'd and truncated past new_seqno_since.

        let mut states = machine
            .state_versions
            .fetch_live_states::<K, V, T, D>(&req.shard_id)
            .await
            .expect("shard codecs should not change");

        debug!(
            "gc {} for [{},{}) got {} versions from scan",
            req.shard_id,
            req.old_seqno_since,
            req.new_seqno_since,
            states.len()
        );

        // Fast-path: Someone already GC'd past `req.new_seqno_since`, don't
        // bother running any of the below logic.
        //
        // Also a fix for #14580.
        if states
            .peek_seqno()
            .map_or(true, |x| x > req.new_seqno_since)
        {
            return;
        }

        let mut deleteable_batch_blobs = HashSet::new();
        let mut deleteable_rollup_blobs = Vec::new();
        while let Some(state) = states.next() {
            if state.seqno < req.new_seqno_since {
                state.collections.trace.map_batches(|b| {
                    for part in b.parts.iter() {
                        // It's okay (expected) if the key already exists in
                        // deleteable_batch_blobs, it may have been present in
                        // previous versions of state.
                        deleteable_batch_blobs.insert(part.key.to_owned());
                    }
                });
            } else if state.seqno == req.new_seqno_since {
                state.collections.trace.map_batches(|b| {
                    for part in b.parts.iter() {
                        // It's okay (expected) if the key doesn't exist in
                        // deleteable_batch_blobs, it may have been added in
                        // this version of state.
                        let _ = deleteable_batch_blobs.remove(&part.key);
                    }
                });
                // We only need to detect deletable rollups in the last iter
                // through the live_diffs loop because they accumulate in state.
                for (seqno, key) in state.collections.rollups.iter() {
                    // SUBTLE: This is intentionally comparing vs
                    // old_seqno_since, not new_seqno_since. We could collect
                    // all rollups less than new_seqno_since, and then remove
                    // them from state _after the truncate_, but the state
                    // change to add the new rollups has to be before the
                    // truncate and we don't want to create 2 state changes per
                    // call into gc.
                    if seqno < &req.old_seqno_since {
                        deleteable_rollup_blobs.push((*seqno, key.clone()));
                    } else {
                        // We iterate in order, may as well short circuit the
                        // rollup loop.
                        break;
                    }
                }
                break;
            } else {
                // Sanity check the loop logic.
                assert!(state.seqno > req.new_seqno_since);
                break;
            }
        }
        let state = states.into_inner();

        // As described in the big rustdoc comment on [StateVersions], we
        // maintain the invariant that there is always a rollup corresponding to
        // the seqno of the first live version in consensus. So, write a new
        // rollup at exactly req.new_seqno_since so we're free to truncate
        // anything before it.
        //
        // NB: We write rollups periodically (via maintenance) to cover the case
        // when GC is being held up by a long seqno hold (such as the 15m read
        // lease timeouts whenever environmentd restarts).
        assert_eq!(state.seqno, req.new_seqno_since);
        let rollup_seqno = state.seqno;
        let rollup_key = PartialRollupKey::new(rollup_seqno, &RollupId::new());
        let () = machine
            .state_versions
            .write_rollup_blob(&machine.shard_metrics, &state, &rollup_key)
            .await;
        let applied = machine
            .add_and_remove_rollups((rollup_seqno, &rollup_key), &deleteable_rollup_blobs)
            .await;
        // We raced with some other GC process to write this rollup out. Ours
        // wasn't registered, so delete it.
        if !applied {
            machine
                .state_versions
                .delete_rollup(&state.shard_id, &rollup_key)
                .await;
        }

        // There's also a bulk delete API in s3 if the performance of this
        // becomes an issue. Maybe make Blob::delete take a list of keys?
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        //
        // Another idea is to use a FuturesUnordered to at least run them
        // concurrently, but this requires a bunch of Arc cloning, so wait to
        // see if it's worth it.
        for key in deleteable_batch_blobs {
            retry_external(&machine.metrics.retries.external.batch_delete, || async {
                machine
                    .state_versions
                    .blob
                    .delete(&key.complete(&req.shard_id))
                    .await
            })
            .instrument(debug_span!("batch::delete"))
            .await;
        }
        for (_, key) in deleteable_rollup_blobs {
            machine
                .state_versions
                .delete_rollup(&req.shard_id, &key)
                .await;
        }

        // Now that we've deleted the eligible blobs, "commit" this info by
        // truncating the state versions that referenced them.
        machine
            .state_versions
            .truncate_diffs(&req.shard_id, req.new_seqno_since)
            .await;
    }
}
