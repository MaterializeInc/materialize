// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_persist::location::{Blob, Consensus, SeqNo, VersionedData};
use prost::Message;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, Instrument, Span};

use crate::r#impl::machine::retry_external;
use crate::r#impl::paths::PartialBlobKey;
use crate::r#impl::state::ProtoStateRollup;
use crate::{Metrics, ShardId};

#[derive(Debug, Clone)]
pub struct GcReq {
    pub shard_id: ShardId,
    pub old_seqno_since: SeqNo,
    pub new_seqno_since: SeqNo,
}

#[derive(Debug, Clone)]
pub struct GarbageCollector {
    consensus: Arc<dyn Consensus + Send + Sync>,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
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
impl GarbageCollector {
    pub fn new(
        consensus: Arc<dyn Consensus + Send + Sync>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Self {
        GarbageCollector {
            consensus,
            blob,
            metrics,
        }
    }

    pub fn gc_and_truncate_background(&self, req: GcReq) -> Option<JoinHandle<()>> {
        // This is an arbitrary-ish threshold that scales with seqno, but never
        // gets particularly big. It probably could be much bigger and certainly
        // could use a tuning pass at some point.
        let gc_threshold = std::cmp::max(
            1,
            u64::from(req.new_seqno_since.0.next_power_of_two().trailing_zeros()),
        );

        // We don't have to be super prompt about GC, only run it when the
        // seqno_since crosses some threshold.
        let should_gc =
            req.new_seqno_since.0 / gc_threshold != req.old_seqno_since.0 / gc_threshold;
        if !should_gc {
            self.metrics.gc.skipped.inc();
            return None;
        }

        // Spawn GC in a background task, so the state change that triggered it
        // isn't blocked on it.
        let gc_span = debug_span!(parent: None, "gc_and_truncate", shard_id=%req.shard_id);
        gc_span.follows_from(&Span::current());

        let consensus = Arc::clone(&self.consensus);
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        Some(mz_ore::task::spawn(
            || "persist::gc_and_truncate",
            async move {
                let start = Instant::now();
                metrics.gc.started.inc();
                Self::gc_and_truncate(consensus, blob, &metrics, req).await;
                metrics.gc.finished.inc();
                metrics.gc.seconds.inc_by(start.elapsed().as_secs_f64());
            }
            .instrument(gc_span),
        ))
    }

    pub async fn gc_and_truncate(
        consensus: Arc<dyn Consensus + Send + Sync>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: &Metrics,
        req: GcReq,
    ) {
        // NB: Because these requests can be processed concurrently (and in
        // arbitrary order), all of the logic below has to work even if we've
        // already gc'd and truncated past new_seqno_since.

        let path = req.shard_id.to_string();
        let state_versions = retry_external(&metrics.retries.external.gc_scan, || async {
            consensus.scan(&path, SeqNo::minimum()).await
        })
        .await;

        debug!(
            "gc {} for [{},{}) got {} versions from scan",
            req.shard_id,
            req.old_seqno_since,
            req.new_seqno_since,
            state_versions.len()
        );

        // It'd be minor-ly more efficient to reverse the order and first build
        // up non_deleteable_blobs, and then check all state_versions with seqno
        // < new_seqno_since so our hashmap grows to O(non_deleteable) rather
        // than O(deleteable+non_deleteable). This version of the code reads
        // slightly more obviously, so hold off on that until/if we see it be a
        // problem in practice.
        let mut deleteable_blobs = HashSet::new();
        for state_version in state_versions {
            if state_version.seqno < req.new_seqno_since {
                Self::for_all_keys(&state_version, |key| {
                    // It's okay (expected) if the key already exists in
                    // deleteable_blobs, it may have been present in previous
                    // versions of state.
                    deleteable_blobs.insert(key.to_owned());
                });
            } else if state_version.seqno == req.new_seqno_since {
                Self::for_all_keys(&state_version, |key| {
                    // It's okay (expected) if the key doesn't exist in
                    // deleteable_blobs, it may have been added in this version
                    // of state.
                    let _ = deleteable_blobs.remove(key);
                });
            } else {
                // Sanity check the loop logic.
                assert!(state_version.seqno > req.new_seqno_since);
                break;
            }
        }

        // There's also a bulk delete API in s3 if the performance of this
        // becomes an issue. Maybe make Blob::delete take a list of keys?
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        //
        // Another idea is to use a FuturesUnordered to at least run them
        // concurrently, but this requires a bunch of Arc cloning, so wait to
        // see if it's worth it.
        for key in deleteable_blobs {
            let key = PartialBlobKey(key).complete(&req.shard_id);
            retry_external(&metrics.retries.external.gc_delete, || async {
                blob.delete(&key).await
            })
            .instrument(debug_span!("gc::delete"))
            .await;
        }

        // Now that we've deleted the eligible blobs, "commit" this info by
        // truncating the state versions that referenced them.
        let _deleted_count = retry_external(&metrics.retries.external.gc_truncate, || async {
            consensus.truncate(&path, req.new_seqno_since).await
        })
        .instrument(debug_span!("gc::truncate"))
        .await;
    }

    fn for_all_keys<F: FnMut(&str)>(data: &VersionedData, mut f: F) {
        let state = ProtoStateRollup::decode(&*data.data)
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        if let Some(trace) = state.trace.as_ref() {
            for batch in trace.spine.iter() {
                for key in batch.keys.iter() {
                    f(key)
                }
            }
        }
    }
}
