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

/// Maximum number of states that get GC'd together in a single pass
const GC_BATCH_SIZE: usize = 20;

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

    pub fn gc_and_truncate_background(&self, req: GcReq) -> JoinHandle<()> {
        // Spawn GC in a background task, so the state change that triggered it
        // isn't blocked on it. Note that unlike compaction, GC maintenance
        // intentionally has no "skipped" heuristic.
        let gc_span = debug_span!(parent: None, "gc_and_truncate", shard_id=%req.shard_id);
        gc_span.follows_from(&Span::current());

        let consensus = Arc::clone(&self.consensus);
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        mz_ore::task::spawn(
            || "persist::gc_and_truncate",
            async move {
                let start = Instant::now();
                metrics.gc.started.inc();
                Self::gc_and_truncate(consensus, blob, &metrics, req).await;
                metrics.gc.finished.inc();
                metrics.gc.seconds.inc_by(start.elapsed().as_secs_f64());
            }
            .instrument(gc_span),
        )
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

        // pull down the state for new_seqno_since, which contains the blobs we must keep.
        let new_seqno_data = retry_external(&metrics.retries.external.gc_scan, || async {
            let mut new_seqno_data = consensus.scan(&path, req.new_seqno_since, 1).await?;
            if new_seqno_data.len() != 1 {
                // this is fine, someone else beat us to gc'ing through this seqno
                Ok(None)
            } else {
                Ok(Some(new_seqno_data.swap_remove(0)))
            }
        })
        .await;

        let new_seqno_data = match new_seqno_data {
            Some(data) => data,
            None => return,
        };
        assert_eq!(req.new_seqno_since, new_seqno_data.seqno);

        let mut required_blobs = HashSet::new();
        Self::for_all_keys(&new_seqno_data, |key| {
            let _ = required_blobs.insert(key.to_owned());
        });

        let mut gc_seqno_progress = SeqNo::minimum();
        while gc_seqno_progress < new_seqno_data.seqno {
            let state_versions = retry_external(&metrics.retries.external.gc_scan, || async {
                consensus
                    .scan(&path, gc_seqno_progress, GC_BATCH_SIZE)
                    .await
            })
            .await;

            debug!(
                "gc {} for [{},{}) got {} versions from scan",
                req.shard_id,
                req.old_seqno_since,
                req.new_seqno_since,
                state_versions.len()
            );

            let mut deleteable_blobs = HashSet::new();
            for state_version in state_versions {
                if state_version.seqno < req.new_seqno_since {
                    Self::for_all_keys(&state_version, |key| {
                        if !required_blobs.contains(key) {
                            deleteable_blobs.insert(key.to_owned());
                        }
                    });
                    gc_seqno_progress = state_version.seqno;
                } else {
                    // Sanity check the loop logic.
                    assert!(state_version.seqno >= req.new_seqno_since);
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

            // truncation is exclusive and scan is inclusive on `seqno`.
            // we've observed up to `gc_seqno_progress` from this last scan,
            // so we can increment seqno to correctly truncate everything
            // we've seen so far and to scan the next page if needed
            gc_seqno_progress = gc_seqno_progress.next();

            // Now that we've deleted the eligible blobs, "commit" this info by
            // truncating the state versions that referenced them.
            let _deleted_count = retry_external(&metrics.retries.external.gc_truncate, || async {
                consensus.truncate(&path, gc_seqno_progress).await
            })
            .instrument(debug_span!("gc::truncate"))
            .await;
        }
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
