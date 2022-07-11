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
use tracing::{debug_span, Instrument, Span};

use crate::r#impl::machine::{retry_external, FOREVER};
use crate::r#impl::state::ProtoStateRollup;
use crate::{Metrics, ShardId};

#[derive(Debug, Clone)]
pub struct GcReq {
    pub shard_id: ShardId,
    pub seqno: SeqNo,
}

#[derive(Debug, Clone)]
pub struct GarbageCollector {
    consensus: Arc<dyn Consensus + Send + Sync>,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

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

    pub fn gc_and_truncate(&self, req: GcReq) {
        // WIP arbitrary-ish threshold that scales with seqno, but never gets
        // particularly big.
        let gc_threshold = req.seqno.0.next_power_of_two().trailing_zeros();

        // WIP this doesn't work. if we have more writes than reader seqno
        // downgrades (presumably the common case) we're unlikely to hit this
        // criteria. I think we need to explictly keep track of which seqnos are
        // still alive.
        #[allow(unused_assignments)]
        let mut should_gc = req.seqno.0 % u64::from(gc_threshold) == 0;
        should_gc = true;
        if !should_gc {
            return;
        }

        // Spawn GC in a background task, so the read that triggered it isn't
        // blocked on it.
        let gc_span = debug_span!(parent: None, "persist::gc", shard_id=%req.shard_id);
        gc_span.follows_from(&Span::current());

        let consensus = Arc::clone(&self.consensus);
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        eprintln!("WIP spawning gc for {}", req.seqno,);
        let _ = mz_ore::task::spawn(
            || "persist::gc",
            async move { Self::gc(consensus, blob, metrics, req).await }.instrument(gc_span),
        );
    }

    pub async fn gc(
        consensus: Arc<dyn Consensus + Send + Sync>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
        req: GcReq,
    ) {
        eprintln!("WIP in gc task {}", req.seqno);
        let deadline = Instant::now() + FOREVER;
        let path = req.shard_id.to_string();
        let state_versions = consensus
            .scan(deadline, &path, SeqNo::minimum())
            .await
            .expect("WIP retry_external");

        eprintln!(
            "WIP gc for {} scanned {} versions",
            req.seqno,
            state_versions.len()
        );

        let mut deleteable_blobs = HashSet::new();
        for state_version in state_versions {
            if state_version.seqno < req.seqno {
                Self::for_all_keys(&state_version, |key| {
                    // It's okay (expected) if the key already exists in
                    // deleteable_blobs, it may have been present in previous
                    // versions of state.
                    deleteable_blobs.insert(key.to_owned());
                });
            } else if state_version.seqno == req.seqno {
                Self::for_all_keys(&state_version, |key| {
                    // It's okay (expected) if the key doesn't exist in
                    // deleteable_blobs, it may have been added in this version
                    // of state.
                    let _ = deleteable_blobs.remove(key);
                });
            } else {
                // Sanity check the loop logic.
                assert!(state_version.seqno > req.seqno);
                break;
            }
        }

        for key in deleteable_blobs {
            blob.delete(deadline, &key)
                .await
                .expect("WIP retry_external");
        }

        // Now that we've deleted the eligible blobs, "commit" this info by
        // truncating the state versions that referenced them.
        let () = retry_external(
            // WIP rename this metric
            &metrics.retries.external.apply_unbatched_cmd_truncate,
            || async {
                consensus
                    .truncate(Instant::now() + FOREVER, &path, req.seqno)
                    .await
            },
        )
        .instrument(debug_span!("gc::truncate"))
        .await;
    }

    fn for_all_keys<F: FnMut(&str)>(data: &VersionedData, mut f: F) {
        let state = ProtoStateRollup::decode(&*data.data).expect("WIP");
        if let Some(trace) = state.trace.as_ref() {
            for batch in trace.spine.iter() {
                for key in batch.keys.iter() {
                    f(key)
                }
            }
        }
    }
}
