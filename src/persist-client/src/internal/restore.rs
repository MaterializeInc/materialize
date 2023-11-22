// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! See documentation on [[restore_blob]].

use crate::internal::encoding::UntypedState;
use crate::internal::paths::BlobKey;
use crate::internal::state::State;
use crate::internal::state_diff::{StateDiff, StateFieldValDiff};
use crate::internal::state_versions::StateVersions;
use crate::ShardId;
use anyhow::anyhow;
use mz_persist::location::Blob;
use tracing::info;

/// Attempt to restore all the blobs referenced by the current state in consensus.
/// Returns a list of blobs that were not possible to restore.
pub(crate) async fn restore_blob(
    versions: &StateVersions,
    blob: &(dyn Blob + Send + Sync),
    build_version: &semver::Version,
    shard_id: ShardId,
) -> anyhow::Result<Vec<BlobKey>> {
    let diffs = versions.fetch_all_live_diffs(&shard_id).await;
    let Some(first_live_seqno) = diffs.0.first().map(|d| d.seqno) else {
        info!("No diffs for shard {shard_id}.");
        return Ok(vec![]);
    };

    fn after<A>(diff: StateFieldValDiff<A>) -> Option<A> {
        match diff {
            StateFieldValDiff::Insert(a) => Some(a),
            StateFieldValDiff::Update(_, a) => Some(a),
            StateFieldValDiff::Delete(_) => None,
        }
    }

    let mut not_restored = vec![];
    let mut check_restored = |key: &BlobKey, result: Result<(), _>| {
        if result.is_err() {
            not_restored.push(key.clone());
        }
    };

    for diff in diffs.0 {
        let diff: StateDiff<u64> = StateDiff::decode(build_version, diff.data);
        for rollup in diff.rollups {
            // We never actually reference rollups from before the first live diff.
            if rollup.key < first_live_seqno {
                continue;
            }
            let Some(value) = after(rollup.val) else {
                continue;
            };
            let key = value.key.complete(&shard_id);
            let rollup_result = blob.restore(&key).await;
            let rollup_restored = rollup_result.is_ok();
            check_restored(&key, rollup_result);

            // Elsewhere, we restore any state referenced in live diffs... but we also
            // need to restore everything referenced in that first rollup.
            // If restoring the rollup failed, let's
            // keep going to try and recover the rest of the blobs before bailing out.
            if rollup.key != first_live_seqno || !rollup_restored {
                continue;
            }
            let rollup_bytes = blob
                .get(&key)
                .await?
                .ok_or_else(|| anyhow!("fetching just-restored rollup"))?;
            let rollup_state: State<u64> =
                UntypedState::decode(build_version, rollup_bytes).check_ts_codec(&shard_id)?;
            for (seqno, rollup) in &rollup_state.collections.rollups {
                // We never actually reference rollups from before the first live diff.
                if *seqno < first_live_seqno {
                    continue;
                }
                let key = rollup.key.complete(&shard_id);
                check_restored(&key, blob.restore(&key).await);
            }
            for batch in rollup_state.collections.trace.batches() {
                for part in &batch.parts {
                    let key = part.key.complete(&shard_id);
                    check_restored(&key, blob.restore(&key).await);
                }
            }
        }
        for batch in diff.spine {
            if let Some(_) = after(batch.val) {
                for part in batch.key.parts {
                    let key = part.key.complete(&shard_id);
                    check_restored(&key, blob.restore(&key).await);
                }
            }
        }
    }
    Ok(not_restored)
}
