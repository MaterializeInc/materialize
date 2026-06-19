// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Primitives for forking a schema's persist shards at a coordinated point in
//! time.
//!
//! [`pick_branch_ts`] picks a single timestamp that is dominated by every
//! shard's upper across a set, so a snapshot at that timestamp is observable
//! on every input. [`fork_shard`] turns one source shard into a new shard
//! whose initial manifest references the source's existing blobs by absolute
//! key, stamps every inherited batch with a `cutoff_ts`, and reports back the
//! list of absolute blob keys so the caller can pin them against persist GC.

use std::sync::Arc;

use mz_persist_client::ShardId;
use mz_repr::Timestamp;
use timely::PartialOrder;
use timely::progress::Antichain;

pub mod commit;
pub mod ddl_freeze;
pub mod drop;
pub mod fork_shard;

/// A timestamp that is dominated by every shard's upper in `uppers`.
///
/// The returned timestamp `t` satisfies `t < min(upper(s))` for every shard
/// in the set, which is the contract a coordinated branch needs: a snapshot
/// at `t` is observable from every input.
///
/// If `uppers` is empty the function returns `T::minimum()` so callers don't
/// have to special-case it. If any upper is the empty antichain (the shard
/// has been finalized) the function returns `None`.
pub fn pick_branch_ts(uppers: &[Antichain<Timestamp>]) -> Option<Timestamp> {
    if uppers.is_empty() {
        return Some(Timestamp::MIN);
    }
    // The greatest lower bound is the meet over all the uppers. With a
    // totally ordered timestamp (which `mz_repr::Timestamp` is) the meet is
    // just the elementwise minimum.
    let mut meet: Option<Timestamp> = None;
    for upper in uppers {
        let Some(value) = upper.as_option().copied() else {
            // An empty upper means the shard has been advanced to infinity
            // (finalized); we can't pick a branch_ts that's strictly below
            // it because there is no such timestamp.
            return None;
        };
        meet = Some(match meet {
            None => value,
            Some(prev) => std::cmp::min(prev, value),
        });
    }
    // `branch_ts` should be strictly less than every upper so that
    // `branch_ts + 1 <= upper` holds; saturate at zero rather than wrap.
    meet.map(|t| t.saturating_sub(1))
}

pub use mz_persist_client::fork::{ForkShardError, ForkShardOutput};

/// Fork `source_shard` at `branch_ts`. Delegates to the persist primitive;
/// the coordinator passes its `PersistClient` and the source object's
/// schemas. The returned `absolute_blob_keys` is what the caller pins into
/// the shared `fork_blob_refs` table to keep persist from reclaiming the
/// source's blobs while the fork is live.
pub async fn fork_shard(
    persist: &mz_persist_client::PersistClient,
    source_shard: ShardId,
    branch_ts: Timestamp,
    diagnostics: mz_persist_client::Diagnostics,
    key_schema: Arc<mz_persist_types::codec_impls::StringSchema>,
    val_schema: Arc<mz_persist_types::codec_impls::StringSchema>,
) -> Result<ForkShardOutput, ForkShardError> {
    persist
        .fork_shard::<String, String, Timestamp, i64>(
            source_shard,
            branch_ts,
            diagnostics,
            key_schema,
            val_schema,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::progress::Antichain;

    fn upper(value: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::new(value))
    }

    #[mz_ore::test]
    fn pick_branch_ts_empty_set_returns_minimum() {
        assert_eq!(pick_branch_ts(&[]), Some(Timestamp::MIN));
    }

    #[mz_ore::test]
    fn pick_branch_ts_single_shard() {
        let t = pick_branch_ts(&[upper(10)]).expect("non-empty upper");
        assert!(t < Timestamp::new(10));
    }

    #[mz_ore::test]
    fn pick_branch_ts_picks_min_upper_minus_one() {
        let t = pick_branch_ts(&[upper(10), upper(7), upper(20)]).expect("non-empty");
        // 7 is the smallest upper; branch_ts must be < 7.
        assert!(t < Timestamp::new(7));
        // Specifically: one below 7.
        assert_eq!(t, Timestamp::new(6));
    }

    #[mz_ore::test]
    fn pick_branch_ts_finalized_shard_returns_none() {
        // An empty antichain means the shard has been advanced to infinity.
        let finalized = Antichain::new();
        let result = pick_branch_ts(&[upper(10), finalized]);
        assert!(result.is_none());
    }

    #[mz_ore::test]
    fn pick_branch_ts_saturates_at_zero() {
        // If the smallest upper is `Timestamp::MIN`, there is no `t` strictly
        // less than it. Saturating subtraction keeps us at `Timestamp::MIN`,
        // which is a defensible default. Tests downstream of this function
        // should treat `MIN` as a sentinel for "no useful branch_ts".
        let t = pick_branch_ts(&[Antichain::from_elem(Timestamp::MIN)]).expect("min upper");
        assert_eq!(t, Timestamp::MIN);
    }

}
