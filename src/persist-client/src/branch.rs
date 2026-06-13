// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist primitives for schema branching.
//!
//! A branched table is represented as `(source_shard, branch_ts, delta_shard)`:
//!
//! - `source_shard`: the upstream shard, read frozen at `branch_ts`.
//! - `delta_shard`: a per-branch shard that captures all local writes at
//!   timestamps strictly greater than `branch_ts`.
//!
//! Reads return `consolidate(source.snapshot(branch_ts) + delta.snapshot(T_read))`,
//! which yields snapshot isolation at `branch_ts` with full read/write support
//! on the branch copy.
//!
//! This module provides the persist-side helpers that the storage controller
//! uses to (a) hold back the source shard's `since` at `branch_ts` so that
//! compaction never advances past the branch point, and (b) release that hold
//! when the branch is dropped.
//!
//! Holds are placed via a [`SinceHandle`] keyed on a freshly allocated
//! [`CriticalReaderId`]. The reader id is the durable handle on the hold: the
//! caller is responsible for persisting it (typically in the catalog) so that
//! the hold can be recovered after a process restart and released when the
//! branch is dropped.
//!
//! # Dyncfg gate
//!
//! [`place_read_hold`] and [`recover_read_hold`] check
//! [`crate::cfg::ENABLE_SCHEMA_BRANCHING`] and return
//! [`InvalidUsage::FeatureDisabled`] when it is off. [`release_read_hold`]
//! is intentionally not gated so cleanup of an existing hold can still run
//! after the flag has been turned off.
//!
//! # Why a full `since` hold
//!
//! All we actually need is for the snapshot at `branch_ts` to stay readable
//! while the branch is alive. Persist only exposes that property via the
//! shard-wide `since`: compaction is at the granularity of the shard's
//! `since`, and there's no way to keep just the batches covering
//! `[0, branch_ts)` alive while letting the rest of the shard compact
//! forward. Pinning `since` at `branch_ts` is more than we need (it also
//! blocks compaction of writes in `[branch_ts, now)` on the source), but
//! it's the only mechanism that gives us the property today.
//!
//! Two alternatives if source compaction pressure becomes a problem:
//!
//! 1. Materialize at branch creation. Copy the source's snapshot at
//!    `branch_ts` into a fresh shard at CREATE BRANCH time. The branch then
//!    has no dependency on the source. Costs the copy upfront.
//! 2. Add a "pin these specific batches" primitive to persist that keeps
//!    `[0, branch_ts)` alive independent of `since`. Significant persist
//!    change.
//!
//! For now the full hold is fine. Revisit if branches grow long-lived.

use std::fmt::Debug;

use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::cfg::ENABLE_SCHEMA_BRANCHING;
use crate::critical::{CriticalReaderId, Opaque, SinceHandle};
use crate::error::InvalidUsage;
use crate::{Diagnostics, PersistClient, ShardId};

/// Diagnostics purpose tag callers should use when constructing
/// [`Diagnostics`] for branch read holds. Exposed so that all branch-related
/// holds in the storage controller share a single, greppable tag.
pub const BRANCH_HOLD_PURPOSE: &str = "schema_branching:source_hold";

/// Identifier used in [`InvalidUsage::FeatureDisabled`] when the schema
/// branching dyncfg is off.
const FEATURE_NAME: &str = "schema branching";

/// Place a critical-since hold on `source_shard` at `branch_ts`.
///
/// Allocates a fresh [`CriticalReaderId`], opens a [`SinceHandle`] against
/// `source_shard`, and downgrades its `since` capability to
/// `Antichain::from_elem(branch_ts)`. The returned tuple is
/// `(reader_id, since_handle)`:
///
/// - `reader_id` must be persisted by the caller before the hold is considered
///   durable. Without it, the hold can never be released and the source
///   shard's `since` will be pinned at `branch_ts` indefinitely.
/// - `since_handle` retains the in-process capability and may be used to
///   further manipulate the hold (e.g. to release it via
///   [`release_read_hold`]). Dropping it does not release the hold; see
///   [`SinceHandle`].
///
/// `opaque` is the fencing token to use when downgrading. Callers that do not
/// need fencing may pass the default opaque (e.g. `Opaque::encode(&0u64)`).
///
/// Returns [`InvalidUsage::FeatureDisabled`] when
/// [`ENABLE_SCHEMA_BRANCHING`] is off.
pub async fn place_read_hold<K, V, T, D>(
    client: &PersistClient,
    source_shard: ShardId,
    branch_ts: T,
    opaque: Opaque,
    diagnostics: Diagnostics,
) -> Result<(CriticalReaderId, SinceHandle<K, V, T, D>), InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
{
    if !ENABLE_SCHEMA_BRANCHING.get(client.dyncfgs()) {
        return Err(InvalidUsage::FeatureDisabled(FEATURE_NAME));
    }

    let reader_id = CriticalReaderId::new();
    let mut handle = client
        .open_critical_since::<K, V, T, D>(
            source_shard,
            reader_id.clone(),
            opaque.clone(),
            diagnostics,
        )
        .await?;

    let new_since = Antichain::from_elem(branch_ts);
    // The first downgrade on a freshly registered handle uses the default
    // opaque as both `expected` and `new` fencing tokens. The handle's
    // recorded `since` is updated in place on success.
    let _ = handle
        .compare_and_downgrade_since(&opaque, (&opaque, &new_since))
        .await
        .expect("freshly opened critical handle must accept its own opaque");

    Ok((reader_id, handle))
}

/// Re-open an existing branch read hold previously placed by
/// [`place_read_hold`].
///
/// Used on bootstrap to reconstruct the in-memory [`SinceHandle`] from the
/// `reader_id` durably recorded by the caller. The returned handle's `since`
/// reflects whatever the persist-side state currently is; callers may inspect
/// [`SinceHandle::since`] to confirm the hold is still at the expected
/// `branch_ts`.
///
/// Returns [`InvalidUsage::FeatureDisabled`] when
/// [`ENABLE_SCHEMA_BRANCHING`] is off. Bootstrap that finds existing branch
/// state with the dyncfg off is treated as a configuration error.
pub async fn recover_read_hold<K, V, T, D>(
    client: &PersistClient,
    source_shard: ShardId,
    reader_id: CriticalReaderId,
    opaque: Opaque,
    diagnostics: Diagnostics,
) -> Result<SinceHandle<K, V, T, D>, InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
{
    if !ENABLE_SCHEMA_BRANCHING.get(client.dyncfgs()) {
        return Err(InvalidUsage::FeatureDisabled(FEATURE_NAME));
    }

    client
        .open_critical_since::<K, V, T, D>(source_shard, reader_id, opaque, diagnostics)
        .await
}

/// Release the read hold identified by `reader_id` on `source_shard`.
///
/// Re-opens the existing hold and downgrades its `since` to the empty
/// antichain so compaction can advance past `branch_ts`.
///
/// Idempotent when the opaque still matches: downgrading the empty antichain
/// to the empty antichain is a no-op, so calling this twice in a row is safe.
///
/// If the opaque has drifted (someone else fenced the reader), the downgrade
/// fails with `Err(opaque)`. We treat that as best-effort: log a warning and
/// return `Ok(())`. Worst case the source shard's compaction frontier stays
/// pinned until manual cleanup; correctness is unaffected.
///
/// Not gated on [`ENABLE_SCHEMA_BRANCHING`]: cleanup of existing holds must
/// still work after the flag has been turned off.
pub async fn release_read_hold<K, V, T, D>(
    client: &PersistClient,
    source_shard: ShardId,
    reader_id: CriticalReaderId,
    opaque: Opaque,
    diagnostics: Diagnostics,
) -> Result<(), InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64 + Send + Sync,
{
    let reader_id_for_log = reader_id.clone();
    let mut handle = client
        .open_critical_since::<K, V, T, D>(source_shard, reader_id, opaque.clone(), diagnostics)
        .await?;
    if let Err(current) = handle
        .compare_and_downgrade_since(&opaque, (&opaque, &Antichain::new()))
        .await
    {
        warn!(
            reader_id = %reader_id_for_log,
            shard = %source_shard,
            ?current,
            "branch read hold release: opaque mismatch, hold may have been \
             taken over by another caller; skipping downgrade",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;
    use timely::progress::Antichain;

    use crate::cfg::ENABLE_SCHEMA_BRANCHING;
    use crate::critical::Opaque;
    use crate::error::InvalidUsage;
    use crate::tests::new_test_client;
    use crate::{Diagnostics, ShardId};

    use super::{BRANCH_HOLD_PURPOSE, place_read_hold, recover_read_hold, release_read_hold};

    /// Round-trip: place a hold, confirm `since` is held at `branch_ts`, then
    /// release it and confirm the hold is gone.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn read_hold_round_trip(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        // Enable the persist-side schema branching dyncfg for this test. The
        // primitives refuse to run when it's off; the off-path is covered by
        // [`read_hold_feature_disabled`].
        client.cfg.set_config(&ENABLE_SCHEMA_BRANCHING, true);

        let source_shard = ShardId::new();
        let branch_ts: u64 = 10;
        let opaque = Opaque::encode(&0u64);

        // Initialize the shard by opening a normal read/write pair, so the
        // shard exists before we try to place a hold on it.
        let (_write, _read) = client
            .expect_open::<String, String, u64, i64>(source_shard)
            .await;

        // Place the hold.
        let (reader_id, hold) = place_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            branch_ts,
            opaque.clone(),
            Diagnostics {
                shard_name: "source".to_string(),
                handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
            },
        )
        .await
        .expect("place_read_hold");

        assert_eq!(
            hold.since(),
            &Antichain::from_elem(branch_ts),
            "since must be held at branch_ts"
        );

        // Drop the in-process handle. Because [`SinceHandle`] does not release
        // its capability on drop, the persist-side hold must survive.
        drop(hold);

        // Recover the hold from `reader_id`.
        let recovered = recover_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            reader_id.clone(),
            opaque.clone(),
            Diagnostics {
                shard_name: "source".to_string(),
                handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
            },
        )
        .await
        .expect("recover_read_hold");

        assert_eq!(
            recovered.since(),
            &Antichain::from_elem(branch_ts),
            "recovered since must still be branch_ts"
        );

        drop(recovered);

        // Release the hold.
        release_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            reader_id.clone(),
            opaque.clone(),
            Diagnostics {
                shard_name: "source".to_string(),
                handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
            },
        )
        .await
        .expect("release_read_hold");

        // Calling `release_read_hold` again on an already-released hold must
        // be a no-op (idempotency contract documented on the function).
        release_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            reader_id.clone(),
            opaque.clone(),
            Diagnostics {
                shard_name: "source".to_string(),
                handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
            },
        )
        .await
        .expect("second release_read_hold must succeed (idempotent)");

        // After release(s) the recorded since must be the empty antichain.
        let after_release = recover_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            reader_id,
            opaque,
            Diagnostics {
                shard_name: "source".to_string(),
                handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
            },
        )
        .await
        .expect("recover after release");

        assert_eq!(
            after_release.since(),
            &Antichain::new(),
            "since must be released to the empty antichain"
        );
    }

    /// With the dyncfg off (the default), `place_read_hold` and
    /// `recover_read_hold` must refuse to run; `release_read_hold` is not
    /// gated.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn read_hold_feature_disabled(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        // Do NOT enable ENABLE_SCHEMA_BRANCHING. Default is false.

        let source_shard = ShardId::new();
        let branch_ts: u64 = 10;
        let opaque = Opaque::encode(&0u64);
        let diagnostics = Diagnostics {
            shard_name: "source".to_string(),
            handle_purpose: BRANCH_HOLD_PURPOSE.to_string(),
        };

        let place_err = place_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            branch_ts,
            opaque.clone(),
            diagnostics.clone(),
        )
        .await
        .expect_err("place_read_hold must refuse when the dyncfg is off");
        assert!(
            matches!(place_err, InvalidUsage::FeatureDisabled(_)),
            "expected FeatureDisabled, got {place_err:?}"
        );

        // recover_read_hold takes a fresh reader id since no hold exists; the
        // dyncfg check fires before persist ever looks at the shard.
        let recover_err = recover_read_hold::<String, String, u64, i64>(
            &client,
            source_shard,
            crate::critical::CriticalReaderId::new(),
            opaque,
            diagnostics,
        )
        .await
        .expect_err("recover_read_hold must refuse when the dyncfg is off");
        assert!(
            matches!(recover_err, InvalidUsage::FeatureDisabled(_)),
            "expected FeatureDisabled, got {recover_err:?}"
        );
    }
}
