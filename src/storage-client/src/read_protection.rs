// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Application of catalog-authorized Persist compaction permission.

use mz_persist_client::critical::{Opaque, SinceHandle};
use mz_persist_client::error::InvalidUsage;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_repr::Timestamp;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::controller::PersistEpoch;

/// The shared controller critical reader, not a process-owned read lease.
pub type CriticalSinceHandle = SinceHandle<SourceData, (), Timestamp, StorageDiff>;

/// Opens the controller's critical reader without acquiring an epoch or advancing
/// its permission. Only use in catalog-protected environments. The caller must
/// supply committed, shard-wide permission to [`downgrade_since`].
pub async fn open_critical_handle(
    persist: &PersistClient,
    shard: ShardId,
    diagnostics: Diagnostics,
) -> Result<CriticalSinceHandle, InvalidUsage<Timestamp>> {
    persist
        .open_critical_since(
            shard,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            Opaque::encode(&PersistEpoch::default()),
            diagnostics,
        )
        .await
}

/// Makes one monotone attempt to apply committed shard-wide permission, preserving
/// the opaque rather than taking ownership. `None` (rate limiting) and `Err`
/// (opaque contention) require a later retry. Contention refreshes the handle's
/// opaque and since. An already applied or stale target needs no Persist write.
/// Empty permission bypasses rate limiting, but does not authorize finalization.
pub async fn downgrade_since(
    handle: &mut CriticalSinceHandle,
    bound: &Antichain<Timestamp>,
) -> Option<Result<Antichain<Timestamp>, Opaque>> {
    if PartialOrder::less_equal(bound, handle.since()) {
        return Some(Ok(handle.since().clone()));
    }
    let opaque = handle.opaque().clone();
    if bound.is_empty() {
        Some(
            handle
                .compare_and_downgrade_since(&opaque, (&opaque, bound))
                .await,
        )
    } else {
        handle
            .maybe_compare_and_downgrade_since(&opaque, (&opaque, bound))
            .await
    }
}
