// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist appends shared by introspection producers.

use mz_persist_client::write::WriteHandle;
use mz_repr::Timestamp;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;

use crate::client::TimestamplessUpdate;

/// Appends updates at a writable timestamp no earlier than `at_least`.
///
/// Definitive upper conflicts retry at the observed upper. These must be blind
/// updates, not retractions conditioned on a snapshot of collection contents.
/// Event timestamps encoded in rows are unaffected by append timestamp selection.
///
/// Empty updates to closed collections are ignored. Nonempty updates to closed
/// collections, or invalid Persist usage, panic.
pub async fn monotonic_append(
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    updates: Vec<TimestamplessUpdate>,
    at_least: Timestamp,
) {
    let mut expected_upper = write_handle.shared_upper();
    loop {
        if updates.is_empty() && expected_upper.is_empty() {
            return;
        }

        let upper = expected_upper
            .into_option()
            .expect("cannot append data to closed collection");

        let lower = std::cmp::max(upper, at_least);
        let new_upper = lower.step_forward();
        let updates = updates
            .iter()
            .map(|TimestamplessUpdate { row, diff }| {
                ((SourceData(Ok(row.clone())), ()), lower, diff.into_inner())
            })
            .collect::<Vec<_>>();
        let res = write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(new_upper),
            )
            .await
            .expect("valid usage");
        match res {
            Ok(()) => return,
            Err(err) => {
                expected_upper = err.current;
                continue;
            }
        }
    }
}
