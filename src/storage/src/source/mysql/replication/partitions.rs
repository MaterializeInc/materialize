// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code related to tracking the frontier of GTID partitions for a MySQL source.

use std::collections::BTreeMap;

use timely::progress::Antichain;
use uuid::Uuid;

use mz_storage_types::sources::mysql::{GtidPartition, GtidState};

use super::super::DefiniteError;

/// Holds the active and future GTID partitions that represent the complete
/// UUID range of all possible GTID source-ids from a MySQL server.
/// The active partitions are all singleton partitions representing a single
/// source-id timestamp, and the future partitions represent the missing
/// UUID ranges that we have not yet seen and are held at timestamp GtidState::Absent.
///
/// This is used to keep track of all partitions and is updated as we receive
/// new GTID updates from the server, and is used to create a full 'frontier'
/// representing the current state of all GTID partitions that we can use
/// to downgrade capabilities for the source.
///
/// We could instead mint capabilities for each individual partition
/// and advance each partition as a frontier separately using its own capabilities,
/// but since this is used inside a fallible operator if we ever hit any errors
/// then all newly minted capabilities would be dropped by the async runtime
/// and we might lose the ability to send any more data.
/// However if we just use the main capabilities provided to the operator, the
/// capabilities externally will be preserved even in the case of an error in
/// the operator, which is why we just manage a single frontier and capability set.
pub(super) struct GtidReplicationPartitions {
    active: BTreeMap<Uuid, GtidPartition>,
    future: Vec<GtidPartition>,
}

impl From<Antichain<GtidPartition>> for GtidReplicationPartitions {
    fn from(frontier: Antichain<GtidPartition>) -> Self {
        let mut active = BTreeMap::new();
        let mut future = Vec::new();
        for part in frontier.iter() {
            if part.timestamp() == &GtidState::Absent {
                future.push(part.clone());
            } else {
                let source_id = part.interval().singleton().unwrap().clone();
                active.insert(source_id, part.clone());
            }
        }
        Self { active, future }
    }
}

impl GtidReplicationPartitions {
    /// Return an Antichain for the frontier composed of all the
    /// active and future GTID partitions.
    pub(super) fn frontier(&self) -> Antichain<GtidPartition> {
        Antichain::from_iter(
            self.active
                .values()
                .cloned()
                .chain(self.future.iter().cloned()),
        )
    }

    /// Given a singleton GTID partition, update the timestamp of the existing
    /// active partition with the same UUID
    /// or split the future partitions to remove this new partition and then
    /// insert the new 'active' partition.
    ///
    /// This is used whenever we receive a GTID update from the server and
    /// need to update our state keeping track
    /// of all the active and future GTID partition timestamps.
    /// This call should usually be followed up by downgrading capabilities
    /// using the frontier returned by `self.frontier()`
    pub(super) fn advance_frontier(
        &mut self,
        new_part: GtidPartition,
    ) -> Result<(), DefiniteError> {
        let source_id = new_part.interval().singleton().unwrap();
        // Check if we have an active partition for the GTID UUID
        match self.active.get_mut(source_id) {
            Some(active_part) => {
                // Since we start replication at a specific upper, we
                // should only see GTID transaction-ids
                // in a monotonic order for each source, starting at that upper.
                if active_part.timestamp() > new_part.timestamp() {
                    let err = DefiniteError::BinlogGtidMonotonicityViolation(
                        source_id.to_string(),
                        new_part.timestamp().clone(),
                    );
                    return Err(err);
                }

                // replace this active partition with the new one
                *active_part = new_part;
            }
            // We've received a GTID for a UUID we don't yet know about
            None => {
                // Extract the future partition whose range encompasses this UUID
                // TODO: Replace with Vec::extract_if() once it's stabilized
                let mut i = 0;
                let mut contained_part = None;
                while i < self.future.len() {
                    if self.future[i].interval().contains(source_id) {
                        contained_part = Some(self.future.remove(i));
                        break;
                    } else {
                        i += 1;
                    }
                }
                let contained_part =
                    contained_part.expect("expected a future partition to contain the UUID");

                // Split the future partition into partitions for before and after this UUID
                // and add back to the future partitions.
                let (before_range, after_range) = contained_part.split(source_id);
                self.future
                    .extend(before_range.into_iter().chain(after_range));

                // Store the new part in our active partitions
                self.active.insert(source_id.clone(), new_part);
            }
        };

        Ok(())
    }
}
