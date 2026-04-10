// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v81 as v81;
use crate::durable::upgrade::objects_v82 as v82;

/// Migration that introduces the `ClusterReplicaSize` durable object type
/// and its two ID allocators (`user_cluster_replica_size` and
/// `system_cluster_replica_size`). The allocators are required for
/// `Transaction::insert_cluster_replica_size` to allocate fresh IDs;
/// catalogs upgraded from v81 must have these allocator entries inserted.
pub fn upgrade(
    snapshot: Vec<v81::StateUpdateKind>,
) -> Vec<MigrationAction<v81::StateUpdateKind, v82::StateUpdateKind>> {
    // Check whether the allocator entries are already present (defensive: should
    // never happen for a fresh v81 catalog, but guards against double-migration).
    let user_alloc_present = snapshot.iter().any(|update| match update {
        v81::StateUpdateKind::IdAlloc(alloc) => alloc.key.name == "user_cluster_replica_size",
        _ => false,
    });
    let system_alloc_present = snapshot.iter().any(|update| match update {
        v81::StateUpdateKind::IdAlloc(alloc) => alloc.key.name == "system_cluster_replica_size",
        _ => false,
    });

    let mut actions = Vec::new();

    if !user_alloc_present {
        actions.push(MigrationAction::Insert(v82::StateUpdateKind::IdAlloc(
            v82::IdAlloc {
                key: v82::IdAllocKey {
                    name: "user_cluster_replica_size".to_string(),
                },
                value: v82::IdAllocValue { next_id: 1 },
            },
        )));
    }

    if !system_alloc_present {
        actions.push(MigrationAction::Insert(v82::StateUpdateKind::IdAlloc(
            v82::IdAlloc {
                key: v82::IdAllocKey {
                    name: "system_cluster_replica_size".to_string(),
                },
                value: v82::IdAllocValue { next_id: 1 },
            },
        )));
    }

    actions
}
