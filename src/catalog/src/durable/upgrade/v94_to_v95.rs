// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v94 as v94;
use crate::durable::upgrade::objects_v95 as v95;

/// Adds the ID allocators for the new `ClusterReplicaSize` collection. The
/// collection itself starts empty and is populated from the
/// `--cluster-replica-sizes` flag on catalog open.
pub fn upgrade(
    _snapshot: Vec<v94::StateUpdateKind>,
) -> Vec<MigrationAction<v94::StateUpdateKind, v95::StateUpdateKind>> {
    ["user_cluster_replica_size", "system_cluster_replica_size"]
        .into_iter()
        .map(|name| {
            MigrationAction::Insert(v95::StateUpdateKind::IdAlloc(v95::IdAlloc {
                key: v95::IdAllocKey {
                    name: name.to_string(),
                },
                value: v95::IdAllocValue { next_id: 1 },
            }))
        })
        .collect()
}
