// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v80 as v80;
use crate::durable::upgrade::objects_v81 as v81;

/// Migration to grant CREATEDATAFLOW privilege to PUBLIC on all existing clusters.
///
/// This ensures backwards compatibility - users who could previously run queries
/// requiring dataflow rendering can continue to do so after the upgrade.
pub fn upgrade(
    snapshot: Vec<v80::StateUpdateKind>,
) -> Vec<MigrationAction<v80::StateUpdateKind, v81::StateUpdateKind>> {
    // CREATE_DATAFLOW privilege bit (1 << 28)
    const CREATE_DATAFLOW_BIT: u64 = 1 << 28;

    let mut migrations = Vec::new();

    for update in snapshot {
        if let v80::StateUpdateKind::Cluster(cluster) = &update {
            // Check if PUBLIC already has CREATEDATAFLOW
            let public_has_createdataflow = cluster.value.privileges.iter().any(|item| {
                matches!(item.grantee, v80::RoleId::Public)
                    && (item.acl_mode.bitflags & CREATE_DATAFLOW_BIT) != 0
            });

            if !public_has_createdataflow {
                // Clone the cluster and add CREATEDATAFLOW for PUBLIC
                let mut new_value = cluster.value.clone();

                // Add CREATEDATAFLOW privilege for PUBLIC
                // Use System(1) as grantor (mz_system role)
                new_value.privileges.push(v80::MzAclItem {
                    grantee: v80::RoleId::Public,
                    grantor: v80::RoleId::System(1),
                    acl_mode: v80::AclMode {
                        bitflags: CREATE_DATAFLOW_BIT,
                    },
                });

                // Since v80 and v81 are JSON-compatible, we can serialize/deserialize
                let old = update.clone();
                let new_json = serde_json::to_value(&v80::StateUpdateKind::Cluster(v80::Cluster {
                    key: cluster.key.clone(),
                    value: new_value,
                }))
                .expect("serialization cannot fail");
                let new: v81::StateUpdateKind =
                    serde_json::from_value(new_json).expect("v80 and v81 are JSON-compatible");

                migrations.push(MigrationAction::Update(old, new));
            }
        }
    }

    migrations
}
