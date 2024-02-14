// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::{wire_compatible, MigrationAction, WireCompatible};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::upgrade::{objects_v46 as v46, objects_v47 as v47};

wire_compatible!(v46::ClusterKey with v47::ClusterKey);
wire_compatible!(v46::MzAclItem with v47::MzAclItem);
wire_compatible!(v46::RoleId with v47::RoleId);
wire_compatible!(v46::ReplicaLogging with v47::ReplicaLogging);
wire_compatible!(v46::ReplicaMergeEffort with v47::ReplicaMergeEffort);

const CLUSTER_COLLECTION: TypedCollection<v46::ClusterKey, v46::ClusterValue> =
    TypedCollection::new("clusters");

/// Introduce empty `optimizer_feature_overrides` in `ManagedCluster`'s.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    CLUSTER_COLLECTION
        .migrate_to::<v47::ClusterKey, v47::ClusterValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_val)| {
                    let new_key = WireCompatible::convert(old_key);
                    let new_val = v47::ClusterValue {
                        name: old_val.name.clone(),
                        owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
                        privileges: old_val
                            .privileges
                            .iter()
                            .map(WireCompatible::convert)
                            .collect(),
                        config: old_val.config.as_ref().map(|config| v47::ClusterConfig {
                            variant: config.variant.as_ref().map(|variant| match variant {
                                v46::cluster_config::Variant::Unmanaged(_) => {
                                    v47::cluster_config::Variant::Unmanaged(v47::Empty {})
                                }
                                v46::cluster_config::Variant::Managed(c) => {
                                    v47::cluster_config::Variant::Managed(
                                        v47::cluster_config::ManagedCluster {
                                            size: c.size.clone(),
                                            replication_factor: c.replication_factor,
                                            availability_zones: c.availability_zones.clone(),
                                            logging: c
                                                .logging
                                                .as_ref()
                                                .map(WireCompatible::convert),
                                            idle_arrangement_merge_effort: c
                                                .idle_arrangement_merge_effort
                                                .as_ref()
                                                .map(WireCompatible::convert),
                                            disk: c.disk,
                                            optimizer_feature_overrides: Vec::new(),
                                        },
                                    )
                                }
                            }),
                        }),
                    };

                    MigrationAction::Update(old_key.clone(), (new_key, new_val))
                })
                .collect()
        })
        .await?;
    Ok(())
}
