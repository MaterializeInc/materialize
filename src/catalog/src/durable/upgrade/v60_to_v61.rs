// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::wire_compatible;
use mz_proto::wire_compatible::WireCompatible;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v60 as v60, objects_v61 as v61};

wire_compatible!(v60::ClusterKey with v61::ClusterKey);
wire_compatible!(v60::RoleId with v61::RoleId);
wire_compatible!(v60::MzAclItem with v61::MzAclItem);
wire_compatible!(v60::cluster_config::ManagedCluster with v61::cluster_config::ManagedCluster);

/// In v61, we added a new optional `workload_class` field to `ClusterConfig`.
pub fn upgrade(
    snapshot: Vec<v60::StateUpdateKind>,
) -> Vec<MigrationAction<v60::StateUpdateKind, v61::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let v60::state_update_kind::Kind::Cluster(v60::state_update_kind::Cluster {
                key,
                value,
            }) = update.kind.as_ref().expect("missing field")
            else {
                return None;
            };

            let old = v60::StateUpdateKind {
                kind: Some(v60::state_update_kind::Kind::Cluster(
                    v60::state_update_kind::Cluster {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )),
            };

            let new = v61::StateUpdateKind {
                kind: Some(v61::state_update_kind::Kind::Cluster(
                    v61::state_update_kind::Cluster {
                        key: key.as_ref().map(WireCompatible::convert),
                        value: value.as_ref().map(|old_val| v61::ClusterValue {
                            name: old_val.name.clone(),
                            owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
                            privileges: old_val
                                .privileges
                                .iter()
                                .map(WireCompatible::convert)
                                .collect(),
                            config: old_val
                                .config
                                .as_ref()
                                .map(|old_config| v61::ClusterConfig {
                                    workload_class: None,
                                    variant: old_config.variant.as_ref().map(
                                        |variant| match variant {
                                            v60::cluster_config::Variant::Unmanaged(_) => {
                                                v61::cluster_config::Variant::Unmanaged(
                                                    v61::Empty {},
                                                )
                                            }
                                            v60::cluster_config::Variant::Managed(c) => {
                                                v61::cluster_config::Variant::Managed(
                                                    WireCompatible::convert(c),
                                                )
                                            }
                                        },
                                    ),
                                }),
                        }),
                    },
                )),
            };

            Some(MigrationAction::Update(old, new))
        })
        .collect()
}
