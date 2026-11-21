// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v96 as v96;
use crate::durable::upgrade::objects_v97 as v97;

crate::json_compatible!(v96::ClusterKey with v97::ClusterKey);
crate::json_compatible!(v96::RoleId with v97::RoleId);
crate::json_compatible!(v96::MzAclItem with v97::MzAclItem);
crate::json_compatible!(v96::ClusterVariant with v97::ClusterVariant);
crate::json_compatible!(v96::QueryPolicyId with v97::QueryPolicyId);

/// Adds `query_routing` to every cluster, set to `Broadcast`, which is how every cluster
/// behaved before the field existed.
pub fn upgrade(
    snapshot: Vec<v96::StateUpdateKind>,
) -> Vec<MigrationAction<v96::StateUpdateKind, v97::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update {
            v96::StateUpdateKind::Cluster(cluster) => {
                let new_cluster = migrate_cluster(cluster.clone());
                Some(MigrationAction::Update(
                    v96::StateUpdateKind::Cluster(cluster),
                    v97::StateUpdateKind::Cluster(new_cluster),
                ))
            }
            _ => None,
        })
        .collect()
}

fn migrate_cluster(old: v96::Cluster) -> v97::Cluster {
    let v96::Cluster { key, value } = old;
    v97::Cluster {
        key: JsonCompatible::convert(&key),
        value: v97::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v97::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: JsonCompatible::convert(&value.config.variant),
                query_policy: value
                    .config
                    .query_policy
                    .as_ref()
                    .map(JsonCompatible::convert),
                query_routing: v97::QueryRouting::Broadcast,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn adds_broadcast_routing_and_preserves_record(cluster in any::<v96::Cluster>()) {
            let old = v96::StateUpdateKind::Cluster(cluster);
            let actions = upgrade(vec![old.clone()]);
            prop_assert_eq!(actions.len(), 1);
            let MigrationAction::Update(retracted, inserted) = &actions[0] else {
                panic!("expected a cluster update");
            };
            prop_assert_eq!(retracted, &old);
            let mut expected = serde_json::to_value(&old).expect("serializable");
            expected["value"]["config"]
                .as_object_mut()
                .expect("config object")
                .insert("query_routing".into(), serde_json::Value::from("Broadcast"));
            prop_assert_eq!(serde_json::to_value(inserted).expect("serializable"), expected);
        }

        #[mz_ore::test]
        fn leaves_other_records_unchanged(replica in any::<v96::ClusterReplica>()) {
            let actions = upgrade(vec![v96::StateUpdateKind::ClusterReplica(replica)]);
            prop_assert!(actions.is_empty());
        }
    }
}
