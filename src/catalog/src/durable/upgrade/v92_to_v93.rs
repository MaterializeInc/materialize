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
use crate::durable::upgrade::objects_v92 as v92;
use crate::durable::upgrade::objects_v93 as v93;

crate::json_compatible!(v92::ClusterKey with v93::ClusterKey);
crate::json_compatible!(v92::RoleId with v93::RoleId);
crate::json_compatible!(v92::MzAclItem with v93::MzAclItem);
crate::json_compatible!(v92::ReplicaLogging with v93::ReplicaLogging);
crate::json_compatible!(v92::OptimizerFeatureOverride with v93::OptimizerFeatureOverride);
crate::json_compatible!(v92::ClusterSchedule with v93::ClusterSchedule);
crate::json_compatible!(v92::AutoScalingStrategy with v93::AutoScalingStrategy);
crate::json_compatible!(v92::ReconfigurationState with v93::ReconfigurationState);
crate::json_compatible!(v92::Duration with v93::Duration);

/// Rewrites active bursts with an unset caught-up readiness timestamp.
pub fn upgrade(
    snapshot: Vec<v92::StateUpdateKind>,
) -> Vec<MigrationAction<v92::StateUpdateKind, v93::StateUpdateKind>> {
    // Unstamped bursts also need rewriting so future retractions match the stored JSON.
    snapshot
        .into_iter()
        .filter_map(|update| match update {
            v92::StateUpdateKind::Cluster(cluster)
                if matches!(
                    &cluster.value.config.variant,
                    v92::ClusterVariant::Managed(managed) if managed.burst.is_some()
                ) =>
            {
                let new_cluster = migrate_cluster(cluster.clone());
                Some(MigrationAction::Update(
                    v92::StateUpdateKind::Cluster(cluster),
                    v93::StateUpdateKind::Cluster(new_cluster),
                ))
            }
            _ => None,
        })
        .collect()
}

fn migrate_cluster(old: v92::Cluster) -> v93::Cluster {
    let v92::Cluster { key, value } = old;
    let v92::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on an active managed burst");
    };
    v93::Cluster {
        key: JsonCompatible::convert(&key),
        value: v93::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v93::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: v93::ClusterVariant::Managed(v93::ManagedCluster {
                    size: m.size,
                    replication_factor: m.replication_factor,
                    availability_zones: m.availability_zones,
                    logging: JsonCompatible::convert(&m.logging),
                    arrangement_compression: m.arrangement_compression,
                    optimizer_feature_overrides: m
                        .optimizer_feature_overrides
                        .iter()
                        .map(JsonCompatible::convert)
                        .collect(),
                    schedule: JsonCompatible::convert(&m.schedule),
                    auto_scaling_strategy: m
                        .auto_scaling_strategy
                        .as_ref()
                        .map(JsonCompatible::convert),
                    reconfiguration: m.reconfiguration.as_ref().map(JsonCompatible::convert),
                    burst: m.burst.map(|burst| v93::BurstState {
                        burst_size: burst.burst_size,
                        linger_duration: JsonCompatible::convert(&burst.linger_duration),
                        // Hydration alone does not establish caught-up readiness.
                        steady_ready_at: None,
                    }),
                }),
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
        fn resets_burst_stamp_and_preserves_record(
            mut cluster in any::<v92::Cluster>(),
            mut managed in any::<v92::ManagedCluster>(),
            mut burst in any::<v92::BurstState>(),
            hydrated_at in any::<u64>(),
        ) {
            for stamp in [Some(hydrated_at), None] {
                burst.steady_hydrated_at = stamp;
                managed.burst = Some(burst.clone());
                cluster.value.config.variant = v92::ClusterVariant::Managed(managed.clone());
                let old = v92::StateUpdateKind::Cluster(cluster.clone());
                let actions = upgrade(vec![old.clone()]);
                prop_assert_eq!(actions.len(), 1);
                let MigrationAction::Update(retracted, inserted) = &actions[0] else {
                    panic!("expected a cluster update");
                };
                prop_assert_eq!(retracted, &old);
                let mut expected = serde_json::to_value(&old).expect("serializable");
                let burst_json = expected["value"]["config"]["variant"]["Managed"]["burst"]
                    .as_object_mut().expect("burst object");
                burst_json.remove("steady_hydrated_at");
                burst_json.insert("steady_ready_at".into(), serde_json::Value::Null);
                prop_assert_eq!(serde_json::to_value(inserted).expect("serializable"), expected);
            }
        }

        #[mz_ore::test]
        fn leaves_records_without_bursts_unchanged(
            mut cluster in any::<v92::Cluster>(),
            mut managed in any::<v92::ManagedCluster>(),
            replica in any::<v92::ClusterReplica>(),
        ) {
            managed.burst = None;
            cluster.value.config.variant = v92::ClusterVariant::Managed(managed);
            let managed = v92::StateUpdateKind::Cluster(cluster.clone());
            cluster.value.config.variant = v92::ClusterVariant::Unmanaged;
            let unmanaged = v92::StateUpdateKind::Cluster(cluster);
            let replica = v92::StateUpdateKind::ClusterReplica(replica);
            for old in [&managed, &unmanaged, &replica] {
                let encoded = serde_json::to_vec(old).expect("serializable");
                let new: v93::StateUpdateKind = serde_json::from_slice(&encoded).expect("compatible");
                prop_assert_eq!(serde_json::to_vec(&new).expect("serializable"), encoded);
            }
            prop_assert!(upgrade(vec![managed, unmanaged, replica]).is_empty());
        }
    }
}
