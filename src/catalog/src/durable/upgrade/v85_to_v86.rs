// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v85 as v85;
use crate::durable::upgrade::objects_v86 as v86;

// Every sub-structure carried across the version unchanged. The only shapes
// that actually gained fields between v85 and v86 are the managed `Cluster`
// variant and the managed `ReplicaLocation`; everything else is JSON-identical
// and is moved across via `JsonCompatible::convert`. The macro also generates
// proptest round-trips asserting the two encodings stay byte-compatible.
crate::json_compatible!(v85::ClusterKey with v86::ClusterKey);
crate::json_compatible!(v85::RoleId with v86::RoleId);
crate::json_compatible!(v85::MzAclItem with v86::MzAclItem);
crate::json_compatible!(v85::ReplicaLogging with v86::ReplicaLogging);
crate::json_compatible!(v85::OptimizerFeatureOverride with v86::OptimizerFeatureOverride);
crate::json_compatible!(v85::ClusterSchedule with v86::ClusterSchedule);
crate::json_compatible!(v85::ClusterReplicaKey with v86::ClusterReplicaKey);
crate::json_compatible!(v85::ClusterId with v86::ClusterId);
crate::json_compatible!(v85::UnmanagedLocation with v86::UnmanagedLocation);

/// Adds the additive, behaviorally-inert durable cluster-controller state and
/// reshapes the managed replica location's availability-zone field:
///
///   - `ManagedCluster` gains `auto_scaling_strategy`, `reconfiguration`, and
///     `burst`, all defaulted to `None`.
///   - The managed `ReplicaLocation`'s single `availability_zone` user-pin
///     becomes an `availability_zones` list recording the zones the replica was
///     provisioned under: for a replica of a managed cluster the owning
///     *cluster*'s current `availability_zones` (the placement pool); for a
///     replica of an unmanaged cluster its prior single pin, carried across as
///     a zero- or one-element list.
///
/// Only the managed `Cluster` variant and managed `ReplicaLocation` changed
/// shape, so only those records are rewritten. Unmanaged clusters and unmanaged
/// replica locations are JSON-identical in v86 and are left untouched —
/// emitting an update for them would just retract and re-add the same bytes.
pub fn upgrade(
    snapshot: Vec<v85::StateUpdateKind>,
) -> Vec<MigrationAction<v85::StateUpdateKind, v86::StateUpdateKind>> {
    // The per-replica provisioned-AZ list is backfilled from the owning
    // cluster's `availability_zones`, so collect every managed cluster's list
    // before rewriting replicas. Unmanaged clusters contribute no entry, so a
    // replica of one backfills the empty list.
    let mut cluster_azs: BTreeMap<v85::ClusterId, Vec<String>> = BTreeMap::new();
    for update in &snapshot {
        if let v85::StateUpdateKind::Cluster(cluster) = update {
            if let v85::ClusterVariant::Managed(managed) = &cluster.value.config.variant {
                cluster_azs.insert(cluster.key.id.clone(), managed.availability_zones.clone());
            }
        }
    }

    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v85::StateUpdateKind::Cluster(old_cluster)
                if matches!(
                    old_cluster.value.config.variant,
                    v85::ClusterVariant::Managed(_)
                ) =>
            {
                let new_cluster = migrate_managed_cluster(old_cluster.clone());
                migrations.push(MigrationAction::Update(
                    v85::StateUpdateKind::Cluster(old_cluster),
                    v86::StateUpdateKind::Cluster(new_cluster),
                ));
            }
            v85::StateUpdateKind::ClusterReplica(old_replica)
                if matches!(
                    old_replica.value.config.location,
                    v85::ReplicaLocation::Managed(_)
                ) =>
            {
                // `Some(pool)` for a replica of a managed cluster (the cluster's
                // AZ list, possibly empty); `None` for a replica of an unmanaged
                // cluster, which contributes no entry and keeps its own pin.
                let cluster_pool = cluster_azs.get(&old_replica.value.cluster_id).cloned();
                let new_replica = migrate_managed_replica(old_replica.clone(), cluster_pool);
                migrations.push(MigrationAction::Update(
                    v85::StateUpdateKind::ClusterReplica(old_replica),
                    v86::StateUpdateKind::ClusterReplica(new_replica),
                ));
            }
            // Unmanaged clusters and unmanaged locations are JSON-identical
            // across the version; nothing else changed shape.
            _ => {}
        }
    }
    migrations
}

/// Rewrites a managed `Cluster`, reconstructing only the `ManagedCluster` that
/// gained fields and carrying every other sub-structure across unchanged.
fn migrate_managed_cluster(old: v85::Cluster) -> v86::Cluster {
    let v85::Cluster { key, value } = old;
    let v85::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on the managed variant");
    };
    v86::Cluster {
        key: JsonCompatible::convert(&key),
        value: v86::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v86::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: v86::ClusterVariant::Managed(v86::ManagedCluster {
                    size: m.size,
                    replication_factor: m.replication_factor,
                    availability_zones: m.availability_zones,
                    logging: JsonCompatible::convert(&m.logging),
                    optimizer_feature_overrides: m
                        .optimizer_feature_overrides
                        .iter()
                        .map(JsonCompatible::convert)
                        .collect(),
                    schedule: JsonCompatible::convert(&m.schedule),
                    // Additive, defaulted: no policy or in-flight state for
                    // existing clusters.
                    auto_scaling_strategy: None,
                    reconfiguration: None,
                    burst: None,
                }),
            },
        },
    }
}

/// Rewrites a `ClusterReplica` with a managed location, reconstructing only the
/// `ManagedLocation` whose `availability_zone` user-pin became an
/// `availability_zones` list and carrying every other sub-structure across
/// unchanged. `cluster_pool` is `Some` (the owning managed cluster's AZ list)
/// for a replica of a managed cluster and `None` for a replica of an unmanaged
/// cluster.
fn migrate_managed_replica(
    old: v85::ClusterReplica,
    cluster_pool: Option<Vec<String>>,
) -> v86::ClusterReplica {
    let v85::ClusterReplica { key, value } = old;
    let v85::ReplicaLocation::Managed(m) = value.config.location else {
        unreachable!("caller guards on the managed location");
    };
    let availability_zones = match cluster_pool {
        // Managed cluster: provisioned under the cluster's `AVAILABILITY ZONES`
        // pool. (A managed cluster's replica never carries its own single-AZ
        // pin, so `m.availability_zone` is always `None` here.)
        Some(pool) => pool,
        // Unmanaged cluster: carry the replica's user-pinned `AVAILABILITY ZONE`
        // across as a zero- or one-element list.
        None => m.availability_zone.into_iter().collect(),
    };
    v86::ClusterReplica {
        key: JsonCompatible::convert(&key),
        value: v86::ClusterReplicaValue {
            cluster_id: JsonCompatible::convert(&value.cluster_id),
            name: value.name,
            config: v86::ReplicaConfig {
                logging: JsonCompatible::convert(&value.config.logging),
                location: v86::ReplicaLocation::Managed(v86::ManagedLocation {
                    size: m.size,
                    availability_zones,
                    internal: m.internal,
                    billed_as: m.billed_as,
                    pending: m.pending,
                }),
            },
            owner_id: JsonCompatible::convert(&value.owner_id),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::objects_v85 as v85;
    use crate::durable::upgrade::objects_v86 as v86;

    fn managed_cluster(id: u64, availability_zones: Vec<String>) -> v85::StateUpdateKind {
        v85::StateUpdateKind::Cluster(v85::Cluster {
            key: v85::ClusterKey {
                id: v85::ClusterId::User(id),
            },
            value: v85::ClusterValue {
                name: format!("cluster_{id}"),
                owner_id: v85::RoleId::User(1),
                privileges: vec![],
                config: v85::ClusterConfig {
                    workload_class: None,
                    variant: v85::ClusterVariant::Managed(v85::ManagedCluster {
                        size: "100cc".to_string(),
                        replication_factor: 1,
                        availability_zones,
                        logging: v85::ReplicaLogging {
                            log_logging: false,
                            interval: None,
                        },
                        optimizer_feature_overrides: vec![],
                        schedule: v85::ClusterSchedule::Manual,
                    }),
                },
            },
        })
    }

    fn unmanaged_cluster(id: u64) -> v85::StateUpdateKind {
        v85::StateUpdateKind::Cluster(v85::Cluster {
            key: v85::ClusterKey {
                id: v85::ClusterId::User(id),
            },
            value: v85::ClusterValue {
                name: format!("cluster_{id}"),
                owner_id: v85::RoleId::User(1),
                privileges: vec![],
                config: v85::ClusterConfig {
                    workload_class: None,
                    variant: v85::ClusterVariant::Unmanaged,
                },
            },
        })
    }

    fn managed_replica(
        replica_id: u64,
        cluster_id: u64,
        availability_zone: Option<String>,
    ) -> v85::StateUpdateKind {
        v85::StateUpdateKind::ClusterReplica(v85::ClusterReplica {
            key: v85::ClusterReplicaKey {
                id: v85::ReplicaId::User(replica_id),
            },
            value: v85::ClusterReplicaValue {
                cluster_id: v85::ClusterId::User(cluster_id),
                name: format!("r{replica_id}"),
                config: v85::ReplicaConfig {
                    logging: v85::ReplicaLogging {
                        log_logging: false,
                        interval: None,
                    },
                    location: v85::ReplicaLocation::Managed(v85::ManagedLocation {
                        size: "100cc".to_string(),
                        availability_zone,
                        internal: false,
                        billed_as: None,
                        pending: false,
                    }),
                },
                owner_id: v85::RoleId::User(1),
            },
        })
    }

    #[mz_ore::test]
    fn test_cluster_new_fields_default_none() {
        let migrations = upgrade(vec![managed_cluster(1, vec!["az1".to_string()])]);
        assert_eq!(migrations.len(), 1);
        let MigrationAction::Update(_, v86::StateUpdateKind::Cluster(cluster)) = &migrations[0]
        else {
            panic!("expected a cluster update");
        };
        let v86::ClusterVariant::Managed(managed) = &cluster.value.config.variant else {
            panic!("expected a managed cluster");
        };
        assert_eq!(managed.auto_scaling_strategy, None);
        assert_eq!(managed.reconfiguration, None);
        assert_eq!(managed.burst, None);
        // Existing fields are preserved.
        assert_eq!(managed.availability_zones, vec!["az1".to_string()]);
    }

    #[mz_ore::test]
    fn test_replica_backfills_cluster_azs() {
        let azs = vec!["az1".to_string(), "az2".to_string()];
        let migrations = upgrade(vec![
            managed_cluster(1, azs.clone()),
            managed_replica(10, 1, None),
        ]);
        assert_eq!(migrations.len(), 2);

        let MigrationAction::Update(_, v86::StateUpdateKind::ClusterReplica(replica)) =
            &migrations[1]
        else {
            panic!("expected a replica update");
        };
        let v86::ReplicaLocation::Managed(loc) = &replica.value.config.location else {
            panic!("expected a managed location");
        };
        assert_eq!(loc.availability_zones, azs);
    }

    #[mz_ore::test]
    fn test_replica_of_unmanaged_cluster_carries_pin_as_list() {
        // A managed location can carry a single-AZ user-pin only when its
        // owning cluster is unmanaged. That pin is carried across as a
        // one-element `availability_zones` list. The unmanaged cluster itself
        // is JSON-identical and emits no migration, so the replica is the only
        // action.
        let migrations = upgrade(vec![
            unmanaged_cluster(1),
            managed_replica(10, 1, Some("az1".to_string())),
        ]);
        assert_eq!(migrations.len(), 1);
        let MigrationAction::Update(_, v86::StateUpdateKind::ClusterReplica(replica)) =
            &migrations[0]
        else {
            panic!("expected a replica update");
        };
        let v86::ReplicaLocation::Managed(loc) = &replica.value.config.location else {
            panic!("expected a managed location");
        };
        assert_eq!(loc.availability_zones, vec!["az1".to_string()]);
    }

    #[mz_ore::test]
    fn test_replica_no_cluster_azs_backfills_empty() {
        let migrations = upgrade(vec![
            managed_cluster(1, vec![]),
            managed_replica(10, 1, None),
        ]);
        let MigrationAction::Update(_, v86::StateUpdateKind::ClusterReplica(replica)) =
            &migrations[1]
        else {
            panic!("expected a replica update");
        };
        let v86::ReplicaLocation::Managed(loc) = &replica.value.config.location else {
            panic!("expected a managed location");
        };
        assert!(loc.availability_zones.is_empty());
    }

    #[mz_ore::test]
    fn test_unmanaged_records_are_not_rewritten() {
        // Unmanaged clusters and unmanaged replica locations are JSON-identical
        // between v85 and v86, so the migration leaves them untouched rather
        // than emitting a no-op retract+add of the same bytes.
        let unmanaged_replica = v85::StateUpdateKind::ClusterReplica(v85::ClusterReplica {
            key: v85::ClusterReplicaKey {
                id: v85::ReplicaId::User(20),
            },
            value: v85::ClusterReplicaValue {
                cluster_id: v85::ClusterId::User(2),
                name: "r20".to_string(),
                config: v85::ReplicaConfig {
                    logging: v85::ReplicaLogging {
                        log_logging: false,
                        interval: None,
                    },
                    location: v85::ReplicaLocation::Unmanaged(v85::UnmanagedLocation {
                        storagectl_addrs: vec!["s".to_string()],
                        computectl_addrs: vec!["c".to_string()],
                    }),
                },
                owner_id: v85::RoleId::User(1),
            },
        });
        let migrations = upgrade(vec![unmanaged_cluster(2), unmanaged_replica]);
        assert!(migrations.is_empty());
    }
}
