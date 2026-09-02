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
use crate::durable::upgrade::objects_v88 as v88;
use crate::durable::upgrade::objects_v89 as v89;

crate::json_compatible!(v88::ClusterKey with v89::ClusterKey);
crate::json_compatible!(v88::RoleId with v89::RoleId);
crate::json_compatible!(v88::MzAclItem with v89::MzAclItem);
crate::json_compatible!(v88::ReplicaLogging with v89::ReplicaLogging);
crate::json_compatible!(v88::OptimizerFeatureOverride with v89::OptimizerFeatureOverride);
crate::json_compatible!(v88::ClusterSchedule with v89::ClusterSchedule);
crate::json_compatible!(v88::AutoScalingStrategy with v89::AutoScalingStrategy);
crate::json_compatible!(v88::BurstState with v89::BurstState);
crate::json_compatible!(v88::ReconfigurationTarget with v89::ReconfigurationTarget);
crate::json_compatible!(v88::OnTimeoutAction with v89::OnTimeoutAction);

/// Adds `ReconfigurationState::status`, backfilling any in-flight
/// reconfiguration as `InProgress`. The remaining v88->v89 changes are
/// additive and confined to the append-only audit log (new reconfiguration
/// lifecycle transitions and hydration-burst lifecycle details), so nothing
/// else is rewritten.
pub fn upgrade(
    snapshot: Vec<v88::StateUpdateKind>,
) -> Vec<MigrationAction<v88::StateUpdateKind, v89::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v88::StateUpdateKind::Cluster(old_cluster)
                if matches!(
                    old_cluster.value.config.variant,
                    v88::ClusterVariant::Managed(v88::ManagedCluster {
                        reconfiguration: Some(_),
                        ..
                    })
                ) =>
            {
                let new_cluster = migrate_cluster(old_cluster.clone());
                migrations.push(MigrationAction::Update(
                    v88::StateUpdateKind::Cluster(old_cluster),
                    v89::StateUpdateKind::Cluster(new_cluster),
                ));
            }
            _ => {}
        }
    }
    migrations
}

fn migrate_cluster(old: v88::Cluster) -> v89::Cluster {
    let v88::Cluster { key, value } = old;
    let v88::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on the managed variant");
    };
    let logging = JsonCompatible::convert(&m.logging);
    let optimizer_feature_overrides = m
        .optimizer_feature_overrides
        .iter()
        .map(JsonCompatible::convert)
        .collect();
    let schedule = JsonCompatible::convert(&m.schedule);
    let auto_scaling_strategy = m
        .auto_scaling_strategy
        .as_ref()
        .map(JsonCompatible::convert);
    let reconfiguration = m.reconfiguration.map(migrate_reconfiguration);
    let burst = m.burst.as_ref().map(JsonCompatible::convert);
    v89::Cluster {
        key: JsonCompatible::convert(&key),
        value: v89::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v89::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: v89::ClusterVariant::Managed(v89::ManagedCluster {
                    size: m.size,
                    replication_factor: m.replication_factor,
                    availability_zones: m.availability_zones,
                    logging,
                    optimizer_feature_overrides,
                    schedule,
                    auto_scaling_strategy,
                    reconfiguration,
                    burst,
                }),
            },
        },
    }
}

fn migrate_reconfiguration(old: v88::ReconfigurationState) -> v89::ReconfigurationState {
    v89::ReconfigurationState {
        target: JsonCompatible::convert(&old.target),
        deadline: old.deadline,
        on_timeout: JsonCompatible::convert(&old.on_timeout),
        status: v89::ReconfigurationStatus::InProgress,
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v88_to_v89::upgrade;
    use crate::durable::upgrade::{objects_v88 as v88, objects_v89 as v89};

    fn managed_cluster(
        id: u64,
        reconfiguration: Option<v88::ReconfigurationState>,
    ) -> v88::Cluster {
        v88::Cluster {
            key: v88::ClusterKey {
                id: v88::ClusterId::User(id),
            },
            value: v88::ClusterValue {
                name: format!("cluster{id}"),
                owner_id: v88::RoleId::User(1),
                privileges: Vec::new(),
                config: v88::ClusterConfig {
                    workload_class: Some("production".to_string()),
                    variant: v88::ClusterVariant::Managed(v88::ManagedCluster {
                        size: "small".to_string(),
                        replication_factor: 1,
                        availability_zones: vec!["az1".to_string()],
                        logging: v88::ReplicaLogging {
                            log_logging: false,
                            interval: None,
                        },
                        optimizer_feature_overrides: Vec::new(),
                        schedule: v88::ClusterSchedule::Manual,
                        auto_scaling_strategy: None,
                        reconfiguration,
                        burst: None,
                    }),
                },
            },
        }
    }

    fn reconfiguration() -> v88::ReconfigurationState {
        v88::ReconfigurationState {
            target: v88::ReconfigurationTarget {
                size: "large".to_string(),
                replication_factor: 2,
                availability_zones: vec!["az2".to_string()],
                logging: v88::ReplicaLogging {
                    log_logging: true,
                    interval: None,
                },
            },
            deadline: 42,
            on_timeout: v88::OnTimeoutAction::Rollback,
        }
    }

    #[mz_ore::test]
    fn backfills_existing_reconfiguration_as_in_progress() {
        let migrations = upgrade(vec![v88::StateUpdateKind::Cluster(managed_cluster(
            1,
            Some(reconfiguration()),
        ))]);
        assert_eq!(migrations.len(), 1);

        let MigrationAction::Update(_, v89::StateUpdateKind::Cluster(cluster)) = &migrations[0]
        else {
            panic!("expected a cluster update");
        };
        let v89::ClusterVariant::Managed(managed) = &cluster.value.config.variant else {
            panic!("expected a managed cluster");
        };
        let record = managed
            .reconfiguration
            .as_ref()
            .expect("reconfiguration should be retained");
        assert_eq!(record.status, v89::ReconfigurationStatus::InProgress);
        assert_eq!(record.target.size, "large");
        assert_eq!(record.deadline, 42);
        assert_eq!(
            cluster.value.config.workload_class.as_deref(),
            Some("production")
        );
    }

    #[mz_ore::test]
    fn skips_clusters_without_reconfiguration() {
        let migrations = upgrade(vec![v88::StateUpdateKind::Cluster(managed_cluster(
            1, None,
        ))]);
        assert!(migrations.is_empty());
    }
}
