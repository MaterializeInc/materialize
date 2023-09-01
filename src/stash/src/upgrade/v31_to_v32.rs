// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::MigrationAction;
use crate::upgrade::{objects_v31, objects_v32};
use crate::{StashError, Transaction, TypedCollection};

/// Remove randomly selected az's.
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
        objects_v31::ClusterReplicaKey,
        objects_v31::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    CLUSTER_REPLICA_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                let new_key: objects_v32::ClusterReplicaKey = key.clone().into();
                let new_value: objects_v32::ClusterReplicaValue = value.clone().into();
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v31::ClusterReplicaKey> for objects_v32::ClusterReplicaKey {
    fn from(key: objects_v31::ClusterReplicaKey) -> Self {
        objects_v32::ClusterReplicaKey {
            id: key
                .id
                .map(|key| objects_v32::ReplicaId { value: key.value }),
        }
    }
}

impl From<objects_v31::RoleId> for objects_v32::RoleId {
    fn from(id: objects_v31::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v31::role_id::Value::User(id)) => {
                Some(objects_v32::role_id::Value::User(id))
            }
            Some(objects_v31::role_id::Value::System(id)) => {
                Some(objects_v32::role_id::Value::System(id))
            }
            Some(objects_v31::role_id::Value::Public(_)) => {
                Some(objects_v32::role_id::Value::Public(Default::default()))
            }
        };
        objects_v32::RoleId { value }
    }
}

impl From<objects_v31::ClusterId> for objects_v32::ClusterId {
    fn from(cluster_id: objects_v31::ClusterId) -> Self {
        let value = match cluster_id.value {
            None => None,
            Some(objects_v31::cluster_id::Value::System(id)) => {
                Some(objects_v32::cluster_id::Value::System(id))
            }
            Some(objects_v31::cluster_id::Value::User(id)) => {
                Some(objects_v32::cluster_id::Value::User(id))
            }
        };
        Self { value }
    }
}

impl From<objects_v31::Duration> for objects_v32::Duration {
    fn from(duration: objects_v31::Duration) -> Self {
        Self {
            secs: duration.secs,
            nanos: duration.nanos,
        }
    }
}

impl From<objects_v31::ReplicaLogging> for objects_v32::ReplicaLogging {
    fn from(logging: objects_v31::ReplicaLogging) -> Self {
        Self {
            log_logging: logging.log_logging,
            interval: logging.interval.map(Into::into),
        }
    }
}

impl From<objects_v31::ReplicaMergeEffort> for objects_v32::ReplicaMergeEffort {
    fn from(effort: objects_v31::ReplicaMergeEffort) -> Self {
        Self {
            effort: effort.effort,
        }
    }
}

impl From<objects_v31::ReplicaConfig> for objects_v32::ReplicaConfig {
    fn from(config: objects_v31::ReplicaConfig) -> Self {
        let location = match config.location {
            Some(objects_v31::replica_config::Location::Unmanaged(
                objects_v31::replica_config::UnmanagedLocation {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                },
            )) => Some(objects_v32::replica_config::Location::Unmanaged(
                objects_v32::replica_config::UnmanagedLocation {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                },
            )),
            Some(objects_v31::replica_config::Location::Managed(
                objects_v31::replica_config::ManagedLocation {
                    size,
                    availability_zone,
                    az_user_specified,
                    disk,
                },
            )) => Some(objects_v32::replica_config::Location::Managed(
                objects_v32::replica_config::ManagedLocation {
                    size,
                    // This is the core migration.
                    availability_zone: if az_user_specified {
                        Some(availability_zone)
                    } else {
                        None
                    },
                    disk,
                },
            )),
            None => None,
        };

        Self {
            logging: config.logging.map(Into::into),
            idle_arrangement_merge_effort: config.idle_arrangement_merge_effort.map(Into::into),
            location,
        }
    }
}

impl From<objects_v31::ClusterReplicaValue> for objects_v32::ClusterReplicaValue {
    fn from(config: objects_v31::ClusterReplicaValue) -> Self {
        Self {
            cluster_id: config.cluster_id.map(Into::into),
            name: config.name,
            config: config.config.map(Into::into),
            owner_id: config.owner_id.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::upgrade;

    use crate::upgrade::v31_to_v32::{objects_v31, objects_v32};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a cluster.
        let cluster_replica_v31: TypedCollection<
            objects_v31::ClusterReplicaKey,
            objects_v31::ClusterReplicaValue,
        > = TypedCollection::new("compute_replicas");

        // Test that we can correctly migrate unmanaged, and both cases of managed replicas.
        for (key, location) in [
            (
                10,
                objects_v31::replica_config::Location::Managed(
                    objects_v31::replica_config::ManagedLocation {
                        size: "s".to_string(),
                        availability_zone: "z".to_string(),
                        az_user_specified: true,
                        disk: true,
                    },
                ),
            ),
            (
                11,
                objects_v31::replica_config::Location::Managed(
                    objects_v31::replica_config::ManagedLocation {
                        size: "s".to_string(),
                        availability_zone: "z".to_string(),
                        az_user_specified: false,
                        disk: true,
                    },
                ),
            ),
            (
                12,
                objects_v31::replica_config::Location::Unmanaged(
                    objects_v31::replica_config::UnmanagedLocation {
                        storagectl_addrs: vec!["one".to_string()],
                        storage_addrs: vec!["two".to_string()],
                        computectl_addrs: vec!["three".to_string()],
                        compute_addrs: vec!["four".to_string()],
                        workers: 50,
                    },
                ),
            ),
        ] {
            cluster_replica_v31
                .insert_without_overwrite(
                    &mut stash,
                    vec![(
                        objects_v31::ClusterReplicaKey {
                            id: Some(objects_v31::ReplicaId { value: key }),
                        },
                        objects_v31::ClusterReplicaValue {
                            cluster_id: Some(objects_v31::ClusterId {
                                value: Some(objects_v31::cluster_id::Value::User(1)),
                            }),
                            name: "gus".to_string(),
                            config: Some(objects_v31::ReplicaConfig {
                                location: Some(location),
                                idle_arrangement_merge_effort: Some(
                                    objects_v31::ReplicaMergeEffort { effort: 1 },
                                ),
                                logging: Some(objects_v31::ReplicaLogging {
                                    log_logging: true,
                                    interval: Some(objects_v31::Duration {
                                        secs: 1,
                                        nanos: 100,
                                    }),
                                }),
                            }),
                            owner_id: Some(objects_v31::RoleId {
                                value: Some(objects_v31::role_id::Value::User(100)),
                            }),
                        },
                    )],
                )
                .await
                .unwrap();
        }

        // Run our migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .expect("migration failed");

        let cluster_replica_v32: TypedCollection<
            objects_v32::ClusterReplicaKey,
            objects_v32::ClusterReplicaValue,
        > = TypedCollection::new("compute_replicas");
        let cluster_replicas = cluster_replica_v32.iter(&mut stash).await.unwrap();

        let cluster_replicas: BTreeMap<_, _> = cluster_replicas
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        for (key, location) in [
            (
                10,
                objects_v32::replica_config::Location::Managed(
                    objects_v32::replica_config::ManagedLocation {
                        size: "s".to_string(),
                        availability_zone: Some("z".to_string()),
                        disk: true,
                    },
                ),
            ),
            (
                11,
                objects_v32::replica_config::Location::Managed(
                    objects_v32::replica_config::ManagedLocation {
                        size: "s".to_string(),
                        availability_zone: None,
                        disk: true,
                    },
                ),
            ),
            (
                12,
                objects_v32::replica_config::Location::Unmanaged(
                    objects_v32::replica_config::UnmanagedLocation {
                        storagectl_addrs: vec!["one".to_string()],
                        storage_addrs: vec!["two".to_string()],
                        computectl_addrs: vec!["three".to_string()],
                        compute_addrs: vec!["four".to_string()],
                        workers: 50,
                    },
                ),
            ),
        ] {
            assert_eq!(
                cluster_replicas[&objects_v32::ClusterReplicaKey {
                    id: Some(objects_v32::ReplicaId { value: key })
                }],
                objects_v32::ClusterReplicaValue {
                    cluster_id: Some(objects_v32::ClusterId {
                        value: Some(objects_v32::cluster_id::Value::User(1)),
                    }),
                    name: "gus".to_string(),
                    config: Some(objects_v32::ReplicaConfig {
                        location: Some(location),
                        idle_arrangement_merge_effort: Some(objects_v32::ReplicaMergeEffort {
                            effort: 1
                        },),
                        logging: Some(objects_v32::ReplicaLogging {
                            log_logging: true,
                            interval: Some(objects_v32::Duration {
                                secs: 1,
                                nanos: 100,
                            }),
                        }),
                    }),
                    owner_id: Some(objects_v32::RoleId {
                        value: Some(objects_v32::role_id::Value::User(100)),
                    }),
                }
            )
        }
    }
}
