// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v22 {
    include!(concat!(env!("OUT_DIR"), "/objects_v22.rs"));
}
pub mod objects_v23 {
    include!(concat!(env!("OUT_DIR"), "/objects_v23.rs"));
}

const MZ_INTROSPECTION_ROLE_ID: objects_v23::RoleId = objects_v23::RoleId {
    value: Some(objects_v23::role_id::Value::System(2)),
};
const PUBLIC_ROLE_ID: objects_v23::RoleId = objects_v23::RoleId {
    value: Some(objects_v23::role_id::Value::Public(objects_v23::Empty {})),
};
const ACL_MODE_USAGE: objects_v23::AclMode = objects_v23::AclMode { bitflags: 1 << 8 };

/// Give mz_introspection USAGE privileges on all clusters.
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
        objects_v22::DefaultPrivilegesKey,
        objects_v22::DefaultPrivilegesValue,
    > = TypedCollection::new("default_privileges");
    const CLUSTER_COLLECTION: TypedCollection<objects_v22::ClusterKey, objects_v22::ClusterValue> =
        TypedCollection::new("compute_instance");

    DEFAULT_PRIVILEGES_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = vec![MigrationAction::Insert(
                objects_v23::DefaultPrivilegesKey {
                    role_id: Some(PUBLIC_ROLE_ID),
                    database_id: None,
                    schema_id: None,
                    object_type: objects_v23::ObjectType::Cluster.into(),
                    grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                },
                objects_v23::DefaultPrivilegesValue {
                    privileges: Some(ACL_MODE_USAGE),
                },
            )];

            for (key, value) in entries {
                let new_key = key.clone().into();
                let new_value = value.clone().into();
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    CLUSTER_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                let new_key: objects_v23::ClusterKey = key.clone().into();
                let mut new_value: objects_v23::ClusterValue = value.clone().into();
                new_value.privileges.push(objects_v23::MzAclItem {
                    grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                    grantor: value.owner_id.clone().map(Into::into),
                    acl_mode: Some(ACL_MODE_USAGE),
                });
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v22::RoleId> for objects_v23::RoleId {
    fn from(id: objects_v22::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v22::role_id::Value::User(id)) => {
                Some(objects_v23::role_id::Value::User(id))
            }
            Some(objects_v22::role_id::Value::System(id)) => {
                Some(objects_v23::role_id::Value::System(id))
            }
            Some(objects_v22::role_id::Value::Public(_)) => {
                Some(objects_v23::role_id::Value::Public(Default::default()))
            }
        };
        objects_v23::RoleId { value }
    }
}

impl From<objects_v22::DatabaseId> for objects_v23::DatabaseId {
    fn from(id: objects_v22::DatabaseId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v22::database_id::Value::User(id)) => {
                Some(objects_v23::database_id::Value::User(id))
            }
            Some(objects_v22::database_id::Value::System(id)) => {
                Some(objects_v23::database_id::Value::System(id))
            }
        };
        objects_v23::DatabaseId { value }
    }
}

impl From<objects_v22::SchemaId> for objects_v23::SchemaId {
    fn from(id: objects_v22::SchemaId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v22::schema_id::Value::User(id)) => {
                Some(objects_v23::schema_id::Value::User(id))
            }
            Some(objects_v22::schema_id::Value::System(id)) => {
                Some(objects_v23::schema_id::Value::System(id))
            }
        };
        objects_v23::SchemaId { value }
    }
}

impl From<objects_v22::ObjectType> for objects_v23::ObjectType {
    fn from(object_type: objects_v22::ObjectType) -> Self {
        match object_type {
            objects_v22::ObjectType::Unknown => objects_v23::ObjectType::Unknown,
            objects_v22::ObjectType::Table => objects_v23::ObjectType::Table,
            objects_v22::ObjectType::View => objects_v23::ObjectType::View,
            objects_v22::ObjectType::MaterializedView => objects_v23::ObjectType::MaterializedView,
            objects_v22::ObjectType::Source => objects_v23::ObjectType::Source,
            objects_v22::ObjectType::Sink => objects_v23::ObjectType::Sink,
            objects_v22::ObjectType::Index => objects_v23::ObjectType::Index,
            objects_v22::ObjectType::Type => objects_v23::ObjectType::Type,
            objects_v22::ObjectType::Role => objects_v23::ObjectType::Role,
            objects_v22::ObjectType::Cluster => objects_v23::ObjectType::Cluster,
            objects_v22::ObjectType::ClusterReplica => objects_v23::ObjectType::ClusterReplica,
            objects_v22::ObjectType::Secret => objects_v23::ObjectType::Secret,
            objects_v22::ObjectType::Connection => objects_v23::ObjectType::Connection,
            objects_v22::ObjectType::Database => objects_v23::ObjectType::Database,
            objects_v22::ObjectType::Schema => objects_v23::ObjectType::Schema,
            objects_v22::ObjectType::Func => objects_v23::ObjectType::Func,
        }
    }
}

impl From<objects_v22::DefaultPrivilegesKey> for objects_v23::DefaultPrivilegesKey {
    fn from(key: objects_v22::DefaultPrivilegesKey) -> Self {
        objects_v23::DefaultPrivilegesKey {
            role_id: key.role_id.map(Into::into),
            database_id: key.database_id.map(Into::into),
            schema_id: key.schema_id.map(Into::into),
            object_type: key.object_type,
            grantee: key.grantee.map(Into::into),
        }
    }
}

impl From<objects_v22::AclMode> for objects_v23::AclMode {
    fn from(acl_mode: objects_v22::AclMode) -> Self {
        objects_v23::AclMode {
            bitflags: acl_mode.bitflags,
        }
    }
}

impl From<objects_v22::DefaultPrivilegesValue> for objects_v23::DefaultPrivilegesValue {
    fn from(value: objects_v22::DefaultPrivilegesValue) -> Self {
        objects_v23::DefaultPrivilegesValue {
            privileges: value.privileges.map(Into::into),
        }
    }
}

impl From<objects_v22::ClusterId> for objects_v23::ClusterId {
    fn from(cluster_id: objects_v22::ClusterId) -> Self {
        let value = match cluster_id.value {
            None => None,
            Some(objects_v22::cluster_id::Value::System(id)) => {
                Some(objects_v23::cluster_id::Value::System(id))
            }
            Some(objects_v22::cluster_id::Value::User(id)) => {
                Some(objects_v23::cluster_id::Value::User(id))
            }
        };
        Self { value }
    }
}

impl From<objects_v22::MzAclItem> for objects_v23::MzAclItem {
    fn from(item: objects_v22::MzAclItem) -> Self {
        Self {
            grantee: item.grantee.map(Into::into),
            grantor: item.grantor.map(Into::into),
            acl_mode: item.acl_mode.map(Into::into),
        }
    }
}

impl From<objects_v22::ClusterKey> for objects_v23::ClusterKey {
    fn from(key: objects_v22::ClusterKey) -> Self {
        Self {
            id: key.id.map(Into::into),
        }
    }
}

impl From<objects_v22::Duration> for objects_v23::Duration {
    fn from(duration: objects_v22::Duration) -> Self {
        Self {
            secs: duration.secs,
            nanos: duration.nanos,
        }
    }
}

impl From<objects_v22::ReplicaLogging> for objects_v23::ReplicaLogging {
    fn from(logging: objects_v22::ReplicaLogging) -> Self {
        Self {
            log_logging: logging.log_logging,
            interval: logging.interval.map(Into::into),
        }
    }
}

impl From<objects_v22::ReplicaMergeEffort> for objects_v23::ReplicaMergeEffort {
    fn from(merge_effort: objects_v22::ReplicaMergeEffort) -> Self {
        Self {
            effort: merge_effort.effort,
        }
    }
}

impl From<objects_v22::cluster_config::ManagedCluster>
    for objects_v23::cluster_config::ManagedCluster
{
    fn from(managed_cluster: objects_v22::cluster_config::ManagedCluster) -> Self {
        Self {
            size: managed_cluster.size,
            replication_factor: managed_cluster.replication_factor,
            availability_zones: managed_cluster.availability_zones,
            logging: managed_cluster.logging.map(Into::into),
            idle_arrangement_merge_effort: managed_cluster
                .idle_arrangement_merge_effort
                .map(Into::into),
        }
    }
}

impl From<objects_v22::cluster_config::Variant> for objects_v23::cluster_config::Variant {
    fn from(variant: objects_v22::cluster_config::Variant) -> Self {
        match variant {
            objects_v22::cluster_config::Variant::Unmanaged(_) => {
                Self::Unmanaged(Default::default())
            }
            objects_v22::cluster_config::Variant::Managed(managed_cluster) => {
                Self::Managed(managed_cluster.into())
            }
        }
    }
}

impl From<objects_v22::ClusterConfig> for objects_v23::ClusterConfig {
    fn from(config: objects_v22::ClusterConfig) -> Self {
        Self {
            variant: config.variant.map(Into::into),
        }
    }
}

impl From<objects_v22::GlobalId> for objects_v23::GlobalId {
    fn from(id: objects_v22::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v22::global_id::Value::User(id)) => {
                Some(objects_v23::global_id::Value::User(id))
            }
            Some(objects_v22::global_id::Value::System(id)) => {
                Some(objects_v23::global_id::Value::System(id))
            }
            Some(objects_v22::global_id::Value::Transient(id)) => {
                Some(objects_v23::global_id::Value::Transient(id))
            }
            Some(objects_v22::global_id::Value::Explain(_)) => {
                Some(objects_v23::global_id::Value::Explain(Default::default()))
            }
        };
        Self { value }
    }
}

impl From<objects_v22::ClusterValue> for objects_v23::ClusterValue {
    fn from(cluster: objects_v22::ClusterValue) -> Self {
        Self {
            name: cluster.name,
            owner_id: cluster.owner_id.map(Into::into),
            privileges: cluster.privileges.into_iter().map(Into::into).collect(),
            linked_object_id: cluster.linked_object_id.map(Into::into),
            config: cluster.config.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v22_to_v23::{
        objects_v22, objects_v23, ACL_MODE_USAGE, MZ_INTROSPECTION_ROLE_ID, PUBLIC_ROLE_ID,
    };
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const ACL_MODE_USAGE_V22: objects_v22::AclMode = objects_v22::AclMode { bitflags: 1 << 8 };
        const OWNER_ROLE_ID_V22: objects_v22::RoleId = objects_v22::RoleId {
            value: Some(objects_v22::role_id::Value::User(1)),
        };
        const OWNER_ROLE_ID_V23: objects_v23::RoleId = objects_v23::RoleId {
            value: Some(objects_v23::role_id::Value::User(1)),
        };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a cluster.
        let clusters_v22: TypedCollection<objects_v22::ClusterKey, objects_v22::ClusterValue> =
            TypedCollection::new("compute_instance");
        clusters_v22
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v22::ClusterKey {
                        id: Some(objects_v22::ClusterId {
                            value: Some(objects_v22::cluster_id::Value::User(42)),
                        }),
                    },
                    objects_v22::ClusterValue {
                        name: "dev".into(),
                        owner_id: Some(OWNER_ROLE_ID_V22),
                        privileges: vec![objects_v22::MzAclItem {
                            grantee: Some(objects_v22::RoleId {
                                value: Some(objects_v22::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V22),
                            acl_mode: Some(ACL_MODE_USAGE_V22),
                        }],
                        linked_object_id: None,
                        config: Some(objects_v22::ClusterConfig {
                            variant: Some(objects_v22::cluster_config::Variant::Unmanaged(
                                Default::default(),
                            )),
                        }),
                    },
                )],
            )
            .await
            .unwrap();

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

        // Read back the default privileges.
        let default_privileges: TypedCollection<
            objects_v23::DefaultPrivilegesKey,
            objects_v23::DefaultPrivilegesValue,
        > = TypedCollection::new("default_privileges");
        let privileges = default_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert!(privileges.contains(&(
            objects_v23::DefaultPrivilegesKey {
                role_id: Some(PUBLIC_ROLE_ID),
                database_id: None,
                schema_id: None,
                object_type: objects_v23::ObjectType::Cluster.into(),
                grantee: Some(MZ_INTROSPECTION_ROLE_ID),
            },
            objects_v23::DefaultPrivilegesValue {
                privileges: Some(ACL_MODE_USAGE),
            }
        )));

        // Read back the cluster.
        let clusters_v23: TypedCollection<objects_v23::ClusterKey, objects_v23::ClusterValue> =
            TypedCollection::new("compute_instance");
        let clusters = clusters_v23.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let mut clusters_v23: Vec<_> = clusters
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();
        clusters_v23.sort();

        assert_eq!(
            clusters_v23,
            vec![(
                objects_v23::ClusterKey {
                    id: Some(objects_v23::ClusterId {
                        value: Some(objects_v23::cluster_id::Value::User(42)),
                    }),
                },
                objects_v23::ClusterValue {
                    name: "dev".into(),
                    owner_id: Some(OWNER_ROLE_ID_V23),
                    privileges: vec![
                        objects_v23::MzAclItem {
                            grantee: Some(objects_v23::RoleId {
                                value: Some(objects_v23::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V23),
                            acl_mode: Some(ACL_MODE_USAGE),
                        },
                        objects_v23::MzAclItem {
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                            grantor: Some(OWNER_ROLE_ID_V23),
                            acl_mode: Some(ACL_MODE_USAGE),
                        }
                    ],
                    linked_object_id: None,
                    config: Some(objects_v23::ClusterConfig {
                        variant: Some(objects_v23::cluster_config::Variant::Unmanaged(
                            Default::default(),
                        )),
                    }),
                },
            )],
        );
    }
}
