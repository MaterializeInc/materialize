// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};
use std::collections::BTreeMap;

pub mod objects_v33 {
    include!(concat!(env!("OUT_DIR"), "/objects_v33.rs"));
}

pub mod objects_v34 {
    include!(concat!(env!("OUT_DIR"), "/objects_v34.rs"));
}

wire_compatible!(objects_v33::ClusterReplicaValue with objects_v34::ClusterReplicaValue);
wire_compatible!(objects_v33::IdAllocKey with objects_v34::IdAllocKey);
wire_compatible!(objects_v33::IdAllocValue with objects_v34::IdAllocValue);
wire_compatible!(objects_v33::AuditLogKey with objects_v34::AuditLogKey);

/// Rename mz_introspection to mz_support
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    let now = mz_ore::now::SYSTEM_TIME();

    pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
        objects_v33::ClusterReplicaKey,
        objects_v33::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    pub const ID_ALLOCATOR_COLLECTION: TypedCollection<
        objects_v33::IdAllocKey,
        objects_v33::IdAllocValue,
    > = TypedCollection::new("id_alloc");

    pub const AUDIT_LOG_COLLECTION: TypedCollection<objects_v33::AuditLogKey, ()> =
        TypedCollection::new("audit_log");

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, objects_v34::ClusterReplicaValue>(tx, |entries| {
            let mut updates = Vec::new();
            for (key, value) in entries {
                let new_key = match (key, &value.cluster_id) {
                    // Re-use IDs of user clusters
                    (
                        objects_v33::ClusterReplicaKey {
                            id: Some(objects_v33::ReplicaId { value: replica_id }),
                        },
                        Some(objects_v33::ClusterId {
                            value: Some(objects_v33::cluster_id::Value::User(_)),
                        }),
                    ) => objects_v34::replica_id::Value::User(*replica_id),
                    // For system clusters, assign their cluster id. This requires that any system
                    // has at most one replica.
                    (
                        _,
                        Some(objects_v33::ClusterId {
                            value: Some(objects_v33::cluster_id::Value::System(cluster_id)),
                        }),
                    ) => objects_v34::replica_id::Value::System(*cluster_id),
                    _ => {
                        panic!("Incorrect cluster replica data in stash {key:?} -> {value:?}")
                    }
                };
                let new_key = objects_v34::ReplicaId {
                    value: Some(new_key),
                };
                updates.push(MigrationAction::Update(
                    key.clone(),
                    (new_key, WireCompatible::convert(value)),
                ));
            }
            updates
        })
        .await?;

    const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
    const DEFAULT_SYSTEM_REPLICA_ID: u64 = 1;
    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |entries| {
            let mut updates = Vec::new();
            let key = objects_v34::IdAllocKey {
                name: SYSTEM_REPLICA_ID_ALLOC_KEY.to_string(),
            };
            if entries.get(&key).is_none() {
                updates.push(MigrationAction::Insert(
                    key,
                    objects_v34::IdAllocValue {
                        next_id: DEFAULT_SYSTEM_REPLICA_ID + 1,
                    },
                ));
            }
            updates
        })
        .await?;

    // We need to allocate additional audit events, which need IDs. Gather the current next_id.
    const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
    let audit_log_id_alloc_key = objects_v33::IdAllocKey {
        name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
    };
    let mut next_audit_log_id = if let Some(value) = tx
        .peek_key_one(
            ID_ALLOCATOR_COLLECTION.from_tx(tx).await?,
            &objects_v33::IdAllocKey {
                name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
            },
        )
        .await?
    {
        value.next_id
    } else {
        return Err("Absent value for auditlog ID allocator".into());
    };

    let mut live_replicas = BTreeMap::new();
    for ((key, ()), _time, _diff) in tx.iter(AUDIT_LOG_COLLECTION.from_tx(tx).await?).await? {
        let (event_type, details) =
            if let Some(objects_v33::audit_log_key::Event::V1(objects_v33::AuditLogEventV1 {
                details: Some(details),
                event_type,
                ..
            })) = key.event
            {
                (event_type, details)
            } else {
                continue;
            };

        match &details {
            objects_v33::audit_log_event_v1::Details::CreateClusterReplicaV1(event) => {
                assert_eq!(
                    event_type,
                    objects_v33::audit_log_event_v1::EventType::Create as i32,
                );
                let key = (event.cluster_id.clone(), event.replica_id.clone());
                let prev = live_replicas.insert(key, event.clone());
                assert_eq!(prev, None, "replica created twice");
            }
            objects_v33::audit_log_event_v1::Details::DropClusterReplicaV1(event) => {
                assert_eq!(
                    event_type,
                    objects_v33::audit_log_event_v1::EventType::Drop as i32,
                );
                let key = (event.cluster_id.clone(), event.replica_id.clone());
                let prev = live_replicas.remove(&key);
                assert!(prev.is_some(), "non-existing replica dropped");
            }
            _ => continue,
        };
    }

    let mut updates = Vec::with_capacity(live_replicas.len() * 2);
    for (_, details) in live_replicas {
        let old_id = details.replica_id.expect("live replicas have IDs");
        let Ok(numeric_id) = old_id.inner.parse::<u64>() else {
            continue;  // replica_id is already in the new format
        };

        // We should only see user clusters in the audit log, but there is no harm in
        // handling system clusters too, just to be sure.
        assert_eq!(
            details.cluster_id.chars().nth(0),
            Some('u'),
            "Can only migrate user clusters in audit log"
        );
        let new_id = format!("u{numeric_id}");

        let drop_event = objects_v34::AuditLogEventV1 {
            id: next_audit_log_id,
            event_type: objects_v34::audit_log_event_v1::EventType::Drop as i32,
            object_type: objects_v34::audit_log_event_v1::ObjectType::ClusterReplica as i32,
            details: Some(
                objects_v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                    objects_v34::audit_log_event_v1::DropClusterReplicaV1 {
                        cluster_id: details.cluster_id.clone(),
                        cluster_name: details.cluser_name.clone(),
                        replica_id: Some(objects_v34::StringWrapper {
                            inner: old_id.inner,
                        }),
                        replica_name: details.replica_name.clone(),
                    },
                ),
            ),
            user: None,
            occurred_at: Some(objects_v34::EpochMillis { millis: now }),
        };
        let create_event = objects_v34::AuditLogEventV1 {
            id: next_audit_log_id + 1,
            event_type: objects_v34::audit_log_event_v1::EventType::Create as i32,
            object_type: objects_v34::audit_log_event_v1::ObjectType::ClusterReplica as i32,
            details: Some(
                objects_v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                    objects_v34::audit_log_event_v1::CreateClusterReplicaV1 {
                        cluster_id: details.cluster_id,
                        cluser_name: details.cluser_name,
                        replica_id: Some(objects_v34::StringWrapper { inner: new_id }),
                        replica_name: details.replica_name,
                        logical_size: details.logical_size,
                        disk: details.disk,
                    },
                ),
            ),
            user: None,
            occurred_at: Some(objects_v34::EpochMillis { millis: now }),
        };

        updates.push(MigrationAction::Insert(
            objects_v34::AuditLogKey {
                event: Some(objects_v34::audit_log_key::Event::V1(drop_event)),
            },
            (),
        ));
        updates.push(MigrationAction::Insert(
            objects_v34::AuditLogKey {
                event: Some(objects_v34::audit_log_key::Event::V1(create_event)),
            },
            (),
        ));
        next_audit_log_id += 2;
    }

    AUDIT_LOG_COLLECTION
        .migrate_compat::<objects_v34::AuditLogKey, ()>(tx, |_entries| updates)
        .await?;

    let key = objects_v34::IdAllocKey {
        name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
    };

    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |_| {
            vec![MigrationAction::Update(
                key.clone(),
                (
                    key,
                    objects_v34::IdAllocValue {
                        next_id: next_audit_log_id,
                    },
                ),
            )]
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::upgrade;

    use crate::upgrade::v32_to_v33::{objects_v32, objects_v33, MZ_INTROSPECTION_ROLE_ID};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory: DebugStashFactory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        let roles_v32: TypedCollection<objects_v32::RoleKey, objects_v32::RoleValue> =
            TypedCollection::new("role");

        let system_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(1)),
        };
        let introspection_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(2)),
        };

        roles_v32
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        objects_v32::RoleKey {
                            id: Some(system_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_system".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                    (
                        objects_v32::RoleKey {
                            id: Some(introspection_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_introspection".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                ],
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

        let roles_v33: TypedCollection<objects_v33::RoleKey, objects_v33::RoleValue> =
            TypedCollection::new("role");
        let roles = roles_v33.iter(&mut stash).await.unwrap();

        let system_role_id_v33 = objects_v33::RoleId {
            value: Some(objects_v33::role_id::Value::System(1)),
        };
        let mut roles = roles
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect_vec();
        roles.sort();
        assert_eq!(
            roles,
            vec![
                (
                    objects_v33::RoleKey {
                        id: Some(system_role_id_v33),
                    },
                    objects_v33::RoleValue {
                        name: "mz_system".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                ),
                (
                    objects_v33::RoleKey {
                        id: Some(MZ_INTROSPECTION_ROLE_ID),
                    },
                    objects_v33::RoleValue {
                        name: "mz_support".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                )
            ]
        );
    }
}
