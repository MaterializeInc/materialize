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

pub mod v33 {
    include!(concat!(env!("OUT_DIR"), "/objects_v33.rs"));
}

pub mod v34 {
    include!(concat!(env!("OUT_DIR"), "/objects_v34.rs"));
}

wire_compatible!(v33::ClusterId with v34::ClusterId);
wire_compatible!(v33::ClusterReplicaValue with v34::ClusterReplicaValue);
wire_compatible!(v33::IdAllocKey with v34::IdAllocKey);
wire_compatible!(v33::IdAllocValue with v34::IdAllocValue);
wire_compatible!(v33::AuditLogKey with v34::AuditLogKey);

const CLUSTER_COLLECTION: TypedCollection<v33::ClusterKey, v33::ClusterValue> =
    TypedCollection::new("compute_instance");

const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    v33::ClusterReplicaKey,
    v33::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");

const ID_ALLOCATOR_COLLECTION: TypedCollection<v33::IdAllocKey, v33::IdAllocValue> =
    TypedCollection::new("id_alloc");

const AUDIT_LOG_COLLECTION: TypedCollection<v33::AuditLogKey, ()> =
    TypedCollection::new("audit_log");

const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";

/// Migrate replica IDs from 'NNN' to 'uNNN'/'sNNN' format.
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    let now = mz_ore::now::SYSTEM_TIME();

    initialize_system_replica_id_allocator(tx).await?;
    let replicas = migrate_cluster_replicas(tx).await?;
    migrate_audit_log(tx, replicas, now).await
}

/// Initialize the "system_replica" ID allocator.
async fn initialize_system_replica_id_allocator(
    tx: &mut Transaction<'_>,
) -> Result<(), StashError> {
    let coll = CLUSTER_REPLICA_COLLECTION.from_tx(tx).await?;
    let mut max_system_id = 0;
    for (_, value) in tx.peek_one(coll).await? {
        let cluster_id = value.cluster_id.unwrap().value.unwrap();
        if let v33::cluster_id::Value::System(id) = cluster_id {
            max_system_id = std::cmp::max(max_system_id, id);
        }
    }
    let next_id = max_system_id + 1;

    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |entries| {
            let key = v34::IdAllocKey {
                name: SYSTEM_REPLICA_ID_ALLOC_KEY.into(),
            };
            assert!(!entries.contains_key(&key));
            let value = v34::IdAllocValue { next_id };
            vec![MigrationAction::Insert(key, value)]
        })
        .await
}

/// Migrate replica IDs in the "cluster_replicas" collection.
async fn migrate_cluster_replicas(
    tx: &mut Transaction<'_>,
) -> Result<Vec<(u64, v34::ClusterReplicaKey, v34::ClusterReplicaValue)>, StashError> {
    let mut add_to_audit_log = Vec::new();

    let action = |(key, value): (&v33::ClusterReplicaKey, &v33::ClusterReplicaValue)| {
        let old_id = key.id.as_ref().unwrap().value;
        let cluster_id = value.cluster_id.as_ref().unwrap().value.as_ref().unwrap();

        // Construct the new ID format based on the plain ID and the cluster type.
        // System clusters currently always have a single replica, so we can set the replica ID to
        // the cluster ID. This does not work for user clusters.
        let new_id = match cluster_id {
            v33::cluster_id::Value::User(_) => v34::replica_id::Value::User(old_id),
            v33::cluster_id::Value::System(id) => v34::replica_id::Value::System(*id),
        };

        let new_key = v34::ClusterReplicaKey {
            id: Some(v34::ReplicaId {
                value: Some(new_id),
            }),
        };
        let new_value: v34::ClusterReplicaValue = WireCompatible::convert(value.clone());

        // Track the new entries we should add to the audit log.
        add_to_audit_log.push((old_id, new_key.clone(), new_value.clone()));

        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, v34::ClusterReplicaValue>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await?;

    Ok(add_to_audit_log)
}

/// Migrate the audit log to the new replica ID format.
///
/// Instead of replacing existing log entries, we add `drop` events for entries with the old ID
/// format and corresponding `create` events with the new format. This way we avoid confusing
/// consumers of the audit log that might have stashed old events at some external place.
async fn migrate_audit_log(
    tx: &mut Transaction<'_>,
    entries: Vec<(u64, v34::ClusterReplicaKey, v34::ClusterReplicaValue)>,
    now: u64,
) -> Result<(), StashError> {
    // Read in all of the active clusters so we can get the name for the audit log.
    let clusters = tx
        .peek_one::<v33::ClusterKey, v33::ClusterValue>(
            tx.collection(CLUSTER_COLLECTION.name()).await?,
        )
        .await?;

    // We need to allocate additional audit events, which need IDs. Gather the current next_id.
    let coll = ID_ALLOCATOR_COLLECTION.from_tx(tx).await?;
    let key = v33::IdAllocKey {
        name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
    };
    let mut next_audit_log_id = tx.peek_key_one(coll, &key).await?.unwrap().next_id;

    let mut new_events = Vec::with_capacity(entries.len());
    for (old_id, new_key, new_value) in entries {
        let cluster_id = match new_value.cluster_id {
            Some(v34::ClusterId {
                value: Some(v34::cluster_id::Value::User(id)),
            }) => format!("u{id}"),
            Some(v34::ClusterId {
                value: Some(v34::cluster_id::Value::System(id)),
            }) => format!("s{id}"),
            Some(v34::ClusterId { value: None }) | None => "".to_string(),
        };
        let cluster_name = clusters
            .get(&v33::ClusterKey {
                id: new_value.cluster_id.map(WireCompatible::convert),
            })
            .map(|v| v.name.clone())
            .unwrap_or_default();
        let new_replica_id = match new_key.id {
            Some(v34::ReplicaId {
                value: Some(v34::replica_id::Value::User(id)),
            }) => format!("u{id}"),
            Some(v34::ReplicaId {
                value: Some(v34::replica_id::Value::System(id)),
            }) => format!("s{id}"),
            Some(v34::ReplicaId { value: None }) | None => "".to_string(),
        };
        let (logical_size, disk) =
            match new_value.config {
                Some(v34::ReplicaConfig {
                    location:
                        Some(v34::replica_config::Location::Managed(
                            v34::replica_config::ManagedLocation { size, disk, .. },
                        )),
                    ..
                }) => (size, disk),
                _ => ("".to_string(), false),
            };

        let drop_event = v34::AuditLogEventV1 {
            id: next_audit_log_id,
            event_type: v34::audit_log_event_v1::EventType::Drop.into(),
            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
            user: None,
            occurred_at: Some(v34::EpochMillis { millis: now }),
            details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                v34::audit_log_event_v1::DropClusterReplicaV1 {
                    cluster_id: cluster_id.clone(),
                    cluster_name: cluster_name.clone(),
                    replica_id: Some(v34::StringWrapper {
                        inner: old_id.to_string(),
                    }),
                    replica_name: new_value.name.clone(),
                },
            )),
        };
        new_events.push(v34::AuditLogKey {
            event: Some(v34::audit_log_key::Event::V1(drop_event)),
        });

        let create_event = v34::AuditLogEventV1 {
            id: next_audit_log_id + 1,
            event_type: v34::audit_log_event_v1::EventType::Create.into(),
            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
            user: None,
            occurred_at: Some(v34::EpochMillis { millis: now }),
            details: Some(v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                v34::audit_log_event_v1::CreateClusterReplicaV1 {
                    cluster_id: cluster_id.clone(),
                    cluster_name: cluster_name.clone(),
                    replica_id: Some(v34::StringWrapper {
                        inner: new_replica_id,
                    }),
                    replica_name: new_value.name.clone(),
                    logical_size,
                    disk,
                },
            )),
        };
        new_events.push(v34::AuditLogKey {
            event: Some(v34::audit_log_key::Event::V1(create_event)),
        });

        next_audit_log_id += 2;
    }

    AUDIT_LOG_COLLECTION
        .migrate_compat(tx, |_| {
            new_events
                .into_iter()
                .map(|e| MigrationAction::Insert(e, ()))
                .collect()
        })
        .await?;

    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |_| {
            let key = v34::IdAllocKey {
                name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
            };
            let value = v34::IdAllocValue {
                next_id: next_audit_log_id,
            };
            vec![MigrationAction::Update(key.clone(), (key, value))]
        })
        .await
}

#[cfg(test)]
mod tests {
    use crate::DebugStashFactory;

    use super::*;

    const CLUSTER_REPLICA_COLLECTION_V34: TypedCollection<
        v34::ClusterReplicaKey,
        v34::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    const ID_ALLOCATOR_COLLECTION_V34: TypedCollection<v34::IdAllocKey, v34::IdAllocValue> =
        TypedCollection::new("id_alloc");

    const AUDIT_LOG_COLLECTION_V34: TypedCollection<v34::AuditLogKey, ()> =
        TypedCollection::new("audit_log");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        CLUSTER_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        v33::ClusterKey {
                            id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::System(5)),
                            }),
                        },
                        v33::ClusterValue {
                            name: "introspection".to_string(),
                            ..Default::default()
                        },
                    ),
                    (
                        v33::ClusterKey {
                            id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::User(7)),
                            }),
                        },
                        v33::ClusterValue {
                            name: "foo".to_string(),
                            ..Default::default()
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        CLUSTER_REPLICA_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        v33::ClusterReplicaKey {
                            id: Some(v33::ReplicaId { value: 100 }),
                        },
                        v33::ClusterReplicaValue {
                            cluster_id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::System(5)),
                            }),
                            config: None,
                            name: "system".into(),
                            owner_id: None,
                        },
                    ),
                    (
                        v33::ClusterReplicaKey {
                            id: Some(v33::ReplicaId { value: 200 }),
                        },
                        v33::ClusterReplicaValue {
                            cluster_id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::User(7)),
                            }),
                            config: None,
                            name: "user".into(),
                            owner_id: None,
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        AUDIT_LOG_COLLECTION
            .insert_without_overwrite(&mut stash, vec![])
            .await
            .unwrap();

        ID_ALLOCATOR_COLLECTION
            .insert_key_without_overwrite(
                &mut stash,
                v33::IdAllocKey {
                    name: AUDIT_LOG_ID_ALLOC_KEY.into(),
                },
                v33::IdAllocValue { next_id: 1 },
            )
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut replicas: Vec<_> = CLUSTER_REPLICA_COLLECTION_V34
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .collect();
        replicas.sort();

        assert_eq!(
            replicas,
            vec![
                (
                    v34::ClusterReplicaKey {
                        id: Some(v34::ReplicaId {
                            value: Some(v34::replica_id::Value::System(5))
                        }),
                    },
                    v34::ClusterReplicaValue {
                        cluster_id: Some(v34::ClusterId {
                            value: Some(v34::cluster_id::Value::System(5)),
                        }),
                        config: None,
                        name: "system".into(),
                        owner_id: None,
                    },
                ),
                (
                    v34::ClusterReplicaKey {
                        id: Some(v34::ReplicaId {
                            value: Some(v34::replica_id::Value::User(200))
                        }),
                    },
                    v34::ClusterReplicaValue {
                        cluster_id: Some(v34::ClusterId {
                            value: Some(v34::cluster_id::Value::User(7)),
                        }),
                        config: None,
                        name: "user".into(),
                        owner_id: None,
                    },
                ),
            ]
        );

        let key = v34::IdAllocKey {
            name: SYSTEM_REPLICA_ID_ALLOC_KEY.into(),
        };
        let next_system_replica_id = ID_ALLOCATOR_COLLECTION_V34
            .peek_key_one(&mut stash, key)
            .await
            .unwrap();

        assert_eq!(
            next_system_replica_id,
            Some(v34::IdAllocValue { next_id: 6 })
        );

        let mut audit_log_events: Vec<_> = AUDIT_LOG_COLLECTION_V34
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(e, ())| e.event)
            .collect();
        audit_log_events.sort();

        // The test uses the real time, so we get the time from the first event.
        let v34::audit_log_key::Event::V1(v34::AuditLogEventV1 { occurred_at, .. }) =
            &audit_log_events[0];

        let expected_events = vec![
            v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                id: 1,
                event_type: v34::audit_log_event_v1::EventType::Drop.into(),
                object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                user: None,
                occurred_at: occurred_at.clone(),
                details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                    v34::audit_log_event_v1::DropClusterReplicaV1 {
                        cluster_id: "s5".to_string(),
                        cluster_name: "introspection".to_string(),
                        replica_id: Some(v34::StringWrapper {
                            inner: "100".to_string(),
                        }),
                        replica_name: "system".to_string(),
                    },
                )),
            }),
            v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                id: 2,
                event_type: v34::audit_log_event_v1::EventType::Create.into(),
                object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                user: None,
                occurred_at: occurred_at.clone(),
                details: Some(v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                    v34::audit_log_event_v1::CreateClusterReplicaV1 {
                        cluster_id: "s5".to_string(),
                        cluster_name: "introspection".to_string(),
                        replica_id: Some(v34::StringWrapper {
                            inner: "s5".to_string(),
                        }),
                        replica_name: "system".to_string(),
                        logical_size: "".to_string(),
                        disk: false,
                    },
                )),
            }),
            v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                id: 3,
                event_type: v34::audit_log_event_v1::EventType::Drop.into(),
                object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                user: None,
                occurred_at: occurred_at.clone(),
                details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                    v34::audit_log_event_v1::DropClusterReplicaV1 {
                        cluster_id: "u7".to_string(),
                        cluster_name: "foo".to_string(),
                        replica_id: Some(v34::StringWrapper {
                            inner: "200".to_string(),
                        }),
                        replica_name: "user".to_string(),
                    },
                )),
            }),
            v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                id: 4,
                event_type: v34::audit_log_event_v1::EventType::Create.into(),
                object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                user: None,
                occurred_at: occurred_at.clone(),
                details: Some(v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                    v34::audit_log_event_v1::CreateClusterReplicaV1 {
                        cluster_id: "u7".to_string(),
                        cluster_name: "foo".to_string(),
                        replica_id: Some(v34::StringWrapper {
                            inner: "u200".to_string(),
                        }),
                        replica_name: "user".to_string(),
                        logical_size: "".to_string(),
                        disk: false,
                    },
                )),
            }),
        ];

        similar_asserts::assert_eq!(audit_log_events, expected_events);
    }
}
