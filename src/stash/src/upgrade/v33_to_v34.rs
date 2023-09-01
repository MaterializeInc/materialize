// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::MigrationAction;
use crate::upgrade::{objects_v33 as v33, objects_v34 as v34};
use crate::{StashError, Transaction, TypedCollection};

wire_compatible!(v33::ClusterReplicaValue with v34::ClusterReplicaValue);
wire_compatible!(v33::IdAllocKey with v34::IdAllocKey);
wire_compatible!(v33::IdAllocValue with v34::IdAllocValue);
wire_compatible!(v33::AuditLogKey with v34::AuditLogKey);

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
    migrate_cluster_replicas(tx).await?;
    migrate_audit_log(tx, now).await
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
async fn migrate_cluster_replicas(tx: &mut Transaction<'_>) -> Result<(), StashError> {
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
        let new_value = WireCompatible::convert(value);
        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, v34::ClusterReplicaValue>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await
}

/// Migrate the audit log to the new replica ID format.
///
/// Instead of replacing existing log entries, we add `drop` events for entries with the old ID
/// format and corresponding `create` events with the new format. This way we avoid confusing
/// consumers of the audit log that might have stashed old events at some external place.
async fn migrate_audit_log(tx: &mut Transaction<'_>, now: u64) -> Result<(), StashError> {
    // We need to allocate additional audit events, which need IDs.
    // Gather the current next_id.
    let coll = ID_ALLOCATOR_COLLECTION.from_tx(tx).await?;
    let key = v33::IdAllocKey {
        name: AUDIT_LOG_ID_ALLOC_KEY.to_string(),
    };
    let mut next_audit_log_id = tx.peek_key_one(coll, &key).await?.unwrap().next_id;

    // Collect all existing audit log events.
    let coll = AUDIT_LOG_COLLECTION.from_tx(tx).await?;
    let audit_events = tx.peek_one(coll).await?;
    let mut events: Vec<_> = audit_events
        .into_keys()
        .map(|key| {
            let v33::audit_log_key::Event::V1(event) = key.event.unwrap();
            event
        })
        .collect();

    // Find live replicas by iterating through the events in order and keeping track of the
    // set of replicas not yet dropped. When every event is processed, this set contains
    // only the currently live replicas.
    events.sort_by_key(|e| e.id);
    let mut live_replicas = BTreeMap::new();
    for event in events {
        let details = event.details.unwrap();
        match details {
            v33::audit_log_event_v1::Details::CreateClusterReplicaV1(details) => {
                let Some(replica_id) = details.replica_id.clone() else {
                    tracing::info!("ignoring replica create event: {details:?}");
                    continue;
                };
                let key = (details.cluster_id.clone(), replica_id);
                live_replicas.insert(key, details);
            }
            v33::audit_log_event_v1::Details::DropClusterReplicaV1(details) => {
                let Some(replica_id) = details.replica_id.clone() else {
                    tracing::info!("ignoring replica drop event: {details:?}");
                    continue;
                };
                let key = (details.cluster_id, replica_id);
                live_replicas.remove(&key);
            }
            _ => (),
        };
    }

    // For each live replica, add a `drop` event with the old ID and a `create` event with
    // the new ID.
    let mut updates = Vec::with_capacity(live_replicas.len() * 2);
    for (_, details) in live_replicas {
        let old_id = details
            .replica_id
            .clone()
            .expect("filtered to only details with `replica_id`s above")
            .inner;
        let numeric_id = old_id.parse::<u64>().unwrap();

        // Same logic as in `migrate_cluster_replicas`.
        let new_id = match details.cluster_id.chars().next() {
            Some('u') => format!("u{numeric_id}"),
            Some('s') => details.cluster_id.clone(),
            _ => {
                tracing::error!("invalid cluster_ID: {details:?}");
                continue;
            }
        };

        let drop_event = v34::AuditLogEventV1 {
            id: next_audit_log_id,
            event_type: v34::audit_log_event_v1::EventType::Drop.into(),
            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
            details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                v34::audit_log_event_v1::DropClusterReplicaV1 {
                    cluster_id: details.cluster_id.clone(),
                    cluster_name: details.cluser_name.clone(),
                    replica_id: Some(v34::StringWrapper { inner: old_id }),
                    replica_name: details.replica_name.clone(),
                },
            )),
            user: None,
            occurred_at: Some(v34::EpochMillis { millis: now }),
        };
        let create_event = v34::AuditLogEventV1 {
            id: next_audit_log_id + 1,
            event_type: v34::audit_log_event_v1::EventType::Create.into(),
            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
            details: Some(v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                v34::audit_log_event_v1::CreateClusterReplicaV1 {
                    cluster_id: details.cluster_id,
                    cluster_name: details.cluser_name,
                    replica_id: Some(v34::StringWrapper { inner: new_id }),
                    replica_name: details.replica_name,
                    logical_size: details.logical_size,
                    disk: details.disk,
                },
            )),
            user: None,
            occurred_at: Some(v34::EpochMillis { millis: now }),
        };

        next_audit_log_id += 2;

        let events = [
            v34::AuditLogKey {
                event: Some(v34::audit_log_key::Event::V1(drop_event)),
            },
            v34::AuditLogKey {
                event: Some(v34::audit_log_key::Event::V1(create_event)),
            },
        ];
        let actions = events.into_iter().map(|e| MigrationAction::Insert(e, ()));
        updates.extend(actions);
    }

    AUDIT_LOG_COLLECTION.migrate_compat(tx, |_| updates).await?;

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
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn audit_log_migration() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        AUDIT_LOG_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        v33::AuditLogKey {
                            event: Some(v33::audit_log_key::Event::V1(v33::AuditLogEventV1 {
                                id: 1,
                                event_type: v33::audit_log_event_v1::EventType::Create.into(),
                                object_type: v33::audit_log_event_v1::ObjectType::ClusterReplica
                                    .into(),
                                details: Some(
                                    v33::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                        v33::audit_log_event_v1::CreateClusterReplicaV1 {
                                            cluster_id: "u1".into(),
                                            cluser_name: "c1".into(),
                                            replica_id: Some(v33::StringWrapper {
                                                inner: "1".into(),
                                            }),
                                            replica_name: "r1".into(),
                                            logical_size: "small".into(),
                                            disk: false,
                                        },
                                    ),
                                ),
                                user: None,
                                occurred_at: Some(v33::EpochMillis { millis: 100 }),
                            })),
                        },
                        (),
                    ),
                    (
                        v33::AuditLogKey {
                            event: Some(v33::audit_log_key::Event::V1(v33::AuditLogEventV1 {
                                id: 2,
                                event_type: v33::audit_log_event_v1::EventType::Create.into(),
                                object_type: v33::audit_log_event_v1::ObjectType::ClusterReplica
                                    .into(),
                                details: Some(
                                    v33::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                        v33::audit_log_event_v1::CreateClusterReplicaV1 {
                                            cluster_id: "u2".into(),
                                            cluser_name: "c2".into(),
                                            replica_id: Some(v33::StringWrapper {
                                                inner: "2".into(),
                                            }),
                                            replica_name: "r2".into(),
                                            logical_size: "big".into(),
                                            disk: true,
                                        },
                                    ),
                                ),
                                user: None,
                                occurred_at: Some(v33::EpochMillis { millis: 200 }),
                            })),
                        },
                        (),
                    ),
                    (
                        v33::AuditLogKey {
                            event: Some(v33::audit_log_key::Event::V1(v33::AuditLogEventV1 {
                                id: 3,
                                event_type: v33::audit_log_event_v1::EventType::Drop.into(),
                                object_type: v33::audit_log_event_v1::ObjectType::ClusterReplica
                                    .into(),
                                details: Some(
                                    v33::audit_log_event_v1::Details::DropClusterReplicaV1(
                                        v33::audit_log_event_v1::DropClusterReplicaV1 {
                                            cluster_id: "u1".into(),
                                            cluster_name: "c1".into(),
                                            replica_id: Some(v33::StringWrapper {
                                                inner: "1".into(),
                                            }),
                                            replica_name: "r1".into(),
                                        },
                                    ),
                                ),
                                user: None,
                                occurred_at: Some(v33::EpochMillis { millis: 300 }),
                            })),
                        },
                        (),
                    ),
                ],
            )
            .await
            .unwrap();

        ID_ALLOCATOR_COLLECTION
            .insert_key_without_overwrite(
                &mut stash,
                v33::IdAllocKey {
                    name: AUDIT_LOG_ID_ALLOC_KEY.into(),
                },
                v33::IdAllocValue { next_id: 4 },
            )
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    migrate_audit_log(&mut tx, 500).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut events: Vec<_> = AUDIT_LOG_COLLECTION_V34
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .collect();
        events.sort();

        assert_eq!(
            events,
            vec![
                (
                    v34::AuditLogKey {
                        event: Some(v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                            id: 1,
                            event_type: v34::audit_log_event_v1::EventType::Create.into(),
                            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                            details: Some(
                                v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                    v34::audit_log_event_v1::CreateClusterReplicaV1 {
                                        cluster_id: "u1".into(),
                                        cluster_name: "c1".into(),
                                        replica_id: Some(v34::StringWrapper { inner: "1".into() }),
                                        replica_name: "r1".into(),
                                        logical_size: "small".into(),
                                        disk: false,
                                    },
                                ),
                            ),
                            user: None,
                            occurred_at: Some(v34::EpochMillis { millis: 100 }),
                        })),
                    },
                    (),
                ),
                (
                    v34::AuditLogKey {
                        event: Some(v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                            id: 2,
                            event_type: v34::audit_log_event_v1::EventType::Create.into(),
                            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                            details: Some(
                                v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                    v34::audit_log_event_v1::CreateClusterReplicaV1 {
                                        cluster_id: "u2".into(),
                                        cluster_name: "c2".into(),
                                        replica_id: Some(v34::StringWrapper { inner: "2".into() }),
                                        replica_name: "r2".into(),
                                        logical_size: "big".into(),
                                        disk: true,
                                    },
                                ),
                            ),
                            user: None,
                            occurred_at: Some(v34::EpochMillis { millis: 200 }),
                        })),
                    },
                    (),
                ),
                (
                    v34::AuditLogKey {
                        event: Some(v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                            id: 3,
                            event_type: v34::audit_log_event_v1::EventType::Drop.into(),
                            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                            details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                                v34::audit_log_event_v1::DropClusterReplicaV1 {
                                    cluster_id: "u1".into(),
                                    cluster_name: "c1".into(),
                                    replica_id: Some(v34::StringWrapper { inner: "1".into() }),
                                    replica_name: "r1".into(),
                                },
                            )),
                            user: None,
                            occurred_at: Some(v34::EpochMillis { millis: 300 }),
                        })),
                    },
                    (),
                ),
                (
                    v34::AuditLogKey {
                        event: Some(v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                            id: 4,
                            event_type: v34::audit_log_event_v1::EventType::Drop.into(),
                            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                            details: Some(v34::audit_log_event_v1::Details::DropClusterReplicaV1(
                                v34::audit_log_event_v1::DropClusterReplicaV1 {
                                    cluster_id: "u2".into(),
                                    cluster_name: "c2".into(),
                                    replica_id: Some(v34::StringWrapper { inner: "2".into() }),
                                    replica_name: "r2".into(),
                                },
                            )),
                            user: None,
                            occurred_at: Some(v34::EpochMillis { millis: 500 }),
                        })),
                    },
                    (),
                ),
                (
                    v34::AuditLogKey {
                        event: Some(v34::audit_log_key::Event::V1(v34::AuditLogEventV1 {
                            id: 5,
                            event_type: v34::audit_log_event_v1::EventType::Create.into(),
                            object_type: v34::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                            details: Some(
                                v34::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                    v34::audit_log_event_v1::CreateClusterReplicaV1 {
                                        cluster_id: "u2".into(),
                                        cluster_name: "c2".into(),
                                        replica_id: Some(v34::StringWrapper { inner: "u2".into() }),
                                        replica_name: "r2".into(),
                                        logical_size: "big".into(),
                                        disk: true,
                                    },
                                )
                            ),
                            user: None,
                            occurred_at: Some(v34::EpochMillis { millis: 500 }),
                        })),
                    },
                    (),
                ),
            ]
        );
    }
}
