// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::{objects_v39 as v39, objects_v40 as v40, MigrationAction};
use crate::{StashError, Transaction, TypedCollection};

wire_compatible!(v39::ClusterId with v40::ClusterId);
wire_compatible!(v39::ClusterReplicaKey with v40::ClusterReplicaKey);
wire_compatible!(v39::EpochMillis with v40::EpochMillis);
wire_compatible!(v39::ReplicaLogging with v40::ReplicaLogging);
wire_compatible!(v39::ReplicaMergeEffort with v40::ReplicaMergeEffort);
wire_compatible!(v39::RoleId with v40::RoleId);
wire_compatible!(v39::StringWrapper with v40::StringWrapper);
wire_compatible!(v39::replica_config::UnmanagedLocation with v40::replica_config::UnmanagedLocation);

const AUDIT_LOG_COLLECTION: TypedCollection<v39::AuditLogKey, ()> =
    TypedCollection::new("audit_log");

pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    v39::ClusterReplicaKey,
    v39::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");

/// Migrate replicas to include internal/billed-as information.
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    migrate_cluster_replicas(tx).await?;
    migrate_audit_log(tx).await
}

/// Migrate replicas in the "cluster_replicas" collection.
async fn migrate_cluster_replicas(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    let action = |(key, value): (&v39::ClusterReplicaKey, &v39::ClusterReplicaValue)| {
        let new_key: v40::ClusterReplicaKey = WireCompatible::convert(key);
        let new_value = v40::ClusterReplicaValue {
            cluster_id: value.cluster_id.as_ref().map(WireCompatible::convert),
            config: value.config.as_ref().map(|config| {
                let logging = config.logging.as_ref().map(WireCompatible::convert);
                let idle_arrangement_merge_effort = config
                    .idle_arrangement_merge_effort
                    .as_ref()
                    .map(WireCompatible::convert);
                let location = config.location.as_ref().map(|config| match config.clone() {
                    v39::replica_config::Location::Managed(
                        v39::replica_config::ManagedLocation {
                            availability_zone,
                            disk,
                            size,
                        },
                    ) => v40::replica_config::Location::Managed(
                        v40::replica_config::ManagedLocation {
                            availability_zone,
                            disk,
                            size,
                            billed_as: None,
                            internal: false,
                        },
                    ),
                    v39::replica_config::Location::Unmanaged(unmanaged) => {
                        v40::replica_config::Location::Unmanaged(WireCompatible::convert(
                            &unmanaged,
                        ))
                    }
                });
                v40::ReplicaConfig {
                    logging,
                    idle_arrangement_merge_effort,
                    location,
                }
            }),
            name: value.name.clone(),
            owner_id: value.owner_id.as_ref().map(WireCompatible::convert),
        };
        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, v40::ClusterReplicaValue>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await
}

/// Migrate the audit log to the new replica billed-as and internal.
async fn migrate_audit_log(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    let action = |key: &v39::AuditLogKey| {
        let v39::audit_log_key::Event::V1(event) = key.event.as_ref()?;
        let details = event.details.as_ref()?;
        match details {
            v39::audit_log_event_v1::Details::CreateClusterReplicaV1(details) => {
                let create_event = v40::AuditLogEventV1 {
                    id: event.id,
                    event_type: v40::audit_log_event_v1::EventType::Create.into(),
                    object_type: v40::audit_log_event_v1::ObjectType::ClusterReplica.into(),
                    details: Some(v40::audit_log_event_v1::Details::CreateClusterReplicaV1(
                        v40::audit_log_event_v1::CreateClusterReplicaV1 {
                            cluster_id: details.cluster_id.clone(),
                            cluster_name: details.cluster_name.clone(),
                            replica_id: details.replica_id.as_ref().map(WireCompatible::convert),
                            replica_name: details.replica_name.clone(),
                            logical_size: details.logical_size.clone(),
                            disk: details.disk,
                            internal: false,
                            billed_as: None,
                        },
                    )),
                    user: event.user.as_ref().map(WireCompatible::convert),
                    occurred_at: event.occurred_at.as_ref().map(WireCompatible::convert),
                };

                let new_key = v40::AuditLogKey {
                    event: Some(v40::audit_log_key::Event::V1(create_event)),
                };
                Some(MigrationAction::Update(key.clone(), (new_key, ())))
            }
            _ => None,
        }
    };
    AUDIT_LOG_COLLECTION
        .migrate_to(tx, |entries| entries.keys().flat_map(action).collect())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DebugStashFactory;

    const AUDIT_LOG_COLLECTION_V40: TypedCollection<v40::AuditLogKey, ()> =
        TypedCollection::new("audit_log");

    pub const CLUSTER_REPLICA_COLLECTION_V40: TypedCollection<
        v40::ClusterReplicaKey,
        v40::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_cluster_replica_migration() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open().await;

        CLUSTER_REPLICA_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                [
                    (
                        v39::ClusterReplicaKey {
                            id: Some(v39::ReplicaId {
                                value: Some(v39::replica_id::Value::User(123)),
                            }),
                        },
                        v39::ClusterReplicaValue {
                            cluster_id: Some(v39::ClusterId {
                                value: Some(v39::cluster_id::Value::User(456)),
                            }),
                            config: Some(v39::ReplicaConfig {
                                idle_arrangement_merge_effort: Some(v39::ReplicaMergeEffort {
                                    effort: 321,
                                }),
                                location: Some(v39::replica_config::Location::Managed(
                                    v39::replica_config::ManagedLocation {
                                        availability_zone: Some("unavailability_zone".to_owned()),
                                        disk: false,
                                        size: "huge".to_owned(),
                                    },
                                )),
                                logging: Some(v39::ReplicaLogging {
                                    interval: Some(v39::Duration {
                                        nanos: 1234,
                                        secs: 13,
                                    }),
                                    log_logging: true,
                                }),
                            }),
                            name: "moritz".to_owned(),
                            owner_id: Some(v39::RoleId {
                                value: Some(v39::role_id::Value::User(987)),
                            }),
                        },
                    ),
                    (
                        v39::ClusterReplicaKey {
                            id: Some(v39::ReplicaId {
                                value: Some(v39::replica_id::Value::User(234)),
                            }),
                        },
                        v39::ClusterReplicaValue {
                            cluster_id: Some(v39::ClusterId {
                                value: Some(v39::cluster_id::Value::User(345)),
                            }),
                            config: Some(v39::ReplicaConfig {
                                idle_arrangement_merge_effort: Some(v39::ReplicaMergeEffort {
                                    effort: 432,
                                }),
                                location: Some(v39::replica_config::Location::Managed(
                                    v39::replica_config::ManagedLocation {
                                        availability_zone: Some("Verfügbar".to_owned()),
                                        disk: false,
                                        size: "groß".to_owned(),
                                    },
                                )),
                                logging: Some(v39::ReplicaLogging {
                                    interval: Some(v39::Duration {
                                        nanos: 4312,
                                        secs: 11,
                                    }),
                                    log_logging: true,
                                }),
                            }),
                            name: "someone".to_owned(),
                            owner_id: Some(v39::RoleId {
                                value: Some(v39::role_id::Value::User(876)),
                            }),
                        },
                    ),
                ],
            )
            .await
            .expect("insert success");

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

        let roles = CLUSTER_REPLICA_COLLECTION_V40
            .peek_one(&mut stash)
            .await
            .expect("read v40");
        insta::assert_debug_snapshot!(roles, @r###"
        {
            ClusterReplicaKey {
                id: Some(
                    ReplicaId {
                        value: Some(
                            User(
                                123,
                            ),
                        ),
                    },
                ),
            }: ClusterReplicaValue {
                cluster_id: Some(
                    ClusterId {
                        value: Some(
                            User(
                                456,
                            ),
                        ),
                    },
                ),
                name: "moritz",
                config: Some(
                    ReplicaConfig {
                        logging: Some(
                            ReplicaLogging {
                                log_logging: true,
                                interval: Some(
                                    Duration {
                                        secs: 13,
                                        nanos: 1234,
                                    },
                                ),
                            },
                        ),
                        idle_arrangement_merge_effort: Some(
                            ReplicaMergeEffort {
                                effort: 321,
                            },
                        ),
                        location: Some(
                            Managed(
                                ManagedLocation {
                                    size: "huge",
                                    availability_zone: Some(
                                        "unavailability_zone",
                                    ),
                                    disk: false,
                                    internal: false,
                                    billed_as: None,
                                },
                            ),
                        ),
                    },
                ),
                owner_id: Some(
                    RoleId {
                        value: Some(
                            User(
                                987,
                            ),
                        ),
                    },
                ),
            },
            ClusterReplicaKey {
                id: Some(
                    ReplicaId {
                        value: Some(
                            User(
                                234,
                            ),
                        ),
                    },
                ),
            }: ClusterReplicaValue {
                cluster_id: Some(
                    ClusterId {
                        value: Some(
                            User(
                                345,
                            ),
                        ),
                    },
                ),
                name: "someone",
                config: Some(
                    ReplicaConfig {
                        logging: Some(
                            ReplicaLogging {
                                log_logging: true,
                                interval: Some(
                                    Duration {
                                        secs: 11,
                                        nanos: 4312,
                                    },
                                ),
                            },
                        ),
                        idle_arrangement_merge_effort: Some(
                            ReplicaMergeEffort {
                                effort: 432,
                            },
                        ),
                        location: Some(
                            Managed(
                                ManagedLocation {
                                    size: "groß",
                                    availability_zone: Some(
                                        "Verfügbar",
                                    ),
                                    disk: false,
                                    internal: false,
                                    billed_as: None,
                                },
                            ),
                        ),
                    },
                ),
                owner_id: Some(
                    RoleId {
                        value: Some(
                            User(
                                876,
                            ),
                        ),
                    },
                ),
            },
        }
        "###);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_audit_log_migration() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open().await;

        AUDIT_LOG_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                [(
                    v39::AuditLogKey {
                        event: Some(v39::audit_log_key::Event::V1(v39::AuditLogEventV1 {
                            id: 1234,
                            user: Some(v39::StringWrapper {
                                inner: "name".to_owned(),
                            }),
                            event_type: 2,
                            object_type: 3,
                            details: Some(
                                v39::audit_log_event_v1::Details::CreateClusterReplicaV1(
                                    v39::audit_log_event_v1::CreateClusterReplicaV1 {
                                        cluster_id: "u23".to_owned(),
                                        cluster_name: "my_cluster".to_owned(),
                                        disk: false,
                                        replica_id: Some(v39::StringWrapper {
                                            inner: "u123".to_owned(),
                                        }),
                                        logical_size: "too small".to_owned(),
                                        replica_name: "my_replica".to_owned(),
                                    },
                                ),
                            ),
                            occurred_at: Some(v39::EpochMillis { millis: 1600000 }),
                        })),
                    },
                    (),
                )],
            )
            .await
            .expect("insert success");

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

        let roles = AUDIT_LOG_COLLECTION_V40
            .peek_one(&mut stash)
            .await
            .expect("read v40");
        insta::assert_debug_snapshot!(roles, @r###"
        {
            AuditLogKey {
                event: Some(
                    V1(
                        AuditLogEventV1 {
                            id: 1234,
                            event_type: Create,
                            object_type: ClusterReplica,
                            user: Some(
                                StringWrapper {
                                    inner: "name",
                                },
                            ),
                            occurred_at: Some(
                                EpochMillis {
                                    millis: 1600000,
                                },
                            ),
                            details: Some(
                                CreateClusterReplicaV1(
                                    CreateClusterReplicaV1 {
                                        cluster_id: "u23",
                                        cluster_name: "my_cluster",
                                        replica_id: Some(
                                            StringWrapper {
                                                inner: "u123",
                                            },
                                        ),
                                        replica_name: "my_replica",
                                        logical_size: "too small",
                                        disk: false,
                                        billed_as: None,
                                        internal: false,
                                    },
                                ),
                            ),
                        },
                    ),
                ),
            }: (),
        }
        "###);
    }
}
