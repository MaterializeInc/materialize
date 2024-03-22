// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v48 as v48, objects_v49 as v49};

/// Remove timestamps.
pub fn upgrade(
    snapshot: Vec<v48::StateUpdateKind>,
) -> Vec<MigrationAction<v48::StateUpdateKind, v49::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter(|update| {
            matches!(
                update.kind.as_ref().expect("missing field"),
                v48::state_update_kind::Kind::Timestamp(_)
            )
        })
        .map(MigrationAction::Delete)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        // Most of the below objects are copied and pasted from the v47_to_v48 migration to seed
        // the catalog with random objects.
        let v48_user_owner = v48::RoleId {
            value: Some(v48::role_id::Value::User(1)),
        };
        let v48_system_owner = v48::RoleId {
            value: Some(v48::role_id::Value::System(1)),
        };
        let v48_db = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Database(
                v48::state_update_kind::Database {
                    key: Some(v48::DatabaseKey {
                        id: Some(v48::DatabaseId {
                            value: Some(v48::database_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v48::DatabaseValue {
                        name: "db".to_string(),
                        owner_id: Some(v48_user_owner.clone()),
                        privileges: Vec::new(),
                        oid: 1,
                    }),
                },
            )),
        };
        let v48_user_schema = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Schema(
                v48::state_update_kind::Schema {
                    key: Some(v48::SchemaKey {
                        id: Some(v48::SchemaId {
                            value: Some(v48::schema_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v48::SchemaValue {
                        database_id: Some(v48::DatabaseId {
                            value: Some(v48::database_id::Value::User(1)),
                        }),
                        name: "sc".to_string(),
                        owner_id: Some(v48_user_owner.clone()),
                        privileges: Vec::new(),
                        oid: 2,
                    }),
                },
            )),
        };
        let v48_system_schema = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Schema(
                v48::state_update_kind::Schema {
                    key: Some(v48::SchemaKey {
                        id: Some(v48::SchemaId {
                            value: Some(v48::schema_id::Value::System(4)),
                        }),
                    }),
                    value: Some(v48::SchemaValue {
                        database_id: None,
                        name: "mz_internal".to_string(),
                        owner_id: Some(v48_system_owner.clone()),
                        privileges: Vec::new(),
                        oid: 3,
                    }),
                },
            )),
        };
        let v48_user_role = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Role(
                v48::state_update_kind::Role {
                    key: Some(v48::RoleKey {
                        id: Some(v48::RoleId {
                            value: Some(v48::role_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v48::RoleValue {
                        name: "joe".to_string(),
                        attributes: Some(v48::RoleAttributes { inherit: true }),
                        membership: Some(v48::RoleMembership { map: Vec::new() }),
                        vars: Some(v48::RoleVars {
                            entries: Vec::new(),
                        }),
                        oid: 4,
                    }),
                },
            )),
        };
        let v48_system_role = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Role(
                v48::state_update_kind::Role {
                    key: Some(v48::RoleKey {
                        id: Some(v48::RoleId {
                            value: Some(v48::role_id::Value::System(2)),
                        }),
                    }),
                    value: Some(v48::RoleValue {
                        name: "mz_support".to_string(),
                        attributes: Some(v48::RoleAttributes { inherit: true }),
                        membership: Some(v48::RoleMembership { map: Vec::new() }),
                        vars: Some(v48::RoleVars {
                            entries: Vec::new(),
                        }),
                        oid: 5,
                    }),
                },
            )),
        };
        let v48_public_role = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Role(
                v48::state_update_kind::Role {
                    key: Some(v48::RoleKey {
                        id: Some(v48::RoleId {
                            value: Some(v48::role_id::Value::Public(v48::Empty {})),
                        }),
                    }),
                    value: Some(v48::RoleValue {
                        name: "public".to_string(),
                        attributes: Some(v48::RoleAttributes { inherit: true }),
                        membership: Some(v48::RoleMembership { map: Vec::new() }),
                        vars: Some(v48::RoleVars {
                            entries: Vec::new(),
                        }),
                        oid: 6,
                    }),
                },
            )),
        };
        let v48_introspection_source_index = v48::StateUpdateKind {
            kind: Some(
                v48::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                    v48::state_update_kind::ClusterIntrospectionSourceIndex {
                        key: Some(v48::ClusterIntrospectionSourceIndexKey {
                            cluster_id: Some(v48::ClusterId {
                                value: Some(v48::cluster_id::Value::User(1)),
                            }),
                            name: "isi".to_string(),
                        }),
                        value: Some(v48::ClusterIntrospectionSourceIndexValue {
                            index_id: 42,
                            oid: 7,
                        }),
                    },
                ),
            ),
        };

        let v48_user_item = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Item(
                v48::state_update_kind::Item {
                    key: Some(v48::ItemKey {
                        gid: Some(v48::GlobalId {
                            value: Some(v48::global_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v48::ItemValue {
                        schema_id: Some(v48::SchemaId {
                            value: Some(v48::schema_id::Value::System(4)),
                        }),
                        name: "t".to_string(),
                        definition: Some(v48::CatalogItem {
                            value: Some(v48::catalog_item::Value::V1(v48::catalog_item::V1 {
                                create_sql: "CREATE TABLE t (a INT)".to_string(),
                            })),
                        }),
                        owner_id: Some(v48_user_owner.clone()),
                        privileges: Vec::new(),
                        oid: 8,
                    }),
                },
            )),
        };

        let v48_timestamp_a = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Timestamp(
                v48::state_update_kind::Timestamp {
                    key: Some(v48::TimestampKey {
                        id: "timeline_a".to_string(),
                    }),
                    value: Some(v48::TimestampValue {
                        ts: Some(v48::Timestamp { internal: 42 }),
                    }),
                },
            )),
        };

        let v48_timestamp_b = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Timestamp(
                v48::state_update_kind::Timestamp {
                    key: Some(v48::TimestampKey {
                        id: "timeline_b".to_string(),
                    }),
                    value: Some(v48::TimestampValue {
                        ts: Some(v48::Timestamp { internal: 666 }),
                    }),
                },
            )),
        };

        let actions = upgrade(vec![
            v48_db,
            v48_user_schema,
            v48_system_schema,
            v48_user_role,
            v48_system_role,
            v48_public_role,
            v48_introspection_source_index,
            v48_user_item,
            v48_timestamp_a.clone(),
            v48_timestamp_b.clone(),
        ]);

        let mut expected_deletes: BTreeSet<_> = [v48_timestamp_a.clone(), v48_timestamp_b.clone()]
            .into_iter()
            .collect();

        for action in actions {
            match action {
                MigrationAction::Insert(v49) => panic!("unexpected insert: {v49:?}"),
                MigrationAction::Update(v48, v49) => {
                    panic!("unexpected update from {v48:?} to {v49:?}");
                }
                MigrationAction::Delete(v48) => {
                    assert!(expected_deletes.remove(&v48), "unexpected delete: {v48:?}");
                }
            }
        }

        assert_eq!(expected_deletes, BTreeSet::new(), "missing deletes");
    }
}
