// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_stash::upgrade::{MigrationAction, WireCompatible};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;
use std::collections::BTreeMap;

use crate::durable::upgrade::{objects_v47 as v47, objects_v48 as v48};

const DATABASES_COLLECTION: TypedCollection<v47::DatabaseKey, v47::DatabaseValue> =
    TypedCollection::new("database");
const SCHEMAS_COLLECTION: TypedCollection<v47::SchemaKey, v47::SchemaValue> =
    TypedCollection::new("schema");
const ROLES_COLLECTION: TypedCollection<v47::RoleKey, v47::RoleValue> =
    TypedCollection::new("role");
pub const CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION: TypedCollection<
    v47::ClusterIntrospectionSourceIndexKey,
    v47::ClusterIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index");
const ITEM_COLLECTION: TypedCollection<v47::ItemKey, v47::ItemValue> = TypedCollection::new("item");
const ID_ALLOCATOR_COLLECTION: TypedCollection<v47::IdAllocKey, v47::IdAllocValue> =
    TypedCollection::new("id_alloc");

/// Persist OIDs in the catalog.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    let mut cur_user_oid: u32 = 20_000;
    // Sorting everything by ID isn't strictly necessary, but it would be nice if the OIDs and the
    // IDs had the same order.
    let mut user_database_oids: BTreeMap<u64, u32> = tx
        .peek_one(
            tx.collection::<v47::DatabaseKey, v47::DatabaseValue>(DATABASES_COLLECTION.name())
                .await?,
        )
        .await?
        .into_keys()
        .map(|key| {
            key.id
                .expect("missing id field")
                .value
                .expect("missing value field")
        })
        .filter_map(|id| match id {
            v47::database_id::Value::System(_) => panic!("system databases don't exit"),
            v47::database_id::Value::User(id) => Some(id),
        })
        .sorted()
        .map(|id| {
            let oid = cur_user_oid;
            cur_user_oid += 1;
            (id, oid)
        })
        .collect();
    DATABASES_COLLECTION
        .migrate_to::<v48::DatabaseKey, v48::DatabaseValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_value)| {
                    let new_key = WireCompatible::convert(old_key);
                    let oid = match old_key
                        .id
                        .as_ref()
                        .expect("missing id field")
                        .value
                        .as_ref()
                        .expect("missing value field")
                    {
                        v47::database_id::Value::System(_) => panic!("system databases don't exit"),
                        v47::database_id::Value::User(id) => {
                            user_database_oids.remove(id).expect("missing id")
                        }
                    };
                    let new_value = v48::DatabaseValue {
                        name: old_value.name.clone(),
                        owner_id: old_value.owner_id.as_ref().map(WireCompatible::convert),
                        privileges: old_value
                            .privileges
                            .iter()
                            .map(WireCompatible::convert)
                            .collect(),
                        oid,
                    };
                    MigrationAction::Update(old_key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;

    let mut user_schema_oids: BTreeMap<u64, u32> = tx
        .peek_one(
            tx.collection::<v47::SchemaKey, v47::SchemaValue>(SCHEMAS_COLLECTION.name())
                .await?,
        )
        .await?
        .into_keys()
        .map(|key| {
            key.id
                .expect("missing id field")
                .value
                .expect("missing value field")
        })
        .filter_map(|id| match id {
            v47::schema_id::Value::System(_) => None,
            v47::schema_id::Value::User(id) => Some(id),
        })
        .sorted()
        .map(|id| {
            let oid = cur_user_oid;
            cur_user_oid += 1;
            (id, oid)
        })
        .collect();
    let mut system_schema_oids: BTreeMap<u64, u32> = [
        // mz_catalog
        (1, 16_656),
        // pg_catalog
        (2, 16_657),
        // mz_internal
        (4, 16_658),
        // information_schema
        (5, 16_659),
        // mz_unsafe
        (6, 16_660),
    ]
    .into_iter()
    .collect();
    SCHEMAS_COLLECTION
        .migrate_to::<v48::SchemaKey, v48::SchemaValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_value)| {
                    let new_key = WireCompatible::convert(old_key);
                    let oid = match old_key
                        .id
                        .as_ref()
                        .expect("missing id field")
                        .value
                        .as_ref()
                        .expect("missing value field")
                    {
                        v47::schema_id::Value::System(id) => {
                            system_schema_oids.remove(id).expect("missing id")
                        }
                        v47::schema_id::Value::User(id) => {
                            user_schema_oids.remove(id).expect("missing id")
                        }
                    };
                    let new_value = v48::SchemaValue {
                        database_id: old_value.database_id.as_ref().map(WireCompatible::convert),
                        name: old_value.name.clone(),
                        owner_id: old_value.owner_id.as_ref().map(WireCompatible::convert),
                        privileges: old_value
                            .privileges
                            .iter()
                            .map(WireCompatible::convert)
                            .collect(),
                        oid,
                    };
                    MigrationAction::Update(old_key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;

    let mut user_role_oids: BTreeMap<u64, u32> = tx
        .peek_one(
            tx.collection::<v47::RoleKey, v47::RoleValue>(ROLES_COLLECTION.name())
                .await?,
        )
        .await?
        .into_keys()
        .map(|key| {
            key.id
                .expect("missing id field")
                .value
                .expect("missing value field")
        })
        .filter_map(|id| match id {
            v47::role_id::Value::System(_) | v47::role_id::Value::Public(_) => None,
            v47::role_id::Value::User(id) => Some(id),
        })
        .sorted()
        .map(|id| {
            let oid = cur_user_oid;
            cur_user_oid += 1;
            (id, oid)
        })
        .collect();
    let mut system_role_oids: BTreeMap<u64, u32> = [
        // mz_system
        (1, 16_661),
        // mz_support
        (2, 16_662),
        // mz_monitor
        (3, 16_663),
        // mz_monitor_redacted
        (4, 16_664),
    ]
    .into_iter()
    .collect();
    let public_role_oid: u32 = 16_944;
    ROLES_COLLECTION
        .migrate_to::<v48::RoleKey, v48::RoleValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_value)| {
                    let new_key = WireCompatible::convert(old_key);
                    let oid = match old_key
                        .id
                        .as_ref()
                        .expect("missing id field")
                        .value
                        .as_ref()
                        .expect("missing value field")
                    {
                        v47::role_id::Value::System(id) => {
                            system_role_oids.remove(id).expect("missing id")
                        }
                        v47::role_id::Value::Public(_) => public_role_oid,
                        v47::role_id::Value::User(id) => {
                            user_role_oids.remove(id).expect("missing id")
                        }
                    };
                    let new_value = v48::RoleValue {
                        name: old_value.name.clone(),
                        attributes: old_value.attributes.as_ref().map(WireCompatible::convert),
                        membership: old_value.membership.as_ref().map(WireCompatible::convert),
                        vars: old_value.vars.as_ref().map(WireCompatible::convert),
                        oid,
                    };
                    MigrationAction::Update(old_key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;

    let mut introspection_source_index_oids: BTreeMap<u64, u32> = tx
        .peek_one(
            tx.collection::<v47::ClusterIntrospectionSourceIndexKey, v47::ClusterIntrospectionSourceIndexValue>(
                CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name(),
            )
            .await?,
        )
        .await?
        .into_values()
        .map(|value| value.index_id)
        .sorted()
        .map(|id| {
            let oid = cur_user_oid;
            cur_user_oid += 1;
            (id, oid)
        })
        .collect();
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
        .migrate_to::<v48::ClusterIntrospectionSourceIndexKey, v48::ClusterIntrospectionSourceIndexValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_value)| {
                    let new_key = WireCompatible::convert(old_key);
                    let oid = introspection_source_index_oids.remove(&old_value.index_id).expect("missing id");
                    let new_value = v48::ClusterIntrospectionSourceIndexValue {
                        index_id: old_value.index_id,
                        oid,
                    };
                    MigrationAction::Update(old_key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;

    let mut user_item_oids: BTreeMap<u64, u32> = tx
        .peek_one(
            tx.collection::<v47::ItemKey, v47::ItemValue>(ITEM_COLLECTION.name())
                .await?,
        )
        .await?
        .into_keys()
        .map(|key| {
            key.gid
                .expect("missing gid field")
                .value
                .expect("missing value field")
        })
        .filter_map(|id| match id {
            v47::global_id::Value::System(_)
            | v47::global_id::Value::Transient(_)
            | v47::global_id::Value::Explain(_) => {
                panic!("variant doesn't exist in items collection")
            }
            v47::global_id::Value::User(id) => Some(id),
        })
        .sorted()
        .map(|id| {
            let oid = cur_user_oid;
            cur_user_oid += 1;
            (id, oid)
        })
        .collect();
    ITEM_COLLECTION
        .migrate_to::<v48::ItemKey, v48::ItemValue>(tx, |entries| {
            entries
                .iter()
                .map(|(old_key, old_value)| {
                    let new_key = WireCompatible::convert(old_key);
                    let oid = match old_key
                        .gid
                        .as_ref()
                        .expect("missing gid field")
                        .value
                        .as_ref()
                        .expect("missing value field")
                    {
                        v47::global_id::Value::System(_)
                        | v47::global_id::Value::Transient(_)
                        | v47::global_id::Value::Explain(_) => {
                            panic!("variant doesn't exist in items collection")
                        }
                        v47::global_id::Value::User(id) => {
                            user_item_oids.remove(id).expect("missing id")
                        }
                    };
                    let new_value = v48::ItemValue {
                        schema_id: old_value.schema_id.as_ref().map(WireCompatible::convert),
                        name: old_value.name.clone(),
                        definition: old_value.definition.as_ref().map(WireCompatible::convert),
                        owner_id: old_value.owner_id.as_ref().map(WireCompatible::convert),
                        privileges: old_value
                            .privileges
                            .iter()
                            .map(WireCompatible::convert)
                            .collect(),
                        oid,
                    };
                    MigrationAction::Update(old_key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;

    ID_ALLOCATOR_COLLECTION
        .migrate_to::<v48::IdAllocKey, v48::IdAllocValue>(tx, |_entries| {
            let id_alloc_key = v48::IdAllocKey {
                name: "oid".to_string(),
            };
            let id_alloc_value = v48::IdAllocValue {
                next_id: cur_user_oid.into(),
            };
            vec![MigrationAction::Insert(id_alloc_key, id_alloc_value)]
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_stash::Stash;
    use std::collections::BTreeSet;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        const DATABASES_COLLECTION_V47: TypedCollection<v47::DatabaseKey, v47::DatabaseValue> =
            TypedCollection::new("database");
        const SCHEMAS_COLLECTION_V47: TypedCollection<v47::SchemaKey, v47::SchemaValue> =
            TypedCollection::new("schema");
        const ROLES_COLLECTION_V47: TypedCollection<v47::RoleKey, v47::RoleValue> =
            TypedCollection::new("role");
        pub const CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION_V47: TypedCollection<
            v47::ClusterIntrospectionSourceIndexKey,
            v47::ClusterIntrospectionSourceIndexValue,
        > = TypedCollection::new("compute_introspection_source_index");
        const ITEM_COLLECTION_V47: TypedCollection<v47::ItemKey, v47::ItemValue> =
            TypedCollection::new("item");

        const DATABASES_COLLECTION_V48: TypedCollection<v48::DatabaseKey, v48::DatabaseValue> =
            TypedCollection::new("database");
        const SCHEMAS_COLLECTION_V48: TypedCollection<v48::SchemaKey, v48::SchemaValue> =
            TypedCollection::new("schema");
        const ROLES_COLLECTION_V48: TypedCollection<v48::RoleKey, v48::RoleValue> =
            TypedCollection::new("role");
        pub const CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION_V48: TypedCollection<
            v48::ClusterIntrospectionSourceIndexKey,
            v48::ClusterIntrospectionSourceIndexValue,
        > = TypedCollection::new("compute_introspection_source_index");
        const ITEM_COLLECTION_V48: TypedCollection<v48::ItemKey, v48::ItemValue> =
            TypedCollection::new("item");
        const ID_ALLOCATOR_COLLECTION_V48: TypedCollection<v48::IdAllocKey, v48::IdAllocValue> =
            TypedCollection::new("id_alloc");

        let v47_user_owner = v47::RoleId {
            value: Some(v47::role_id::Value::User(1)),
        };
        let v47_system_owner = v47::RoleId {
            value: Some(v47::role_id::Value::System(1)),
        };
        let v47_db_key = v47::DatabaseKey {
            id: Some(v47::DatabaseId {
                value: Some(v47::database_id::Value::User(1)),
            }),
        };
        let v47_db_value = v47::DatabaseValue {
            name: "db".to_string(),
            owner_id: Some(v47_user_owner.clone()),
            privileges: Vec::new(),
        };
        let v47_user_schema_key = v47::SchemaKey {
            id: Some(v47::SchemaId {
                value: Some(v47::schema_id::Value::User(1)),
            }),
        };
        let v47_user_schema_value = v47::SchemaValue {
            database_id: Some(v47::DatabaseId {
                value: Some(v47::database_id::Value::User(1)),
            }),
            name: "sc".to_string(),
            owner_id: Some(v47_user_owner.clone()),
            privileges: Vec::new(),
        };
        let v47_system_schema_key = v47::SchemaKey {
            id: Some(v47::SchemaId {
                value: Some(v47::schema_id::Value::System(4)),
            }),
        };
        let v47_system_schema_value = v47::SchemaValue {
            database_id: None,
            name: "mz_internal".to_string(),
            owner_id: Some(v47_system_owner.clone()),
            privileges: Vec::new(),
        };
        let v47_user_role_key = v47::RoleKey {
            id: Some(v47::RoleId {
                value: Some(v47::role_id::Value::User(1)),
            }),
        };
        let v47_user_role_value = v47::RoleValue {
            name: "joe".to_string(),
            attributes: Some(v47::RoleAttributes { inherit: true }),
            membership: Some(v47::RoleMembership { map: Vec::new() }),
            vars: Some(v47::RoleVars {
                entries: Vec::new(),
            }),
        };
        let v47_system_role_key = v47::RoleKey {
            id: Some(v47::RoleId {
                value: Some(v47::role_id::Value::System(2)),
            }),
        };
        let v47_system_role_value = v47::RoleValue {
            name: "mz_support".to_string(),
            attributes: Some(v47::RoleAttributes { inherit: true }),
            membership: Some(v47::RoleMembership { map: Vec::new() }),
            vars: Some(v47::RoleVars {
                entries: Vec::new(),
            }),
        };
        let v47_public_role_key = v47::RoleKey {
            id: Some(v47::RoleId {
                value: Some(v47::role_id::Value::Public(v47::Empty {})),
            }),
        };
        let v47_public_role_value = v47::RoleValue {
            name: "public".to_string(),
            attributes: Some(v47::RoleAttributes { inherit: true }),
            membership: Some(v47::RoleMembership { map: Vec::new() }),
            vars: Some(v47::RoleVars {
                entries: Vec::new(),
            }),
        };
        let v47_introspection_source_index_key = v47::ClusterIntrospectionSourceIndexKey {
            cluster_id: Some(v47::ClusterId {
                value: Some(v47::cluster_id::Value::User(1)),
            }),
            name: "isi".to_string(),
        };
        let v47_introspection_source_index_value =
            v47::ClusterIntrospectionSourceIndexValue { index_id: 42 };
        let v47_user_item_key = v47::ItemKey {
            gid: Some(v47::GlobalId {
                value: Some(v47::global_id::Value::User(1)),
            }),
        };
        let v47_user_item_value = v47::ItemValue {
            schema_id: Some(v47::SchemaId {
                value: Some(v47::schema_id::Value::System(4)),
            }),
            name: "t".to_string(),
            definition: Some(v47::CatalogItem {
                value: Some(v47::catalog_item::Value::V1(v47::catalog_item::V1 {
                    create_sql: "CREATE TABLE t (a INT)".to_string(),
                })),
            }),
            owner_id: Some(v47_user_owner.clone()),
            privileges: Vec::new(),
        };

        let v48_user_owner = v48::RoleId {
            value: Some(v48::role_id::Value::User(1)),
        };
        let v48_system_owner = v48::RoleId {
            value: Some(v48::role_id::Value::System(1)),
        };
        let v48_db_key = v48::DatabaseKey {
            id: Some(v48::DatabaseId {
                value: Some(v48::database_id::Value::User(1)),
            }),
        };
        let v48_db_value = v48::DatabaseValue {
            name: "db".to_string(),
            owner_id: Some(v48_user_owner.clone()),
            privileges: Vec::new(),
            oid: 20_000,
        };
        let v48_user_schema_key = v48::SchemaKey {
            id: Some(v48::SchemaId {
                value: Some(v48::schema_id::Value::User(1)),
            }),
        };
        let v48_user_schema_value = v48::SchemaValue {
            database_id: Some(v48::DatabaseId {
                value: Some(v48::database_id::Value::User(1)),
            }),
            name: "sc".to_string(),
            owner_id: Some(v48_user_owner.clone()),
            privileges: Vec::new(),
            oid: 20_001,
        };
        let v48_system_schema_key = v48::SchemaKey {
            id: Some(v48::SchemaId {
                value: Some(v48::schema_id::Value::System(4)),
            }),
        };
        let v48_system_schema_value = v48::SchemaValue {
            database_id: None,
            name: "mz_internal".to_string(),
            owner_id: Some(v48_system_owner.clone()),
            privileges: Vec::new(),
            oid: 16_658,
        };
        let v48_user_role_key = v48::RoleKey {
            id: Some(v48::RoleId {
                value: Some(v48::role_id::Value::User(1)),
            }),
        };
        let v48_user_role_value = v48::RoleValue {
            name: "joe".to_string(),
            attributes: Some(v48::RoleAttributes { inherit: true }),
            membership: Some(v48::RoleMembership { map: Vec::new() }),
            vars: Some(v48::RoleVars {
                entries: Vec::new(),
            }),
            oid: 20_002,
        };
        let v48_system_role_key = v48::RoleKey {
            id: Some(v48::RoleId {
                value: Some(v48::role_id::Value::System(2)),
            }),
        };
        let v48_system_role_value = v48::RoleValue {
            name: "mz_support".to_string(),
            attributes: Some(v48::RoleAttributes { inherit: true }),
            membership: Some(v48::RoleMembership { map: Vec::new() }),
            vars: Some(v48::RoleVars {
                entries: Vec::new(),
            }),
            oid: 16_662,
        };
        let v48_public_role_key = v48::RoleKey {
            id: Some(v48::RoleId {
                value: Some(v48::role_id::Value::Public(v48::Empty {})),
            }),
        };
        let v48_public_role_value = v48::RoleValue {
            name: "public".to_string(),
            attributes: Some(v48::RoleAttributes { inherit: true }),
            membership: Some(v48::RoleMembership { map: Vec::new() }),
            vars: Some(v48::RoleVars {
                entries: Vec::new(),
            }),
            oid: 16_944,
        };
        let v48_introspection_source_index_key = v48::ClusterIntrospectionSourceIndexKey {
            cluster_id: Some(v48::ClusterId {
                value: Some(v48::cluster_id::Value::User(1)),
            }),
            name: "isi".to_string(),
        };
        let v48_introspection_source_index_value = v48::ClusterIntrospectionSourceIndexValue {
            index_id: 42,
            oid: 20_003,
        };
        let v48_user_item_key = v48::ItemKey {
            gid: Some(v48::GlobalId {
                value: Some(v48::global_id::Value::User(1)),
            }),
        };
        let v48_user_item_value = v48::ItemValue {
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
            oid: 20_004,
        };
        let v48_id_alloc_key = v48::IdAllocKey {
            name: "oid".to_string(),
        };
        let v48_id_alloc_value = v48::IdAllocValue { next_id: 20_005 };

        Stash::with_debug_stash(|mut stash: Stash| async move {
            DATABASES_COLLECTION_V47
                .insert_without_overwrite(&mut stash, [(v47_db_key, v47_db_value)])
                .await
                .expect("insert success");
            SCHEMAS_COLLECTION_V47
                .insert_without_overwrite(
                    &mut stash,
                    [
                        (v47_user_schema_key, v47_user_schema_value),
                        (v47_system_schema_key, v47_system_schema_value),
                    ],
                )
                .await
                .expect("insert success");
            ROLES_COLLECTION_V47
                .insert_without_overwrite(
                    &mut stash,
                    [
                        (v47_user_role_key, v47_user_role_value),
                        (v47_system_role_key, v47_system_role_value),
                        (v47_public_role_key, v47_public_role_value),
                    ],
                )
                .await
                .expect("insert success");
            ITEM_COLLECTION_V47
                .insert_without_overwrite(&mut stash, [(v47_user_item_key, v47_user_item_value)])
                .await
                .expect("insert success");
            CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION_V47
                .insert_without_overwrite(
                    &mut stash,
                    [(
                        v47_introspection_source_index_key,
                        v47_introspection_source_index_value,
                    )],
                )
                .await
                .expect("insert success");

            // Run the migration.
            stash
                .with_transaction(|tx| {
                    Box::pin(async move {
                        upgrade(&tx).await?;
                        Ok(())
                    })
                })
                .await
                .expect("transaction failed");

            let databases_v48: BTreeSet<_> = DATABASES_COLLECTION_V48
                .peek_one(&mut stash)
                .await
                .expect("read v48")
                .into_iter()
                .collect();
            assert_eq!(
                databases_v48,
                [(v48_db_key, v48_db_value)].into_iter().collect()
            );

            let schemas_v48: BTreeSet<_> = SCHEMAS_COLLECTION_V48
                .peek_one(&mut stash)
                .await
                .expect("read v48")
                .into_iter()
                .collect();
            assert_eq!(
                schemas_v48,
                [
                    (v48_user_schema_key, v48_user_schema_value),
                    (v48_system_schema_key, v48_system_schema_value),
                ]
                .into_iter()
                .collect()
            );

            let roles_v48: BTreeSet<_> = ROLES_COLLECTION_V48
                .peek_one(&mut stash)
                .await
                .expect("read v48")
                .into_iter()
                .collect();
            assert_eq!(
                roles_v48,
                [
                    (v48_user_role_key, v48_user_role_value),
                    (v48_system_role_key, v48_system_role_value),
                    (v48_public_role_key, v48_public_role_value)
                ]
                .into_iter()
                .collect()
            );

            let introspection_source_indexes_v48: BTreeSet<_> =
                CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION_V48
                    .peek_one(&mut stash)
                    .await
                    .expect("read v48")
                    .into_iter()
                    .collect();
            assert_eq!(
                introspection_source_indexes_v48,
                [(
                    v48_introspection_source_index_key,
                    v48_introspection_source_index_value
                )]
                .into_iter()
                .collect()
            );

            let items_v48: BTreeSet<_> = ITEM_COLLECTION_V48
                .peek_one(&mut stash)
                .await
                .expect("read v48")
                .into_iter()
                .collect();
            assert_eq!(
                items_v48,
                [(v48_user_item_key, v48_user_item_value)]
                    .into_iter()
                    .collect()
            );

            let id_allocs_v48: BTreeSet<_> = ID_ALLOCATOR_COLLECTION_V48
                .peek_one(&mut stash)
                .await
                .expect("read v48")
                .into_iter()
                .collect();
            assert_eq!(
                id_allocs_v48,
                [(v48_id_alloc_key, v48_id_alloc_value)]
                    .into_iter()
                    .collect()
            );
        })
        .await
        .expect("stash failed");
    }
}
