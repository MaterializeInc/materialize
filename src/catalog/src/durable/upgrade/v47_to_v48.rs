// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::WireCompatible;
use mz_stash::wire_compatible;
use std::collections::BTreeMap;

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v47 as v47, objects_v48 as v48};

wire_compatible!(v47::DatabaseKey with v48::DatabaseKey);
wire_compatible!(v47::SchemaKey with v48::SchemaKey);
wire_compatible!(v47::RoleKey with v48::RoleKey);
wire_compatible!(v47::ItemKey with v48::ItemKey);
wire_compatible!(v47::RoleId with v48::RoleId);
wire_compatible!(v47::MzAclItem with v48::MzAclItem);
wire_compatible!(v47::DatabaseId with v48::DatabaseId);
wire_compatible!(v47::RoleAttributes with v48::RoleAttributes);
wire_compatible!(v47::RoleMembership with v48::RoleMembership);
wire_compatible!(v47::RoleVars with v48::RoleVars);
wire_compatible!(v47::SchemaId with v48::SchemaId);
wire_compatible!(v47::ClusterIntrospectionSourceIndexKey with v48::ClusterIntrospectionSourceIndexKey);
wire_compatible!(v47::CatalogItem with v48::CatalogItem);

const FIRST_USER_OID: u32 = 20_000;

// SCHEMAS

const MZ_CATALOG_OID: u32 = 16_656;
const PG_CATALOG_OID: u32 = 16_657;
const MZ_INTERNAL_OID: u32 = 16_658;
const INFORMATION_SCHEMA_OID: u32 = 16_659;
const MZ_UNSAFE_OID: u32 = 16_660;

const MZ_CATALOG_ID: u64 = 1;
const PG_CATALOG_ID: u64 = 2;
const MZ_INTERNAL_ID: u64 = 4;
const INFORMATION_SCHEMA_ID: u64 = 5;
const MZ_UNSAFE_ID: u64 = 6;

// ROLES

const MZ_SYSTEM_OID: u32 = 16_661;
const MZ_SUPPORT_OID: u32 = 16_662;
const MZ_MONITOR_OID: u32 = 16_663;
const MZ_MONITOR_REDACTED_OID: u32 = 16_664;
const PUBLIC_ROLE_OID: u32 = 16_944;

const MZ_SYSTEM_ID: u64 = 1;
const MZ_SUPPORT_ID: u64 = 2;
const MZ_MONITOR_ID: u64 = 3;
const MZ_MONITOR_REDACTED_ID: u64 = 4;

struct OidAllocator {
    cur_oid: u32,
}

impl OidAllocator {
    fn new() -> Self {
        Self {
            cur_oid: FIRST_USER_OID,
        }
    }

    fn allocate_oid(&mut self) -> u32 {
        let oid = self.cur_oid;
        self.cur_oid += 1;
        oid
    }

    fn into_cur_oid(self) -> u32 {
        self.cur_oid
    }
}

/// Persist OIDs in the catalog.
pub fn upgrade(
    snapshot: Vec<v47::StateUpdateKind>,
) -> Vec<MigrationAction<v47::StateUpdateKind, v48::StateUpdateKind>> {
    let mut oid_allocator = OidAllocator::new();
    let mut databases = Vec::new();
    let mut schemas = Vec::new();
    let mut roles = Vec::new();
    let mut introspection_source_indexes = Vec::new();
    let mut items = Vec::new();
    for update in snapshot {
        match update.kind.expect("missing field") {
            v47::state_update_kind::Kind::Database(database) => databases.push(database),
            v47::state_update_kind::Kind::Schema(schema) => schemas.push(schema),
            v47::state_update_kind::Kind::Role(role) => roles.push(role),
            v47::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                introspection_source_index,
            ) => introspection_source_indexes.push(introspection_source_index),
            v47::state_update_kind::Kind::Item(item) => items.push(item),
            _ => {}
        }
    }

    // Sorting everything by ID isn't strictly necessary, but it would be nice if the OIDs and the
    // IDs had the same order.
    databases.sort_by(|a, b| {
        let a_id = a
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        let b_id = b
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        a_id.cmp(b_id)
    });
    schemas.sort_by(|a, b| {
        let a_id = a
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        let b_id = b
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        a_id.cmp(b_id)
    });
    roles.sort_by(|a, b| {
        let a_id = a
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        let b_id = b
            .key
            .as_ref()
            .expect("missing key field")
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        a_id.cmp(b_id)
    });
    introspection_source_indexes.sort_by(|a, b| {
        let a_id = a.value.as_ref().expect("missing value field").index_id;
        let b_id = b.value.as_ref().expect("missing value field").index_id;
        a_id.cmp(&b_id)
    });
    items.sort_by(|a, b| {
        let a_id = a
            .key
            .as_ref()
            .expect("missing key field")
            .gid
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        let b_id = b
            .key
            .as_ref()
            .expect("missing key field")
            .gid
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field");
        a_id.cmp(b_id)
    });

    let mut migrations = Vec::new();

    for database in databases {
        let old_key = database.key.expect("missing key field");
        let old_value = database.value.expect("missing value field");
        let new_key = WireCompatible::convert(&old_key);
        let oid = oid_allocator.allocate_oid();
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
        let old = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Database(
                v47::state_update_kind::Database {
                    key: Some(old_key),
                    value: Some(old_value),
                },
            )),
        };
        let new = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Database(
                v48::state_update_kind::Database {
                    key: Some(new_key),
                    value: Some(new_value),
                },
            )),
        };
        let action = MigrationAction::Update(old, new);
        migrations.push(action);
    }

    let mut system_schemas: BTreeMap<u64, u32> = [
        (MZ_CATALOG_ID, MZ_CATALOG_OID),
        (PG_CATALOG_ID, PG_CATALOG_OID),
        (MZ_INTERNAL_ID, MZ_INTERNAL_OID),
        (INFORMATION_SCHEMA_ID, INFORMATION_SCHEMA_OID),
        (MZ_UNSAFE_ID, MZ_UNSAFE_OID),
    ]
    .into_iter()
    .collect();
    for schema in schemas {
        let old_key = schema.key.expect("missing key field");
        let old_value = schema.value.expect("missing value field");
        let new_key = WireCompatible::convert(&old_key);
        let oid = match old_key
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field")
        {
            v47::schema_id::Value::System(id) => {
                system_schemas.remove(id).expect("unexpected system schema")
            }
            v47::schema_id::Value::User(_) => oid_allocator.allocate_oid(),
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
        let old = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Schema(
                v47::state_update_kind::Schema {
                    key: Some(old_key),
                    value: Some(old_value),
                },
            )),
        };
        let new = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Schema(
                v48::state_update_kind::Schema {
                    key: Some(new_key),
                    value: Some(new_value),
                },
            )),
        };
        let action = MigrationAction::Update(old, new);
        migrations.push(action);
    }

    let mut system_roles: BTreeMap<u64, u32> = [
        (MZ_SYSTEM_ID, MZ_SYSTEM_OID),
        (MZ_SUPPORT_ID, MZ_SUPPORT_OID),
        (MZ_MONITOR_ID, MZ_MONITOR_OID),
        (MZ_MONITOR_REDACTED_ID, MZ_MONITOR_REDACTED_OID),
    ]
    .into_iter()
    .collect();
    for role in roles {
        let old_key = role.key.expect("missing key field");
        let old_value = role.value.expect("missing value field");
        let new_key = WireCompatible::convert(&old_key);
        let oid = match old_key
            .id
            .as_ref()
            .expect("missing id field")
            .value
            .as_ref()
            .expect("missing value field")
        {
            v47::role_id::Value::System(id) => {
                system_roles.remove(id).expect("unexpected system schema")
            }
            v47::role_id::Value::Public(_) => PUBLIC_ROLE_OID,
            v47::role_id::Value::User(_) => oid_allocator.allocate_oid(),
        };
        let new_value = v48::RoleValue {
            name: old_value.name.clone(),
            attributes: old_value.attributes.as_ref().map(WireCompatible::convert),
            membership: old_value.membership.as_ref().map(WireCompatible::convert),
            vars: old_value.vars.as_ref().map(WireCompatible::convert),
            oid,
        };
        let old = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Role(
                v47::state_update_kind::Role {
                    key: Some(old_key),
                    value: Some(old_value),
                },
            )),
        };
        let new = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Role(
                v48::state_update_kind::Role {
                    key: Some(new_key),
                    value: Some(new_value),
                },
            )),
        };
        let action = MigrationAction::Update(old, new);
        migrations.push(action);
    }

    for introspection_source_index in introspection_source_indexes {
        let old_key = introspection_source_index.key.expect("missing key field");
        let old_value = introspection_source_index
            .value
            .expect("missing value field");
        let new_key = WireCompatible::convert(&old_key);
        let oid = oid_allocator.allocate_oid();
        let new_value = v48::ClusterIntrospectionSourceIndexValue {
            index_id: old_value.index_id,
            oid,
        };
        let old = v47::StateUpdateKind {
            kind: Some(
                v47::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                    v47::state_update_kind::ClusterIntrospectionSourceIndex {
                        key: Some(old_key),
                        value: Some(old_value),
                    },
                ),
            ),
        };
        let new = v48::StateUpdateKind {
            kind: Some(
                v48::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                    v48::state_update_kind::ClusterIntrospectionSourceIndex {
                        key: Some(new_key),
                        value: Some(new_value),
                    },
                ),
            ),
        };
        let action = MigrationAction::Update(old, new);
        migrations.push(action);
    }

    for item in items {
        let old_key = item.key.expect("missing key field");
        let old_value = item.value.expect("missing value field");
        let new_key = WireCompatible::convert(&old_key);
        let oid = oid_allocator.allocate_oid();
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
        let old = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Item(
                v47::state_update_kind::Item {
                    key: Some(old_key),
                    value: Some(old_value),
                },
            )),
        };
        let new = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::Item(
                v48::state_update_kind::Item {
                    key: Some(new_key),
                    value: Some(new_value),
                },
            )),
        };
        let action = MigrationAction::Update(old, new);
        migrations.push(action);
    }

    let id_alloc_key = v48::IdAllocKey {
        name: "oid".to_string(),
    };
    let id_alloc_value = v48::IdAllocValue {
        next_id: oid_allocator.into_cur_oid().into(),
    };
    let id_alloc = v48::StateUpdateKind {
        kind: Some(v48::state_update_kind::Kind::IdAlloc(
            v48::state_update_kind::IdAlloc {
                key: Some(id_alloc_key),
                value: Some(id_alloc_value),
            },
        )),
    };
    migrations.push(MigrationAction::Insert(id_alloc));

    migrations
}

#[cfg(test)]
mod tests {

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        let v47_user_owner = v47::RoleId {
            value: Some(v47::role_id::Value::User(1)),
        };
        let v47_system_owner = v47::RoleId {
            value: Some(v47::role_id::Value::System(1)),
        };
        let v47_db = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Database(
                v47::state_update_kind::Database {
                    key: Some(v47::DatabaseKey {
                        id: Some(v47::DatabaseId {
                            value: Some(v47::database_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v47::DatabaseValue {
                        name: "db".to_string(),
                        owner_id: Some(v47_user_owner.clone()),
                        privileges: Vec::new(),
                    }),
                },
            )),
        };
        let v47_user_schema = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Schema(
                v47::state_update_kind::Schema {
                    key: Some(v47::SchemaKey {
                        id: Some(v47::SchemaId {
                            value: Some(v47::schema_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v47::SchemaValue {
                        database_id: Some(v47::DatabaseId {
                            value: Some(v47::database_id::Value::User(1)),
                        }),
                        name: "sc".to_string(),
                        owner_id: Some(v47_user_owner.clone()),
                        privileges: Vec::new(),
                    }),
                },
            )),
        };
        let v47_system_schema = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Schema(
                v47::state_update_kind::Schema {
                    key: Some(v47::SchemaKey {
                        id: Some(v47::SchemaId {
                            value: Some(v47::schema_id::Value::System(4)),
                        }),
                    }),
                    value: Some(v47::SchemaValue {
                        database_id: None,
                        name: "mz_internal".to_string(),
                        owner_id: Some(v47_system_owner.clone()),
                        privileges: Vec::new(),
                    }),
                },
            )),
        };
        let v47_user_role = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Role(
                v47::state_update_kind::Role {
                    key: Some(v47::RoleKey {
                        id: Some(v47::RoleId {
                            value: Some(v47::role_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v47::RoleValue {
                        name: "joe".to_string(),
                        attributes: Some(v47::RoleAttributes { inherit: true }),
                        membership: Some(v47::RoleMembership { map: Vec::new() }),
                        vars: Some(v47::RoleVars {
                            entries: Vec::new(),
                        }),
                    }),
                },
            )),
        };
        let v47_system_role = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Role(
                v47::state_update_kind::Role {
                    key: Some(v47::RoleKey {
                        id: Some(v47::RoleId {
                            value: Some(v47::role_id::Value::System(2)),
                        }),
                    }),
                    value: Some(v47::RoleValue {
                        name: "mz_support".to_string(),
                        attributes: Some(v47::RoleAttributes { inherit: true }),
                        membership: Some(v47::RoleMembership { map: Vec::new() }),
                        vars: Some(v47::RoleVars {
                            entries: Vec::new(),
                        }),
                    }),
                },
            )),
        };
        let v47_public_role = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Role(
                v47::state_update_kind::Role {
                    key: Some(v47::RoleKey {
                        id: Some(v47::RoleId {
                            value: Some(v47::role_id::Value::Public(v47::Empty {})),
                        }),
                    }),
                    value: Some(v47::RoleValue {
                        name: "public".to_string(),
                        attributes: Some(v47::RoleAttributes { inherit: true }),
                        membership: Some(v47::RoleMembership { map: Vec::new() }),
                        vars: Some(v47::RoleVars {
                            entries: Vec::new(),
                        }),
                    }),
                },
            )),
        };
        let v47_introspection_source_index = v47::StateUpdateKind {
            kind: Some(
                v47::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                    v47::state_update_kind::ClusterIntrospectionSourceIndex {
                        key: Some(v47::ClusterIntrospectionSourceIndexKey {
                            cluster_id: Some(v47::ClusterId {
                                value: Some(v47::cluster_id::Value::User(1)),
                            }),
                            name: "isi".to_string(),
                        }),
                        value: Some(v47::ClusterIntrospectionSourceIndexValue { index_id: 42 }),
                    },
                ),
            ),
        };

        let v47_user_item = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Item(
                v47::state_update_kind::Item {
                    key: Some(v47::ItemKey {
                        gid: Some(v47::GlobalId {
                            value: Some(v47::global_id::Value::User(1)),
                        }),
                    }),
                    value: Some(v47::ItemValue {
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
                    }),
                },
            )),
        };

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
                        oid: 20_000,
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
                        oid: 20_001,
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
                        oid: 16_658,
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
                        oid: 20_002,
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
                        oid: 16_662,
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
                        oid: 16_944,
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
                            oid: 20_003,
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
                        oid: 20_004,
                    }),
                },
            )),
        };
        let v48_id_alloc = v48::StateUpdateKind {
            kind: Some(v48::state_update_kind::Kind::IdAlloc(
                v48::state_update_kind::IdAlloc {
                    key: Some(v48::IdAllocKey {
                        name: "oid".to_string(),
                    }),
                    value: Some(v48::IdAllocValue { next_id: 20_005 }),
                },
            )),
        };

        let mut updates: BTreeMap<_, _> = [
            (v47_db.clone(), v48_db),
            (v47_user_schema.clone(), v48_user_schema),
            (v47_system_schema.clone(), v48_system_schema),
            (v47_user_role.clone(), v48_user_role),
            (v47_system_role.clone(), v48_system_role),
            (v47_public_role.clone(), v48_public_role),
            (
                v47_introspection_source_index.clone(),
                v48_introspection_source_index,
            ),
            (v47_user_item.clone(), v48_user_item),
        ]
        .into_iter()
        .collect();

        let actions = upgrade(vec![
            v47_db.clone(),
            v47_user_schema.clone(),
            v47_system_schema.clone(),
            v47_user_role.clone(),
            v47_system_role.clone(),
            v47_public_role.clone(),
            v47_introspection_source_index.clone(),
            v47_user_item.clone(),
        ]);

        for action in actions {
            match action {
                MigrationAction::Insert(v48) => assert_eq!(v48, v48_id_alloc),
                MigrationAction::Update(v47, v48) => {
                    let expected_v48 = updates.remove(&v47).expect("unexpected update: {v47:?}");
                    assert_eq!(v48, expected_v48);
                }
                MigrationAction::Delete(v47) => panic!("unexpected delete: {v47:?}"),
            }
        }
    }
}
