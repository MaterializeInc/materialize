// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::wire_compatible;
use mz_proto::wire_compatible::WireCompatible;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v53 as v53, objects_v54 as v54};

wire_compatible!(v53::RoleAttributes with v54::RoleAttributes);
wire_compatible!(v53::RoleVars with v54::RoleVars);
wire_compatible!(v53::ItemKey with v54::ItemKey);
wire_compatible!(v53::SchemaId with v54::SchemaId);
wire_compatible!(v53::CatalogItem with v54::CatalogItem);
wire_compatible!(v53::AclMode with v54::AclMode);

const MZ_MONITOR_SYSTEM_ID: u64 = 3;
const MZ_MONITOR_REDACTED_SYSTEM_ID: u64 = 4;

const MZ_MONITOR_PREDEFINED_ID: u64 = 1;
const MZ_MONITOR_REDACTED_PREDEFINED_ID: u64 = 2;

/// Migrate predefined role from system to predefined.
///
/// `RoleId`s show up in a lot of places but we know that `mz_monitor` and `mz_monitor` redacted
/// can only show up in the following places:
///
///   - As a key in the Roles collections.
///   - In a `RoleMembership` in the Roles collection.
///   - As a grantee in an `MzAclItem` in the Items collection.
pub fn upgrade(
    snapshot: Vec<v53::StateUpdateKind>,
) -> Vec<MigrationAction<v53::StateUpdateKind, v54::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update.kind.expect("missing kind field") {
            v53::state_update_kind::Kind::Item(old_item) => {
                let item = old_item.clone();
                let key = item.key.as_ref().map(WireCompatible::convert);
                let value = item.value.map(|old_val| v54::ItemValue {
                    schema_id: old_val.schema_id.as_ref().map(WireCompatible::convert),
                    name: old_val.name,
                    definition: old_val.definition.as_ref().map(WireCompatible::convert),
                    owner_id: old_val.owner_id.map(migrate_role_id),
                    privileges: old_val
                        .privileges
                        .into_iter()
                        .map(migrate_mz_acl_item)
                        .collect(),
                    oid: old_val.oid,
                });
                Some((
                    v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::Item(old_item)),
                    },
                    v54::StateUpdateKind {
                        kind: Some(v54::state_update_kind::Kind::Item(
                            v54::state_update_kind::Item { key, value },
                        )),
                    },
                ))
            }
            v53::state_update_kind::Kind::Role(old_role) => {
                let role = old_role.clone();
                let key = role.key.map(migrate_role_key);
                let value = role.value.map(|old_val| v54::RoleValue {
                    name: old_val.name,
                    attributes: old_val.attributes.as_ref().map(WireCompatible::convert),
                    membership: old_val.membership.map(migrate_role_membership),
                    vars: old_val.vars.as_ref().map(WireCompatible::convert),
                    oid: old_val.oid,
                });
                Some((
                    v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::Role(old_role)),
                    },
                    v54::StateUpdateKind {
                        kind: Some(v54::state_update_kind::Kind::Role(
                            v54::state_update_kind::Role { key, value },
                        )),
                    },
                ))
            }
            v53::state_update_kind::Kind::AuditLog(_)
            | v53::state_update_kind::Kind::Cluster(_)
            | v53::state_update_kind::Kind::ClusterReplica(_)
            | v53::state_update_kind::Kind::Comment(_)
            | v53::state_update_kind::Kind::Config(_)
            | v53::state_update_kind::Kind::Database(_)
            | v53::state_update_kind::Kind::DefaultPrivileges(_)
            | v53::state_update_kind::Kind::Epoch(_)
            | v53::state_update_kind::Kind::IdAlloc(_)
            | v53::state_update_kind::Kind::ClusterIntrospectionSourceIndex(_)
            | v53::state_update_kind::Kind::Schema(_)
            | v53::state_update_kind::Kind::Setting(_)
            | v53::state_update_kind::Kind::StorageUsage(_)
            | v53::state_update_kind::Kind::ServerConfiguration(_)
            | v53::state_update_kind::Kind::GidMapping(_)
            | v53::state_update_kind::Kind::SystemPrivileges(_)
            | v53::state_update_kind::Kind::StorageCollectionMetadata(_)
            | v53::state_update_kind::Kind::UnfinalizedShard(_)
            | v53::state_update_kind::Kind::PersistTxnShard(_) => None,
        })
        .map(|(old_update, new_update)| MigrationAction::Update(old_update, new_update))
        .collect()
}

fn migrate_role_id(role_id: v53::RoleId) -> v54::RoleId {
    let value = match role_id.value.expect("missing value") {
        v53::role_id::Value::System(MZ_MONITOR_SYSTEM_ID) => {
            v54::role_id::Value::Predefined(MZ_MONITOR_PREDEFINED_ID)
        }
        v53::role_id::Value::System(MZ_MONITOR_REDACTED_SYSTEM_ID) => {
            v54::role_id::Value::Predefined(MZ_MONITOR_REDACTED_PREDEFINED_ID)
        }
        v53::role_id::Value::System(id) => v54::role_id::Value::System(id),
        v53::role_id::Value::User(id) => v54::role_id::Value::User(id),
        v53::role_id::Value::Public(_empty) => v54::role_id::Value::Public(v54::Empty {}),
    };
    v54::RoleId { value: Some(value) }
}

fn migrate_mz_acl_item(mz_acl_item: v53::MzAclItem) -> v54::MzAclItem {
    v54::MzAclItem {
        grantee: mz_acl_item.grantee.map(migrate_role_id),
        grantor: mz_acl_item.grantor.map(migrate_role_id),
        acl_mode: mz_acl_item.acl_mode.as_ref().map(WireCompatible::convert),
    }
}

fn migrate_role_key(role_key: v53::RoleKey) -> v54::RoleKey {
    v54::RoleKey {
        id: role_key.id.map(migrate_role_id),
    }
}

fn migrate_role_membership(role_membership: v53::RoleMembership) -> v54::RoleMembership {
    v54::RoleMembership {
        map: role_membership
            .map
            .into_iter()
            .map(migrate_role_membership_entry)
            .collect(),
    }
}

fn migrate_role_membership_entry(
    role_membership_entry: v53::role_membership::Entry,
) -> v54::role_membership::Entry {
    v54::role_membership::Entry {
        key: role_membership_entry.key.map(migrate_role_id),
        value: role_membership_entry.value.map(migrate_role_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        const MZ_MONITOR_NAME: &str = "mz_monitor";
        const MZ_MONITOR_REDACTED_NAME: &str = "mz_monitor_redacted";

        let v53_mz_monitor_role_id = v53::RoleId {
            value: Some(v53::role_id::Value::System(MZ_MONITOR_SYSTEM_ID)),
        };
        let v53_mz_monitor_redacted_role_id = v53::RoleId {
            value: Some(v53::role_id::Value::System(MZ_MONITOR_REDACTED_SYSTEM_ID)),
        };
        let v53_mz_monitor = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Role(empty_role_v53(
                v53_mz_monitor_role_id.clone(),
                MZ_MONITOR_NAME,
                1,
            ))),
        };
        let v53_mz_monitor_redacted = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Role(empty_role_v53(
                v53_mz_monitor_redacted_role_id.clone(),
                MZ_MONITOR_REDACTED_NAME,
                2,
            ))),
        };
        let v53_mz_monitor_member = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Role(
                role_with_membership_v53(v53_mz_monitor_role_id.clone(), MZ_MONITOR_NAME, 1),
            )),
        };
        let v53_mz_monitor_redacted_member = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Role(
                role_with_membership_v53(
                    v53_mz_monitor_redacted_role_id.clone(),
                    MZ_MONITOR_REDACTED_NAME,
                    2,
                ),
            )),
        };
        let v53_item_monitor = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Item(empty_view_v53(
                v53_mz_monitor_role_id,
            ))),
        };
        let v53_item_monitor_redacted = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Item(empty_view_v53(
                v53_mz_monitor_redacted_role_id,
            ))),
        };

        let v54_mz_monitor_role_id = v54::RoleId {
            value: Some(v54::role_id::Value::Predefined(MZ_MONITOR_PREDEFINED_ID)),
        };
        let v54_mz_monitor_redacted_role_id = v54::RoleId {
            value: Some(v54::role_id::Value::Predefined(
                MZ_MONITOR_REDACTED_PREDEFINED_ID,
            )),
        };
        let v54_mz_monitor = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Role(empty_role_v54(
                v54_mz_monitor_role_id.clone(),
                MZ_MONITOR_NAME,
                1,
            ))),
        };
        let v54_mz_monitor_redacted = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Role(empty_role_v54(
                v54_mz_monitor_redacted_role_id.clone(),
                MZ_MONITOR_REDACTED_NAME,
                2,
            ))),
        };
        let v54_mz_monitor_member = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Role(
                role_with_membership_v54(v54_mz_monitor_role_id.clone(), MZ_MONITOR_NAME, 1),
            )),
        };
        let v54_mz_monitor_redacted_member = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Role(
                role_with_membership_v54(
                    v54_mz_monitor_redacted_role_id.clone(),
                    MZ_MONITOR_REDACTED_NAME,
                    2,
                ),
            )),
        };
        let v54_item_monitor = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Item(empty_view_v54(
                v54_mz_monitor_role_id,
            ))),
        };
        let v54_item_monitor_redacted = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Item(empty_view_v54(
                v54_mz_monitor_redacted_role_id,
            ))),
        };

        let mut expected_updates: BTreeSet<_> = [
            (v53_mz_monitor.clone(), v54_mz_monitor),
            (v53_mz_monitor_redacted.clone(), v54_mz_monitor_redacted),
            (v53_mz_monitor_member.clone(), v54_mz_monitor_member),
            (
                v53_mz_monitor_redacted_member.clone(),
                v54_mz_monitor_redacted_member,
            ),
            (v53_item_monitor.clone(), v54_item_monitor),
            (v53_item_monitor_redacted.clone(), v54_item_monitor_redacted),
        ]
        .into_iter()
        .collect();

        let actions = upgrade(vec![
            v53_mz_monitor,
            v53_mz_monitor_redacted,
            v53_mz_monitor_member,
            v53_mz_monitor_redacted_member,
            v53_item_monitor,
            v53_item_monitor_redacted,
        ]);

        for action in actions {
            match action {
                MigrationAction::Insert(v54) => panic!("unexpected insert: {v54:?}"),
                MigrationAction::Update(v53, v54) => {
                    assert!(
                        expected_updates.remove(&(v53.clone(), v54.clone())),
                        "unexpected update from {v53:?} to {v54:?}"
                    )
                }
                MigrationAction::Delete(v53) => panic!("unexpected delete: {v53:?}"),
            }
        }

        assert_eq!(expected_updates, BTreeSet::new(), "missing updates");
    }

    fn empty_role_v53(role_id: v53::RoleId, name: &str, oid: u32) -> v53::state_update_kind::Role {
        v53::state_update_kind::Role {
            key: Some(v53::RoleKey { id: Some(role_id) }),
            value: Some(v53::RoleValue {
                name: name.to_string(),
                attributes: Some(Default::default()),
                membership: Some(Default::default()),
                vars: Some(Default::default()),
                oid,
            }),
        }
    }

    fn empty_role_v54(role_id: v54::RoleId, name: &str, oid: u32) -> v54::state_update_kind::Role {
        v54::state_update_kind::Role {
            key: Some(v54::RoleKey { id: Some(role_id) }),
            value: Some(v54::RoleValue {
                name: name.to_string(),
                attributes: Some(Default::default()),
                membership: Some(Default::default()),
                vars: Some(Default::default()),
                oid,
            }),
        }
    }

    fn role_with_membership_v53(
        role_id: v53::RoleId,
        name: &str,
        oid: u32,
    ) -> v53::state_update_kind::Role {
        let mut role = empty_role_v53(role_id, name, oid);
        let entry = v53::role_membership::Entry {
            key: Some(v53::RoleId {
                value: Some(v53::role_id::Value::User(2)),
            }),
            value: Some(v53::RoleId {
                value: Some(v53::role_id::Value::System(1)),
            }),
        };
        role.value
            .as_mut()
            .expect("missing value field")
            .membership
            .as_mut()
            .expect("missing membership field")
            .map
            .push(entry);
        role
    }

    fn role_with_membership_v54(
        role_id: v54::RoleId,
        name: &str,
        oid: u32,
    ) -> v54::state_update_kind::Role {
        let mut role = empty_role_v54(role_id, name, oid);
        let entry = v54::role_membership::Entry {
            key: Some(v54::RoleId {
                value: Some(v54::role_id::Value::User(2)),
            }),
            value: Some(v54::RoleId {
                value: Some(v54::role_id::Value::System(1)),
            }),
        };
        role.value
            .as_mut()
            .expect("missing value field")
            .membership
            .as_mut()
            .expect("missing membership field")
            .map
            .push(entry);
        role
    }

    fn empty_view_v53(role_id: v53::RoleId) -> v53::state_update_kind::Item {
        v53::state_update_kind::Item {
            key: Some(v53::ItemKey {
                gid: Some(v53::GlobalId {
                    value: Some(v53::global_id::Value::User(1)),
                }),
            }),
            value: Some(v53::ItemValue {
                schema_id: Some(v53::SchemaId {
                    value: Some(v53::schema_id::Value::System(4)),
                }),
                name: "t".to_string(),
                definition: Some(v53::CatalogItem {
                    value: Some(v53::catalog_item::Value::V1(v53::catalog_item::V1 {
                        create_sql: "CREATE TABLE t (a INT)".to_string(),
                    })),
                }),
                owner_id: Some(v53::RoleId {
                    value: Some(v53::role_id::Value::User(1)),
                }),
                privileges: vec![v53::MzAclItem {
                    grantee: Some(role_id),
                    grantor: Some(v53::RoleId {
                        value: Some(v53::role_id::Value::System(1)),
                    }),
                    acl_mode: Some(v53::AclMode { bitflags: 42666 }),
                }],
                oid: 8,
            }),
        }
    }

    fn empty_view_v54(role_id: v54::RoleId) -> v54::state_update_kind::Item {
        v54::state_update_kind::Item {
            key: Some(v54::ItemKey {
                gid: Some(v54::GlobalId {
                    value: Some(v54::global_id::Value::User(1)),
                }),
            }),
            value: Some(v54::ItemValue {
                schema_id: Some(v54::SchemaId {
                    value: Some(v54::schema_id::Value::System(4)),
                }),
                name: "t".to_string(),
                definition: Some(v54::CatalogItem {
                    value: Some(v54::catalog_item::Value::V1(v54::catalog_item::V1 {
                        create_sql: "CREATE TABLE t (a INT)".to_string(),
                    })),
                }),
                owner_id: Some(v54::RoleId {
                    value: Some(v54::role_id::Value::User(1)),
                }),
                privileges: vec![v54::MzAclItem {
                    grantee: Some(role_id),
                    grantor: Some(v54::RoleId {
                        value: Some(v54::role_id::Value::System(1)),
                    }),
                    acl_mode: Some(v54::AclMode { bitflags: 42666 }),
                }],
                oid: 8,
            }),
        }
    }
}
