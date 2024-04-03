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

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v52 as v52, objects_v53 as v53};

wire_compatible!(v52::RoleAttributes with v53::RoleAttributes);
wire_compatible!(v52::RoleVars with v53::RoleVars);
wire_compatible!(v52::ItemKey with v53::ItemKey);
wire_compatible!(v52::SchemaId with v53::SchemaId);
wire_compatible!(v52::CatalogItem with v53::CatalogItem);
wire_compatible!(v52::AclMode with v53::AclMode);

const MZ_MONITOR_SYSTEM_ID: u64 = 3;
const MZ_MONITOR_REDACTED_SYSTEM_ID: u64 = 4;

const MZ_MONITOR_GROUP_ID: u64 = 1;
const MZ_MONITOR_REDACTED_GROUP_ID: u64 = 2;

/// Migrate group role from system to group.
///
/// `RoleId`s show up in a lot of places but we know that `mz_monitor` and `mz_monitor` redacted
/// can only show up in the following places:
///
///   - As a key in the Roles collections.
///   - In a `RoleMembership` in the Roles collection.
///   - As a grantee in an `MzAclItem` in the Items collection.
pub fn upgrade(
    snapshot: Vec<v52::StateUpdateKind>,
) -> Vec<MigrationAction<v52::StateUpdateKind, v53::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update.kind.expect("missing kind field") {
            v52::state_update_kind::Kind::Item(old_item) => {
                let item = old_item.clone();
                let key = item.key.as_ref().map(WireCompatible::convert);
                let value = item.value.map(|old_val| v53::ItemValue {
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
                    v52::StateUpdateKind {
                        kind: Some(v52::state_update_kind::Kind::Item(old_item)),
                    },
                    v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::Item(
                            v53::state_update_kind::Item { key, value },
                        )),
                    },
                ))
            }
            v52::state_update_kind::Kind::Role(old_role) => {
                let role = old_role.clone();
                let key = role.key.map(migrate_role_key);
                let value = role.value.map(|old_val| v53::RoleValue {
                    name: old_val.name,
                    attributes: old_val.attributes.as_ref().map(WireCompatible::convert),
                    membership: old_val.membership.map(migrate_role_membership),
                    vars: old_val.vars.as_ref().map(WireCompatible::convert),
                    oid: old_val.oid,
                });
                Some((
                    v52::StateUpdateKind {
                        kind: Some(v52::state_update_kind::Kind::Role(old_role)),
                    },
                    v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::Role(
                            v53::state_update_kind::Role { key, value },
                        )),
                    },
                ))
            }
            v52::state_update_kind::Kind::AuditLog(_)
            | v52::state_update_kind::Kind::Cluster(_)
            | v52::state_update_kind::Kind::ClusterReplica(_)
            | v52::state_update_kind::Kind::Comment(_)
            | v52::state_update_kind::Kind::Config(_)
            | v52::state_update_kind::Kind::Database(_)
            | v52::state_update_kind::Kind::DefaultPrivileges(_)
            | v52::state_update_kind::Kind::Epoch(_)
            | v52::state_update_kind::Kind::IdAlloc(_)
            | v52::state_update_kind::Kind::ClusterIntrospectionSourceIndex(_)
            | v52::state_update_kind::Kind::Schema(_)
            | v52::state_update_kind::Kind::Setting(_)
            | v52::state_update_kind::Kind::StorageUsage(_)
            | v52::state_update_kind::Kind::ServerConfiguration(_)
            | v52::state_update_kind::Kind::GidMapping(_)
            | v52::state_update_kind::Kind::SystemPrivileges(_)
            | v52::state_update_kind::Kind::StorageCollectionMetadata(_)
            | v52::state_update_kind::Kind::UnfinalizedShard(_)
            | v52::state_update_kind::Kind::PersistTxnShard(_) => None,
        })
        .map(|(old_update, new_update)| MigrationAction::Update(old_update, new_update))
        .collect()
}

fn migrate_role_id(role_id: v52::RoleId) -> v53::RoleId {
    let value = match role_id.value.expect("missing value") {
        v52::role_id::Value::System(MZ_MONITOR_SYSTEM_ID) => {
            v53::role_id::Value::Group(MZ_MONITOR_GROUP_ID)
        }
        v52::role_id::Value::System(MZ_MONITOR_REDACTED_SYSTEM_ID) => {
            v53::role_id::Value::Group(MZ_MONITOR_REDACTED_GROUP_ID)
        }
        v52::role_id::Value::System(id) => v53::role_id::Value::System(id),
        v52::role_id::Value::User(id) => v53::role_id::Value::User(id),
        v52::role_id::Value::Public(_empty) => v53::role_id::Value::Public(v53::Empty {}),
    };
    v53::RoleId { value: Some(value) }
}

fn migrate_mz_acl_item(mz_acl_item: v52::MzAclItem) -> v53::MzAclItem {
    v53::MzAclItem {
        grantee: mz_acl_item.grantee.map(migrate_role_id),
        grantor: mz_acl_item.grantor.map(migrate_role_id),
        acl_mode: mz_acl_item.acl_mode.as_ref().map(WireCompatible::convert),
    }
}

fn migrate_role_key(role_key: v52::RoleKey) -> v53::RoleKey {
    v53::RoleKey {
        id: role_key.id.map(migrate_role_id),
    }
}

fn migrate_role_membership(role_membership: v52::RoleMembership) -> v53::RoleMembership {
    v53::RoleMembership {
        map: role_membership
            .map
            .into_iter()
            .map(migrate_role_membership_entry)
            .collect(),
    }
}

fn migrate_role_membership_entry(
    role_membership_entry: v52::role_membership::Entry,
) -> v53::role_membership::Entry {
    v53::role_membership::Entry {
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

        let v52_mz_monitor_role_id = v52::RoleId {
            value: Some(v52::role_id::Value::System(MZ_MONITOR_SYSTEM_ID)),
        };
        let v52_mz_monitor_redacted_role_id = v52::RoleId {
            value: Some(v52::role_id::Value::System(MZ_MONITOR_REDACTED_SYSTEM_ID)),
        };
        let v52_mz_monitor = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Role(empty_role_v52(
                v52_mz_monitor_role_id.clone(),
                MZ_MONITOR_NAME,
                1,
            ))),
        };
        let v52_mz_monitor_redacted = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Role(empty_role_v52(
                v52_mz_monitor_redacted_role_id.clone(),
                MZ_MONITOR_REDACTED_NAME,
                2,
            ))),
        };
        let v52_mz_monitor_member = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Role(
                role_with_membership_v52(v52_mz_monitor_role_id.clone(), MZ_MONITOR_NAME, 1),
            )),
        };
        let v52_mz_monitor_redacted_member = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Role(
                role_with_membership_v52(
                    v52_mz_monitor_redacted_role_id.clone(),
                    MZ_MONITOR_REDACTED_NAME,
                    2,
                ),
            )),
        };
        let v52_item_monitor = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Item(empty_view_v52(
                v52_mz_monitor_role_id,
            ))),
        };
        let v52_item_monitor_redacted = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Item(empty_view_v52(
                v52_mz_monitor_redacted_role_id,
            ))),
        };

        let v53_mz_monitor_role_id = v53::RoleId {
            value: Some(v53::role_id::Value::Group(MZ_MONITOR_GROUP_ID)),
        };
        let v53_mz_monitor_redacted_role_id = v53::RoleId {
            value: Some(v53::role_id::Value::Group(MZ_MONITOR_REDACTED_GROUP_ID)),
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

        let mut expected_updates: BTreeSet<_> = [
            (v52_mz_monitor.clone(), v53_mz_monitor),
            (v52_mz_monitor_redacted.clone(), v53_mz_monitor_redacted),
            (v52_mz_monitor_member.clone(), v53_mz_monitor_member),
            (
                v52_mz_monitor_redacted_member.clone(),
                v53_mz_monitor_redacted_member,
            ),
            (v52_item_monitor.clone(), v53_item_monitor),
            (v52_item_monitor_redacted.clone(), v53_item_monitor_redacted),
        ]
        .into_iter()
        .collect();

        let actions = upgrade(vec![
            v52_mz_monitor,
            v52_mz_monitor_redacted,
            v52_mz_monitor_member,
            v52_mz_monitor_redacted_member,
            v52_item_monitor,
            v52_item_monitor_redacted,
        ]);

        for action in actions {
            match action {
                MigrationAction::Insert(v53) => panic!("unexpected insert: {v53:?}"),
                MigrationAction::Update(v52, v53) => {
                    assert!(
                        expected_updates.remove(&(v52.clone(), v53.clone())),
                        "unexpected update from {v52:?} to {v53:?}"
                    )
                }
                MigrationAction::Delete(v52) => panic!("unexpected delete: {v52:?}"),
            }
        }

        assert_eq!(expected_updates, BTreeSet::new(), "missing updates");
    }

    fn empty_role_v52(role_id: v52::RoleId, name: &str, oid: u32) -> v52::state_update_kind::Role {
        v52::state_update_kind::Role {
            key: Some(v52::RoleKey { id: Some(role_id) }),
            value: Some(v52::RoleValue {
                name: name.to_string(),
                attributes: Some(Default::default()),
                membership: Some(Default::default()),
                vars: Some(Default::default()),
                oid,
            }),
        }
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

    fn role_with_membership_v52(
        role_id: v52::RoleId,
        name: &str,
        oid: u32,
    ) -> v52::state_update_kind::Role {
        let mut role = empty_role_v52(role_id, name, oid);
        let entry = v52::role_membership::Entry {
            key: Some(v52::RoleId {
                value: Some(v52::role_id::Value::User(2)),
            }),
            value: Some(v52::RoleId {
                value: Some(v52::role_id::Value::System(1)),
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

    fn empty_view_v52(role_id: v52::RoleId) -> v52::state_update_kind::Item {
        v52::state_update_kind::Item {
            key: Some(v52::ItemKey {
                gid: Some(v52::GlobalId {
                    value: Some(v52::global_id::Value::User(1)),
                }),
            }),
            value: Some(v52::ItemValue {
                schema_id: Some(v52::SchemaId {
                    value: Some(v52::schema_id::Value::System(4)),
                }),
                name: "t".to_string(),
                definition: Some(v52::CatalogItem {
                    value: Some(v52::catalog_item::Value::V1(v52::catalog_item::V1 {
                        create_sql: "CREATE TABLE t (a INT)".to_string(),
                    })),
                }),
                owner_id: Some(v52::RoleId {
                    value: Some(v52::role_id::Value::User(1)),
                }),
                privileges: vec![v52::MzAclItem {
                    grantee: Some(role_id),
                    grantor: Some(v52::RoleId {
                        value: Some(v52::role_id::Value::System(1)),
                    }),
                    acl_mode: Some(v52::AclMode { bitflags: 42666 }),
                }],
                oid: 8,
            }),
        }
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
}
