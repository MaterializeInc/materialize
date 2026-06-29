// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL-205: relocate the `CREATE_NETWORK_POLICY` privilege bit.
//!
//! Before v89, `CREATE_NETWORK_POLICY` was stored at `1 << 32`. The upper 32
//! bits of `AclMode` are reserved for grant options (bit `n + 32` is the
//! grant-option slot for bit `n`), so bit 32 is the grant-option slot for
//! `INSERT` (bit 0). Grant options aren't implemented today, so there's no
//! live conflict, but once they ship a stored `CREATE_NETWORK_POLICY` grant
//! would decode as `INSERT WITH GRANT OPTION`.
//!
//! This migration rewrites every persisted `AclMode` that has bit 32 set:
//! clear bit 32, set bit 28 (the next free slot below bit 31, under the
//! "grow downward from 31" rule for Materialize-custom privileges). Records
//! without the bit emit no action, so we don't churn the catalog with
//! retract+re-add of identical bytes.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v88 as v88;
use crate::durable::upgrade::objects_v89 as v89;

// Literals inlined deliberately. Migrations don't import constants from
// elsewhere in the codebase, so a later code change can't alter what this
// migration did.
const OLD_BIT: u64 = 1u64 << 32;
const NEW_BIT: u64 = 1u64 << 28;

fn rewrite_bits(bits: u64) -> u64 {
    if bits & OLD_BIT != 0 {
        (bits & !OLD_BIT) | NEW_BIT
    } else {
        bits
    }
}

fn mode_needs_rewrite(m: &v88::AclMode) -> bool {
    m.bitflags & OLD_BIT != 0
}

fn items_need_rewrite(items: &[v88::MzAclItem]) -> bool {
    items.iter().any(|i| mode_needs_rewrite(&i.acl_mode))
}

/// v88 and v89 have the same shape. Only the meaning of one bit changes, so
/// a serde round-trip is enough to clone a v88 record as the matching v89
/// record. The migration then mutates the `AclMode` bits in place.
fn clone_as_v89<A, B>(old: &A) -> B
where
    A: Serialize,
    B: DeserializeOwned,
{
    let bytes = serde_json::to_vec(old).expect("v88 record serializes");
    serde_json::from_slice(&bytes).expect("v88 and v89 are byte-identical")
}

fn rewrite_mode_in_place(m: &mut v89::AclMode) {
    m.bitflags = rewrite_bits(m.bitflags);
}

fn rewrite_items_in_place(items: &mut [v89::MzAclItem]) {
    for i in items {
        rewrite_mode_in_place(&mut i.acl_mode);
    }
}

pub fn upgrade(
    snapshot: Vec<v88::StateUpdateKind>,
) -> Vec<MigrationAction<v88::StateUpdateKind, v89::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        if let Some(new_update) = migrate(&update) {
            migrations.push(MigrationAction::Update(update, new_update));
        }
    }
    migrations
}

fn migrate(update: &v88::StateUpdateKind) -> Option<v89::StateUpdateKind> {
    match update {
        v88::StateUpdateKind::Cluster(c) if items_need_rewrite(&c.value.privileges) => {
            let mut new: v89::Cluster = clone_as_v89(c);
            rewrite_items_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::Cluster(new))
        }
        v88::StateUpdateKind::Database(d) if items_need_rewrite(&d.value.privileges) => {
            let mut new: v89::Database = clone_as_v89(d);
            rewrite_items_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::Database(new))
        }
        v88::StateUpdateKind::Schema(s) if items_need_rewrite(&s.value.privileges) => {
            let mut new: v89::Schema = clone_as_v89(s);
            rewrite_items_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::Schema(new))
        }
        v88::StateUpdateKind::Item(i) if items_need_rewrite(&i.value.privileges) => {
            let mut new: v89::Item = clone_as_v89(i);
            rewrite_items_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::Item(new))
        }
        v88::StateUpdateKind::NetworkPolicy(np) if items_need_rewrite(&np.value.privileges) => {
            let mut new: v89::NetworkPolicy = clone_as_v89(np);
            rewrite_items_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::NetworkPolicy(new))
        }
        v88::StateUpdateKind::DefaultPrivileges(dp) if mode_needs_rewrite(&dp.value.privileges) => {
            let mut new: v89::DefaultPrivileges = clone_as_v89(dp);
            rewrite_mode_in_place(&mut new.value.privileges);
            Some(v89::StateUpdateKind::DefaultPrivileges(new))
        }
        v88::StateUpdateKind::SystemPrivileges(sp) if mode_needs_rewrite(&sp.value.acl_mode) => {
            let mut new: v89::SystemPrivileges = clone_as_v89(sp);
            rewrite_mode_in_place(&mut new.value.acl_mode);
            Some(v89::StateUpdateKind::SystemPrivileges(new))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::upgrade::MigrationAction;

    const SELECT_BIT: u64 = 1 << 1;

    fn acl_with(bits: u64) -> v88::MzAclItem {
        v88::MzAclItem {
            grantee: v88::RoleId::User(7),
            grantor: v88::RoleId::User(1),
            acl_mode: v88::AclMode { bitflags: bits },
        }
    }

    fn cluster(privileges: Vec<v88::MzAclItem>) -> v88::StateUpdateKind {
        v88::StateUpdateKind::Cluster(v88::Cluster {
            key: v88::ClusterKey {
                id: v88::ClusterId::User(1),
            },
            value: v88::ClusterValue {
                name: "c".to_string(),
                owner_id: v88::RoleId::User(1),
                privileges,
                config: v88::ClusterConfig {
                    workload_class: None,
                    variant: v88::ClusterVariant::Unmanaged,
                },
            },
        })
    }

    fn database(privileges: Vec<v88::MzAclItem>) -> v88::StateUpdateKind {
        v88::StateUpdateKind::Database(v88::Database {
            key: v88::DatabaseKey {
                id: v88::DatabaseId::User(1),
            },
            value: v88::DatabaseValue {
                name: "d".to_string(),
                owner_id: v88::RoleId::User(1),
                privileges,
                oid: 100,
            },
        })
    }

    fn schema(privileges: Vec<v88::MzAclItem>) -> v88::StateUpdateKind {
        v88::StateUpdateKind::Schema(v88::Schema {
            key: v88::SchemaKey {
                id: v88::SchemaId::User(1),
            },
            value: v88::SchemaValue {
                database_id: None,
                name: "s".to_string(),
                owner_id: v88::RoleId::User(1),
                privileges,
                oid: 101,
            },
        })
    }

    fn network_policy(privileges: Vec<v88::MzAclItem>) -> v88::StateUpdateKind {
        v88::StateUpdateKind::NetworkPolicy(v88::NetworkPolicy {
            key: v88::NetworkPolicyKey {
                id: v88::NetworkPolicyId::User(1),
            },
            value: v88::NetworkPolicyValue {
                name: "np".to_string(),
                rules: vec![],
                owner_id: v88::RoleId::User(1),
                privileges,
                oid: 102,
            },
        })
    }

    fn default_privileges(privileges_bits: u64) -> v88::StateUpdateKind {
        v88::StateUpdateKind::DefaultPrivileges(v88::DefaultPrivileges {
            key: v88::DefaultPrivilegesKey {
                role_id: v88::RoleId::User(1),
                database_id: None,
                schema_id: None,
                object_type: v88::ObjectType::Table,
                grantee: v88::RoleId::User(2),
            },
            value: v88::DefaultPrivilegesValue {
                privileges: v88::AclMode {
                    bitflags: privileges_bits,
                },
            },
        })
    }

    fn system_privileges(acl_mode_bits: u64) -> v88::StateUpdateKind {
        v88::StateUpdateKind::SystemPrivileges(v88::SystemPrivileges {
            key: v88::SystemPrivilegesKey {
                grantee: v88::RoleId::User(2),
                grantor: v88::RoleId::User(1),
            },
            value: v88::SystemPrivilegesValue {
                acl_mode: v88::AclMode {
                    bitflags: acl_mode_bits,
                },
            },
        })
    }

    fn assert_rewritten_to_new_bit(
        migrations: &[MigrationAction<v88::StateUpdateKind, v89::StateUpdateKind>],
    ) {
        assert_eq!(migrations.len(), 1, "expected exactly one migration action");
        let MigrationAction::Update(_, new) = &migrations[0] else {
            panic!("expected an Update migration action");
        };
        let bits = match new {
            v89::StateUpdateKind::Cluster(c) => c.value.privileges[0].acl_mode.bitflags,
            v89::StateUpdateKind::Database(d) => d.value.privileges[0].acl_mode.bitflags,
            v89::StateUpdateKind::Schema(s) => s.value.privileges[0].acl_mode.bitflags,
            v89::StateUpdateKind::Item(i) => i.value.privileges[0].acl_mode.bitflags,
            v89::StateUpdateKind::NetworkPolicy(np) => np.value.privileges[0].acl_mode.bitflags,
            v89::StateUpdateKind::DefaultPrivileges(dp) => dp.value.privileges.bitflags,
            v89::StateUpdateKind::SystemPrivileges(sp) => sp.value.acl_mode.bitflags,
            other => panic!("unexpected migrated kind: {other:?}"),
        };
        assert_eq!(
            bits & OLD_BIT,
            0,
            "old bit 32 must be cleared, got {bits:#x}",
        );
        assert!(bits & NEW_BIT != 0, "new bit 28 must be set, got {bits:#x}",);
    }

    #[mz_ore::test]
    fn rewrites_cluster_privilege() {
        let migrations = upgrade(vec![cluster(vec![acl_with(OLD_BIT)])]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn rewrites_database_privilege() {
        let migrations = upgrade(vec![database(vec![acl_with(OLD_BIT)])]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn rewrites_schema_privilege() {
        let migrations = upgrade(vec![schema(vec![acl_with(OLD_BIT)])]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn rewrites_network_policy_privilege() {
        let migrations = upgrade(vec![network_policy(vec![acl_with(OLD_BIT)])]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn rewrites_default_privileges() {
        let migrations = upgrade(vec![default_privileges(OLD_BIT)]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn rewrites_system_privileges() {
        let migrations = upgrade(vec![system_privileges(OLD_BIT)]);
        assert_rewritten_to_new_bit(&migrations);
    }

    #[mz_ore::test]
    fn preserves_other_bits() {
        // CREATE_NETWORK_POLICY (old bit 32) combined with SELECT (bit 1).
        // SELECT survives, bit 32 clears, bit 28 sets.
        let migrations = upgrade(vec![system_privileges(OLD_BIT | SELECT_BIT)]);
        assert_eq!(migrations.len(), 1);
        let MigrationAction::Update(_, v89::StateUpdateKind::SystemPrivileges(sp)) = &migrations[0]
        else {
            panic!("expected SystemPrivileges update");
        };
        assert_eq!(sp.value.acl_mode.bitflags, NEW_BIT | SELECT_BIT);
    }

    #[mz_ore::test]
    fn no_action_when_bit_not_set() {
        // A SELECT-only grant has no bit 32. The migration must not emit a
        // retract+add of identical bytes.
        let migrations = upgrade(vec![system_privileges(SELECT_BIT)]);
        assert!(
            migrations.is_empty(),
            "expected no migration; got {:?}",
            migrations
        );
    }

    #[mz_ore::test]
    fn no_action_for_records_without_acl_mode() {
        // A FenceToken carries no `AclMode` and must be passed through.
        let token = v88::StateUpdateKind::FenceToken(v88::FenceToken {
            deploy_generation: 0,
            epoch: 1,
        });
        let migrations = upgrade(vec![token]);
        assert!(migrations.is_empty());
    }

    #[mz_ore::test]
    fn preserves_unrelated_fields() {
        // After rewriting a Cluster's privileges, every other field comes
        // back unchanged from the v88->v89 conversion.
        let migrations = upgrade(vec![cluster(vec![acl_with(OLD_BIT)])]);
        let MigrationAction::Update(_, v89::StateUpdateKind::Cluster(new)) = &migrations[0] else {
            panic!("expected Cluster update");
        };
        assert_eq!(new.value.name, "c");
        assert_eq!(new.value.owner_id, v89::RoleId::User(1));
        assert!(matches!(
            new.value.config.variant,
            v89::ClusterVariant::Unmanaged
        ));
    }
}
