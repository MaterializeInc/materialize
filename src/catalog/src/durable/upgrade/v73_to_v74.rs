// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::traits::UpgradeFrom;
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v73 as v73, objects_v74 as v74};
use crate::json_compatible;

json_compatible!(v73::RoleKey with v74::RoleKey);

/// in v74, we add attributes to RoleAttribute.
pub fn upgrade(
    snapshot: Vec<v73::StateUpdateKind>,
) -> Vec<MigrationAction<v73::StateUpdateKind, v74::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v73::state_update_kind::Kind::Role(old_role)) => {
                let new_role = v74::state_update_kind::Role::upgrade_from(old_role.clone());
                let old_role = v73::StateUpdateKind {
                    kind: Some(v73::state_update_kind::Kind::Role(old_role)),
                };
                let new_role = v74::StateUpdateKind {
                    kind: Some(v74::state_update_kind::Kind::Role(new_role)),
                };

                let migration = MigrationAction::Update(old_role, new_role);
                migrations.push(migration);
            }
            _ => {
                // We don't need to do anything for other kinds of updates.
                // The upgrade from v73 to v74 is only concerned with the Role kind.
            }
        }
    }
    migrations
}

impl UpgradeFrom<v73::state_update_kind::Role> for v74::state_update_kind::Role {
    fn upgrade_from(value: v73::state_update_kind::Role) -> Self {
        let new_key = value.key.map(|key| v74::RoleKey {
            id: key.id.map(v74::RoleId::upgrade_from),
        });
        let new_value = value.value.map(|value| v74::RoleValue {
            name: value.name,
            oid: value.oid,
            attributes: value.attributes.map(v74::RoleAttributes::upgrade_from),
            membership: value.membership.map(v74::RoleMembership::upgrade_from),
            vars: value.vars.map(v74::RoleVars::upgrade_from),
        });
        v74::state_update_kind::Role {
            key: new_key,
            value: new_value,
        }
    }
}

impl UpgradeFrom<v73::RoleVars> for v74::RoleVars {
    fn upgrade_from(value: v73::RoleVars) -> Self {
        v74::RoleVars {
            entries: value
                .entries
                .iter()
                .map(|val| v74::role_vars::Entry::upgrade_from(val.clone()))
                .collect(),
        }
    }
}

impl UpgradeFrom<v73::RoleMembership> for v74::RoleMembership {
    fn upgrade_from(value: v73::RoleMembership) -> Self {
        v74::RoleMembership {
            map: value
                .map
                .iter()
                .map(|val| v74::role_membership::Entry::upgrade_from(*val))
                .collect(),
        }
    }
}

impl UpgradeFrom<v73::role_membership::Entry> for v74::role_membership::Entry {
    fn upgrade_from(value: v73::role_membership::Entry) -> Self {
        v74::role_membership::Entry {
            key: value.key.map(v74::RoleId::upgrade_from),
            value: value.value.map(v74::RoleId::upgrade_from),
        }
    }
}

impl UpgradeFrom<v73::role_vars::Entry> for v74::role_vars::Entry {
    fn upgrade_from(value: v73::role_vars::Entry) -> Self {
        v74::role_vars::Entry {
            key: value.key,
            val: value.val.map(v74::role_vars::entry::Val::upgrade_from),
        }
    }
}

impl UpgradeFrom<v73::role_vars::entry::Val> for v74::role_vars::entry::Val {
    fn upgrade_from(value: v73::role_vars::entry::Val) -> Self {
        match value {
            v73::role_vars::entry::Val::Flat(x) => v74::role_vars::entry::Val::Flat(x),
            v73::role_vars::entry::Val::SqlSet(x) => {
                v74::role_vars::entry::Val::SqlSet(v74::role_vars::SqlSet::upgrade_from(x))
            }
        }
    }
}
impl UpgradeFrom<v73::role_vars::SqlSet> for v74::role_vars::SqlSet {
    fn upgrade_from(value: v73::role_vars::SqlSet) -> Self {
        v74::role_vars::SqlSet {
            entries: value.entries,
        }
    }
}

impl UpgradeFrom<v73::RoleAttributes> for v74::RoleAttributes {
    fn upgrade_from(value: v73::RoleAttributes) -> Self {
        v74::RoleAttributes {
            inherit: value.inherit,
            ..Default::default()
        }
    }
}

impl UpgradeFrom<v73::RoleId> for v74::RoleId {
    fn upgrade_from(value: v73::RoleId) -> Self {
        let value = match value.value {
            Some(v73::role_id::Value::System(x)) => Some(v74::role_id::Value::System(x)),
            Some(v73::role_id::Value::User(x)) => Some(v74::role_id::Value::User(x)),
            Some(v73::role_id::Value::Public(_)) => {
                Some(v74::role_id::Value::Public(v74::Empty {}))
            }
            Some(v73::role_id::Value::Predefined(x)) => Some(v74::role_id::Value::Predefined(x)),
            None => None,
        };
        v74::RoleId { value }
    }
}

impl UpgradeFrom<v73::RoleKey> for v74::RoleKey {
    fn upgrade_from(value: v73::RoleKey) -> Self {
        Self {
            id: value.id.map(v74::RoleId::upgrade_from),
        }
    }
}
