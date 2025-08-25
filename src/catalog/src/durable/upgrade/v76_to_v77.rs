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
use crate::durable::upgrade::{objects_v76 as v76, objects_v77 as v77};
use crate::wire_compatible;

wire_compatible!(v76::RoleKey with v77::RoleKey);
wire_compatible!(v76::RoleValue with v77::RoleValue);
wire_compatible!(v76::RoleVars with v77::RoleVars);
wire_compatible!(v76::RoleMembership with v77::RoleMembership);
wire_compatible!(v76::role_membership::Entry with v77::role_membership::Entry);
wire_compatible!(v76::role_vars::Entry with v77::role_vars::Entry);
wire_compatible!(v76::role_vars::SqlSet with v77::role_vars::SqlSet);
wire_compatible!(v76::RoleAttributes with v77::RoleAttributes);
wire_compatible!(v76::RoleId with v77::RoleId);

/// This upgrade doesn't change any protos, simply retroactively marks mz_system as login
pub fn upgrade(
    snapshot: Vec<v76::StateUpdateKind>,
) -> Vec<MigrationAction<v76::StateUpdateKind, v77::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v76::state_update_kind::Kind::Role(old_role)) => {
                let new_role = v77::state_update_kind::Role::upgrade_from(old_role.clone());
                let old_role = v76::StateUpdateKind {
                    kind: Some(v76::state_update_kind::Kind::Role(old_role)),
                };
                let new_role = v77::StateUpdateKind {
                    kind: Some(v77::state_update_kind::Kind::Role(new_role)),
                };
                let migration = MigrationAction::Update(old_role, new_role);
                migrations.push(migration);
            }
            _ => {}
        }
    }
    migrations
}

impl UpgradeFrom<v76::state_update_kind::Role> for v77::state_update_kind::Role {
    fn upgrade_from(value: v76::state_update_kind::Role) -> Self {
        let new_key = value.key.map(|key| v77::RoleKey {
            id: key.id.map(v77::RoleId::upgrade_from),
        });

        let is_mz_system = value
            .value
            .as_ref()
            .map_or(false, |v| v.name == "mz_system");

        let mut new_value = value.value.map(|value| v77::RoleValue {
            name: value.name,
            oid: value.oid,
            attributes: value.attributes.map(v77::RoleAttributes::upgrade_from),
            membership: value.membership.map(v77::RoleMembership::upgrade_from),
            vars: value.vars.map(v77::RoleVars::upgrade_from),
        });

        if is_mz_system {
            if let Some(ref mut value) = new_value {
                if let Some(ref mut attrs) = value.attributes {
                    attrs.login = Some(true);
                }
            }
        }

        v77::state_update_kind::Role {
            key: new_key,
            value: new_value,
        }
    }
}

impl UpgradeFrom<v76::RoleVars> for v77::RoleVars {
    fn upgrade_from(value: v76::RoleVars) -> Self {
        v77::RoleVars {
            entries: value
                .entries
                .iter()
                .map(|val| v77::role_vars::Entry::upgrade_from(val.clone()))
                .collect(),
        }
    }
}

impl UpgradeFrom<v76::RoleMembership> for v77::RoleMembership {
    fn upgrade_from(value: v76::RoleMembership) -> Self {
        v77::RoleMembership {
            map: value
                .map
                .iter()
                .map(|val| v77::role_membership::Entry::upgrade_from(*val))
                .collect(),
        }
    }
}

impl UpgradeFrom<v76::role_membership::Entry> for v77::role_membership::Entry {
    fn upgrade_from(value: v76::role_membership::Entry) -> Self {
        v77::role_membership::Entry {
            key: value.key.map(v77::RoleId::upgrade_from),
            value: value.value.map(v77::RoleId::upgrade_from),
        }
    }
}

impl UpgradeFrom<v76::role_vars::Entry> for v77::role_vars::Entry {
    fn upgrade_from(value: v76::role_vars::Entry) -> Self {
        v77::role_vars::Entry {
            key: value.key,
            val: value.val.map(v77::role_vars::entry::Val::upgrade_from),
        }
    }
}

impl UpgradeFrom<v76::role_vars::entry::Val> for v77::role_vars::entry::Val {
    fn upgrade_from(value: v76::role_vars::entry::Val) -> Self {
        match value {
            v76::role_vars::entry::Val::Flat(x) => v77::role_vars::entry::Val::Flat(x),
            v76::role_vars::entry::Val::SqlSet(x) => {
                v77::role_vars::entry::Val::SqlSet(v77::role_vars::SqlSet::upgrade_from(x))
            }
        }
    }
}

impl UpgradeFrom<v76::role_vars::SqlSet> for v77::role_vars::SqlSet {
    fn upgrade_from(value: v76::role_vars::SqlSet) -> Self {
        v77::role_vars::SqlSet {
            entries: value.entries,
        }
    }
}

impl UpgradeFrom<v76::RoleAttributes> for v77::RoleAttributes {
    fn upgrade_from(value: v76::RoleAttributes) -> Self {
        v77::RoleAttributes {
            inherit: value.inherit,
            ..Default::default()
        }
    }
}

impl UpgradeFrom<v76::RoleId> for v77::RoleId {
    fn upgrade_from(value: v76::RoleId) -> Self {
        let value = match value.value {
            Some(v76::role_id::Value::System(x)) => Some(v77::role_id::Value::System(x)),
            Some(v76::role_id::Value::User(x)) => Some(v77::role_id::Value::User(x)),
            Some(v76::role_id::Value::Public(_)) => {
                Some(v77::role_id::Value::Public(v77::Empty {}))
            }
            Some(v76::role_id::Value::Predefined(x)) => Some(v77::role_id::Value::Predefined(x)),
            None => None,
        };
        v77::RoleId { value }
    }
}

impl UpgradeFrom<v76::RoleKey> for v77::RoleKey {
    fn upgrade_from(value: v76::RoleKey) -> Self {
        Self {
            id: value.id.map(v77::RoleId::upgrade_from),
        }
    }
}
