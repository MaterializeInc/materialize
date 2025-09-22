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
use crate::durable::upgrade::objects_v76 as v76;

/// This upgrade doesn't change any protos, simply retroactively marks mz_system as login
pub fn upgrade(
    snapshot: Vec<v76::StateUpdateKind>,
) -> Vec<MigrationAction<v76::StateUpdateKind, v76::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v76::state_update_kind::Kind::Role(old_role)) => {
                let new_role = v76::state_update_kind::Role::upgrade_from(old_role.clone());
                let old_role = v76::StateUpdateKind {
                    kind: Some(v76::state_update_kind::Kind::Role(old_role)),
                };
                let new_role = v76::StateUpdateKind {
                    kind: Some(v76::state_update_kind::Kind::Role(new_role)),
                };
                let migration = MigrationAction::Update(old_role, new_role);
                migrations.push(migration);
            }
            _ => {}
        }
    }
    migrations
}

impl UpgradeFrom<v76::state_update_kind::Role> for v76::state_update_kind::Role {
    fn upgrade_from(value: v76::state_update_kind::Role) -> Self {
        let new_key = value.key;

        let is_mz_system = value
            .value
            .as_ref()
            .map_or(false, |v| v.name == "mz_system");

        let mut new_value = value.value.map(|value| v76::RoleValue {
            name: value.name,
            oid: value.oid,
            attributes: value.attributes,
            membership: value.membership,
            vars: value.vars,
        });

        if is_mz_system {
            if let Some(ref mut value) = new_value {
                if let Some(ref mut attrs) = value.attributes {
                    attrs.login = Some(true);
                }
            }
        }

        v76::state_update_kind::Role {
            key: new_key,
            value: new_value,
        }
    }
}
