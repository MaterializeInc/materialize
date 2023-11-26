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

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v44 as v44, objects_v45 as v45};

wire_compatible!(v44::DatabaseValue with v45::DatabaseValue);

const MZ_SYSTEM_ROLE_ID: v45::RoleId = v45::RoleId {
    value: Some(v45::role_id::Value::System(1)),
};

/// Sample migration that converts Database IDs to u32 and converts all even IDs to be owned by
/// MZ_SYSTEM.
pub fn upgrade(
    snapshot: Vec<v44::StateUpdateKind>,
) -> Vec<MigrationAction<v44::StateUpdateKind, v45::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update.kind.clone().expect("missing field") {
            v44::state_update_kind::Kind::Database(database) => {
                let key = database.key.expect("missing field");
                let value = database.value.expect("missing field");
                let mut new_value = v45::DatabaseValue::convert(&value);
                let id = key.id.expect("missing field");
                let id_value = id.value.expect("missing field");
                let id_value = match id_value {
                    v44::database_id::Value::System(id) =>
                    {
                        #[allow(clippy::as_conversions)]
                        v45::database_id::Value::System(id as u32)
                    }
                    v44::database_id::Value::User(id) =>
                    {
                        #[allow(clippy::as_conversions)]
                        v45::database_id::Value::User(id as u32)
                    }
                };
                let id_value_contents = match &id_value {
                    v45::database_id::Value::System(id) => id,
                    v45::database_id::Value::User(id) => id,
                };
                if id_value_contents % 2 == 0 {
                    new_value.owner_id = Some(MZ_SYSTEM_ROLE_ID);
                }
                let new_key = v45::DatabaseKey {
                    id: Some(v45::DatabaseId {
                        value: Some(id_value),
                    }),
                };

                let new_update = v45::StateUpdateKind {
                    kind: Some(v45::state_update_kind::Kind::Database(
                        v45::state_update_kind::Database {
                            key: Some(new_key),
                            value: Some(new_value),
                        },
                    )),
                };

                let migration_action = MigrationAction::Update(update, new_update);
                Some(migration_action)
            }
            _ => None,
        })
        .collect()
}
