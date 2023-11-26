// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::{MigrationAction, WireCompatible};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::upgrade::{objects_v44 as v44, objects_v45 as v45};

const DATABASE_COLLECTION: TypedCollection<v44::DatabaseKey, v44::DatabaseValue> =
    TypedCollection::new("database");

const MZ_SYSTEM_ROLE_ID: v45::RoleId = v45::RoleId {
    value: Some(v45::role_id::Value::System(1)),
};

/// Sample migration that converts Database IDs to u32 and converts all even IDs to be owned by
/// MZ_SYSTEM.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    DATABASE_COLLECTION
        .migrate_to(tx, |entries| {
            entries
                .iter()
                .map(|(key, value)| {
                    let mut new_value = v45::DatabaseValue::convert(value);
                    let id = key.id.clone().expect("missing field");
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
                    MigrationAction::Update(key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await?;
    Ok(())
}
