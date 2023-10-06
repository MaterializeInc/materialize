// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::{objects_v37, objects_v38, MigrationAction};
use crate::{StashError, Transaction, TypedCollection};

/// Update fingerprint of builtin types to have timestsamp with precision
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    const GID_MAPPING_COLLECTION: TypedCollection<
        objects_v37::GidMappingKey,
        objects_v37::GidMappingValue,
    > = TypedCollection::new("system_gid_mapping");

    GID_MAPPING_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                let new_key: objects_v38::GidMappingKey = key.clone().into();
                let new_fingerprint = value
                    .fingerprint
                    .replace("\"TimestampTz\"", "{\"TimestampTz\":{\"precision\":null}}");
                let new_value: objects_v38::GidMappingValue = objects_v38::GidMappingValue {
                    id: value.id,
                    fingerprint: new_fingerprint,
                };
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;
    Ok(())
}

impl From<objects_v37::GidMappingKey> for objects_v38::GidMappingKey {
    fn from(key: objects_v37::GidMappingKey) -> Self {
        objects_v38::GidMappingKey {
            schema_name: key.schema_name,
            object_type: key.object_type,
            object_name: key.object_name,
        }
    }
}
