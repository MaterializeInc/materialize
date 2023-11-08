// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::MigrationAction;
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::upgrade::{objects_v37, objects_v38};

/// Update fingerprint of builtin types to have timestsamp with precision
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    const GID_MAPPING_COLLECTION: TypedCollection<
        objects_v37::GidMappingKey,
        objects_v37::GidMappingValue,
    > = TypedCollection::new("system_gid_mapping");

    GID_MAPPING_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                let new_key: objects_v38::GidMappingKey = gid_mapping_key_from(key.clone());
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

fn gid_mapping_key_from(key: objects_v37::GidMappingKey) -> objects_v38::GidMappingKey {
    objects_v38::GidMappingKey {
        schema_name: key.schema_name,
        object_type: key.object_type,
        object_name: key.object_name,
    }
}
