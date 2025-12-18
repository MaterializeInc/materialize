// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v77 as v77;
use base64::prelude::*;
use mz_ore::now::SYSTEM_TIME;

// Old: SCRAM-SHA-256$<iters>:<salt>$<client_key>:<server_key>
// New: SCRAM-SHA-256$<iters>:<salt>$<stored_key>:<server_key>, where stored_key = SHA256(client_key)
fn migrate_scram_client_to_stored(hash_old: &str) -> Option<String> {
    let parts: Vec<&str> = hash_old.split('$').collect();
    if parts.len() != 3 || parts[0] != "SCRAM-SHA-256" {
        return None;
    }
    let iters_salt = parts[1];
    let key_parts: Vec<&str> = parts[2].split(':').collect();
    if key_parts.len() != 2 {
        return None;
    }
    let first_key_b64 = key_parts[0];
    let server_key_b64 = key_parts[1];

    let Ok(first_key) = BASE64_STANDARD.decode(first_key_b64) else {
        tracing::warn!("failed to decode base64 client key during scram migration");
        return None;
    };
    if first_key.len() != 32 {
        tracing::warn!("unexpected client key length during scram migration");
        return None;
    }

    let stored_key = openssl::sha::sha256(&first_key);
    let stored_key_b64 = BASE64_STANDARD.encode(stored_key);

    Some(format!(
        "SCRAM-SHA-256${}${}:{}",
        iters_salt, stored_key_b64, server_key_b64
    ))
}

pub fn upgrade(
    snapshot: Vec<v77::StateUpdateKind>,
) -> Vec<MigrationAction<v77::StateUpdateKind, v77::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v77::state_update_kind::Kind::RoleAuth(old_role_auth)) => {
                let Some(ref value) = old_role_auth.value else {
                    continue;
                };
                let Some(ref hashed_password) = value.password_hash else {
                    continue;
                };
                let Some(new_hash) = migrate_scram_client_to_stored(hashed_password) else {
                    continue;
                };

                let new_role_auth = v77::state_update_kind::RoleAuth {
                    key: old_role_auth.key.clone(),
                    value: Some(v77::RoleAuthValue {
                        password_hash: Some(new_hash),
                        updated_at: Some(v77::EpochMillis {
                            millis: SYSTEM_TIME(),
                        }),
                    }),
                };
                let old_role_auth = v77::StateUpdateKind {
                    kind: Some(v77::state_update_kind::Kind::RoleAuth(old_role_auth)),
                };
                let new_role_auth = v77::StateUpdateKind {
                    kind: Some(v77::state_update_kind::Kind::RoleAuth(new_role_auth)),
                };
                let migration = MigrationAction::Update(old_role_auth, new_role_auth);
                migrations.push(migration);
            }
            _ => {}
        }
    }
    migrations
}
