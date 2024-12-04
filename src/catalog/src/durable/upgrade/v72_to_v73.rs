// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v72 as v72, objects_v73 as v73};

/// Adds the introspection source index global ID variant. Included in this change is shortening
/// cluster IDs from 64 bits to 32 bits.
pub fn upgrade(
    snapshot: Vec<v72::StateUpdateKind>,
) -> Vec<MigrationAction<v72::StateUpdateKind, v73::StateUpdateKind>> {
    let mut migrations = Vec::new();

    for update in snapshot {
        match update.kind {
            // Attempting to duplicate the introspection source index ID allocation logic in this
            // file would be extremely cumbersome and error-prone. Instead, we delete all existing
            // introspection source indexes and let the builtin migration code recreate them with
            // the new correct IDs.
            Some(v72::state_update_kind::Kind::ClusterIntrospectionSourceIndex(_)) => {
                let migration = MigrationAction::Delete(update);
                migrations.push(migration);
            }
            _ => {}
        }
    }

    migrations
}
