// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v61 as v61, objects_v62 as v62};

/// In v62, we removed storage usage.
pub fn upgrade(
    snapshot: Vec<v61::StateUpdateKind>,
) -> Vec<MigrationAction<v61::StateUpdateKind, v62::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match &update.kind {
            Some(v61::state_update_kind::Kind::StorageUsage(_)) => {
                Some(MigrationAction::Delete(update))
            }
            _ => None,
        })
        .collect()
}
