// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::{MigrationAction, objects_v95 as v95, objects_v96 as v96};

/// Initializes the query-policy ID allocator.
pub fn upgrade(
    _snapshot: Vec<v95::StateUpdateKind>,
) -> Vec<MigrationAction<v95::StateUpdateKind, v96::StateUpdateKind>> {
    // Absent attachments remain omitted when serialized, so existing records need no rewrites.
    vec![MigrationAction::Insert(v96::StateUpdateKind::IdAlloc(
        v96::IdAlloc {
            key: v96::IdAllocKey {
                name: "user_query_policy".to_string(),
            },
            value: v96::IdAllocValue { next_id: 1 },
        },
    ))]
}
