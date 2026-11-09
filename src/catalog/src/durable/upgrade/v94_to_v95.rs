// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::{MigrationAction, objects_v94 as v94, objects_v95 as v95};

/// Initializes the query-policy ID allocator.
pub fn upgrade(
    _snapshot: Vec<v94::StateUpdateKind>,
) -> Vec<MigrationAction<v94::StateUpdateKind, v95::StateUpdateKind>> {
    // Absent attachments remain omitted when serialized, so existing records need no rewrites.
    vec![MigrationAction::Insert(v95::StateUpdateKind::IdAlloc(
        v95::IdAlloc {
            key: v95::IdAllocKey {
                name: "user_query_policy".to_string(),
            },
            value: v95::IdAllocValue { next_id: 1 },
        },
    ))]
}
