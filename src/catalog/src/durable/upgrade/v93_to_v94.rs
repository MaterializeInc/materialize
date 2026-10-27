// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::{MigrationAction, objects_v93 as v93, objects_v94 as v94};

/// Initializes the query-policy ID allocator.
pub fn upgrade(
    _snapshot: Vec<v93::StateUpdateKind>,
) -> Vec<MigrationAction<v93::StateUpdateKind, v94::StateUpdateKind>> {
    // Absent attachments remain omitted when serialized, so existing records need no rewrites.
    vec![MigrationAction::Insert(v94::StateUpdateKind::IdAlloc(
        v94::IdAlloc {
            key: v94::IdAllocKey {
                name: "user_query_policy".to_string(),
            },
            value: v94::IdAllocValue { next_id: 1 },
        },
    ))]
}
