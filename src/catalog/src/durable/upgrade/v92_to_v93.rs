// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::{MigrationAction, objects_v92 as v92, objects_v93 as v93};

/// Initializes the query-policy ID allocator. Query-policy attachments on
/// existing clusters and roles deserialize as absent, so need no rewrites.
pub fn upgrade(
    _snapshot: Vec<v92::StateUpdateKind>,
) -> Vec<MigrationAction<v92::StateUpdateKind, v93::StateUpdateKind>> {
    vec![MigrationAction::Insert(v93::StateUpdateKind::IdAlloc(
        v93::IdAlloc {
            key: v93::IdAllocKey {
                name: "user_query_policy".to_string(),
            },
            value: v93::IdAllocValue { next_id: 1 },
        },
    ))]
}
