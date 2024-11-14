// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v71 as v71, objects_v72 as v72};

const USER_NETWORK_POLICY_ID_ALLOC_KEY: &str = "user_network_policy";

/// Adds the "user_network_policy" ID allocator.
pub fn upgrade(
    snapshot: Vec<v71::StateUpdateKind>,
) -> Vec<MigrationAction<v71::StateUpdateKind, v72::StateUpdateKind>> {
    // New environments will already have this ID Allocator created.
    let network_policies_id_alloc = snapshot
        .iter()
        .filter_map(|update| match &update.kind {
            Some(v71::state_update_kind::Kind::IdAlloc(id_allocator)) => {
                id_allocator.key.as_ref().map(|key| &key.name)
            }
            _ => None,
        })
        .any(|id_alloc_name| id_alloc_name == USER_NETWORK_POLICY_ID_ALLOC_KEY);

    if !network_policies_id_alloc {
        let key = v72::IdAllocKey {
            name: USER_NETWORK_POLICY_ID_ALLOC_KEY.to_string(),
        };
        // V71 introduced a default network policy with ID 1, so ID 2 is next.
        let val = v72::IdAllocValue { next_id: 2 };

        let create_id_alloc = v72::StateUpdateKind {
            kind: Some(v72::state_update_kind::Kind::IdAlloc(
                v72::state_update_kind::IdAlloc {
                    key: Some(key),
                    value: Some(val),
                },
            )),
        };
        vec![MigrationAction::Insert(create_id_alloc)]
    } else {
        vec![]
    }
}
