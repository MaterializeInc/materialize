// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v82 as v82;
use crate::durable::upgrade::objects_v83 as v83;

/// No-op migration. Adds the `pre_allocated_shards` collection which starts empty.
pub fn upgrade(
    snapshot: Vec<v82::StateUpdateKind>,
) -> Vec<MigrationAction<v82::StateUpdateKind, v83::StateUpdateKind>> {
    let _ = snapshot;
    Vec::new()
}
