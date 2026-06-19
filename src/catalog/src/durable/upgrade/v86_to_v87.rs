// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v86 as v86;
use crate::durable::upgrade::objects_v87 as v87;

/// No-op migration. v87 adds the `BranchDescriptor` collection (and its
/// `StateUpdateKind` variant); existing v86 rows are JSON-compatible and
/// don't need rewriting.
pub fn upgrade(
    _snapshot: Vec<v86::StateUpdateKind>,
) -> Vec<MigrationAction<v86::StateUpdateKind, v87::StateUpdateKind>> {
    Vec::new()
}
