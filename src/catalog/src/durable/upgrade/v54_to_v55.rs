// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v54 as v54, objects_v55 as v55};

/// No-op migration for adding storage objects.
pub fn upgrade(
    _snapshot: Vec<v54::StateUpdateKind>,
) -> Vec<MigrationAction<v54::StateUpdateKind, v55::StateUpdateKind>> {
    Vec::new()
}
