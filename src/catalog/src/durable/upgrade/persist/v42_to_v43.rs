// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v42 as v42, objects_v43 as v43};

/// No-op migration for removing storage objects.
pub fn upgrade(
    _snapshot: Vec<v42::StateUpdateKind>,
) -> Vec<MigrationAction<v42::StateUpdateKind, v43::StateUpdateKind>> {
    Vec::new()
}
