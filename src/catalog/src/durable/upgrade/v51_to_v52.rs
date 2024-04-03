// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v51 as v51, objects_v52 as v52};

/// No-op migration for adding storage objects.
pub fn upgrade(
    _snapshot: Vec<v51::StateUpdateKind>,
) -> Vec<MigrationAction<v51::StateUpdateKind, v52::StateUpdateKind>> {
    Vec::new()
}
