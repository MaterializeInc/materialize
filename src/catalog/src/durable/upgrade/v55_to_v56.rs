// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v55 as v55, objects_v56 as v56};

/// No-op migration for adding storage objects.
pub fn upgrade(
    _snapshot: Vec<v55::StateUpdateKind>,
) -> Vec<MigrationAction<v55::StateUpdateKind, v56::StateUpdateKind>> {
    Vec::new()
}
