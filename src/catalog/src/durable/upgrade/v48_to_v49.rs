// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v48 as v48, objects_v49 as v49};

/// No-op migration for removing storage objects.
pub fn upgrade(
    _snapshot: Vec<v48::StateUpdateKind>,
) -> Vec<MigrationAction<v48::StateUpdateKind, v49::StateUpdateKind>> {
    Vec::new()
}
