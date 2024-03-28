// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v50 as v50, objects_v51 as v51};

/// No-op migration for removing storage objects.
pub fn upgrade(
    _snapshot: Vec<v50::StateUpdateKind>,
) -> Vec<MigrationAction<v50::StateUpdateKind, v51::StateUpdateKind>> {
    Vec::new()
}
