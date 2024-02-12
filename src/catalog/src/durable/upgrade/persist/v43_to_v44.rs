// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v43 as v43, objects_v44 as v44};

/// No-op migration for updating a comment in the proto file.
pub fn upgrade(
    _snapshot: Vec<v43::StateUpdateKind>,
) -> Vec<MigrationAction<v43::StateUpdateKind, v44::StateUpdateKind>> {
    Vec::new()
}
