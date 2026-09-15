// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v93 as v93;
use crate::durable::upgrade::objects_v94 as v94;

/// Adds two collections without changing any existing records.
pub fn upgrade(
    _snapshot: Vec<v93::StateUpdateKind>,
) -> Vec<MigrationAction<v93::StateUpdateKind, v94::StateUpdateKind>> {
    Vec::new()
}
