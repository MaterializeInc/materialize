// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v94 as v94;
use crate::durable::upgrade::objects_v95 as v95;

/// Adds the written plan selection collection without changing any existing records.
pub fn upgrade(
    _snapshot: Vec<v94::StateUpdateKind>,
) -> Vec<MigrationAction<v94::StateUpdateKind, v95::StateUpdateKind>> {
    Vec::new()
}
