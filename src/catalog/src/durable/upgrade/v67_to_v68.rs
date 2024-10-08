// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v67 as v67, objects_v68 as v68};

/// WIP
pub fn upgrade(
    _snapshot: Vec<v67::StateUpdateKind>,
) -> Vec<MigrationAction<v67::StateUpdateKind, v68::StateUpdateKind>> {
    Vec::new()
}
