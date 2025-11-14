// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v78 as v78, objects_v79 as v79};

/// In v79, we added the replacement catalog item.
pub fn upgrade(
    _snapshot: Vec<v78::StateUpdateKind>,
) -> Vec<MigrationAction<v78::StateUpdateKind, v79::StateUpdateKind>> {
    Vec::new()
}
