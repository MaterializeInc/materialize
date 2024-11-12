// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v68 as v68, objects_v69 as v69};

/// In v68, we add the Network Policy resources.
pub fn upgrade(
    _snapshot: Vec<v68::StateUpdateKind>,
) -> Vec<MigrationAction<v68::StateUpdateKind, v69::StateUpdateKind>> {
    vec![]
}
