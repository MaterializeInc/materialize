// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v73 as v73, objects_v74 as v74};

pub fn upgrade(
    _snapshot: Vec<v73::StateUpdateKind>,
) -> Vec<MigrationAction<v73::StateUpdateKind, v74::StateUpdateKind>> {
    Vec::new()
}
