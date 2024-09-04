// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v63 as v63, objects_v64 as v64};

/// In v64, we added the fence token variant.
pub fn upgrade(
    _snapshot: Vec<v63::StateUpdateKind>,
) -> Vec<MigrationAction<v63::StateUpdateKind, v64::StateUpdateKind>> {
    Vec::new()
}
