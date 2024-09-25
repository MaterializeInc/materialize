// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v65 as v65, objects_v66 as v66};

/// In v66, we add various Continual Task enum variants.
pub fn upgrade(
    _snapshot: Vec<v65::StateUpdateKind>,
) -> Vec<MigrationAction<v65::StateUpdateKind, v66::StateUpdateKind>> {
    Vec::new()
}
