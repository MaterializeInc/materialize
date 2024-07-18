// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v59 as v59, objects_v60 as v60};

/// No-op migration. In v60, we add some new audit log variants.
pub fn upgrade(
    _snapshot: Vec<v59::StateUpdateKind>,
) -> Vec<MigrationAction<v59::StateUpdateKind, v60::StateUpdateKind>> {
    Vec::new()
}
