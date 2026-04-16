// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v81 as v81;
use crate::durable::upgrade::objects_v82 as v82;

/// No-op migration. All types are JSON-compatible between v81 and v82.
/// The only change is adding the `enable_upsert_v2: Option<bool>` field to
/// `ManagedCluster`, which defaults to `None` when absent via `#[serde(default)]`.
pub fn upgrade(
    _snapshot: Vec<v81::StateUpdateKind>,
) -> Vec<MigrationAction<v81::StateUpdateKind, v82::StateUpdateKind>> {
    Vec::new()
}
