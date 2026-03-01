// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v80 as v80;
use crate::durable::upgrade::objects_v81 as v81;

/// No-op migration. All types are JSON-compatible between v80 and v81.
/// The only change is adding the `PersistedIntrospectionSource` state
/// update kind variant and related types.
pub fn upgrade(
    _snapshot: Vec<v80::StateUpdateKind>,
) -> Vec<MigrationAction<v80::StateUpdateKind, v81::StateUpdateKind>> {
    Vec::new()
}
