// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v79 as v79;
use crate::durable::upgrade::objects_v80 as v80;

/// No-op migration. All types are JSON-compatible between v79 and v80.
/// The only change is adding new audit log event types and fields.
///
/// There is one exception, `AlterSetClusterV1`, which was changed in an
/// incompatible way. We can do this only because we know this audit event has
/// never been written before.
pub fn upgrade(
    _snapshot: Vec<v79::StateUpdateKind>,
) -> Vec<MigrationAction<v79::StateUpdateKind, v80::StateUpdateKind>> {
    Vec::new()
}
