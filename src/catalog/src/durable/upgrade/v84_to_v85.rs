// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v84 as v84;
use crate::durable::upgrade::objects_v85 as v85;

/// No-op migration. All types are JSON-compatible between v84 and v85.
/// The only changes are adding the new `AlterAddColumnV1` and
/// `AlterSourceTimestampIntervalV1` audit log event types.
pub fn upgrade(
    _snapshot: Vec<v84::StateUpdateKind>,
) -> Vec<MigrationAction<v84::StateUpdateKind, v85::StateUpdateKind>> {
    Vec::new()
}
