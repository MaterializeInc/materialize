// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v82 as v82;
use crate::durable::upgrade::objects_v83 as v83;

/// No-op migration. All types are JSON-compatible between v82 and v83.
/// The only changes are adding the new `AlterAddColumnV1` and
/// `AlterSourceTimestampIntervalV1` audit log event types.
pub fn upgrade(
    _snapshot: Vec<v82::StateUpdateKind>,
) -> Vec<MigrationAction<v82::StateUpdateKind, v83::StateUpdateKind>> {
    Vec::new()
}
