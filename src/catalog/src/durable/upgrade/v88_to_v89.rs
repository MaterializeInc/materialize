// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v88 as v88;
use crate::durable::upgrade::objects_v89 as v89;

/// No-op migration. The v88->v89 change is additive and confined to the
/// append-only audit log: a new `MetricSink` `ObjectType` discriminant and a
/// new `CreateMetricSinkV1` event detail. No existing durable record
/// references either, so a v88-serialized record is already valid v89 and
/// nothing is rewritten.
pub fn upgrade(
    _snapshot: Vec<v88::StateUpdateKind>,
) -> Vec<MigrationAction<v88::StateUpdateKind, v89::StateUpdateKind>> {
    Vec::new()
}
