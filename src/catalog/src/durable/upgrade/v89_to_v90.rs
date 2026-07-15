// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v89 as v89;
use crate::durable::upgrade::objects_v90 as v90;

/// No-op migration: v90 only adds new `ObjectType::Api` and `ObjectType::Metric`
/// enum variants. No existing rows reference them.
pub fn upgrade(
    _snapshot: Vec<v89::StateUpdateKind>,
) -> Vec<MigrationAction<v89::StateUpdateKind, v90::StateUpdateKind>> {
    Vec::new()
}
