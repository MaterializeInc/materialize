// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v85 as v85;
use crate::durable::upgrade::objects_v86 as v86;

/// No-op migration: v86 only adds new `ObjectType::Api` and `ObjectType::Metric`
/// enum variants. No existing rows reference them.
pub fn upgrade(
    _snapshot: Vec<v85::StateUpdateKind>,
) -> Vec<MigrationAction<v85::StateUpdateKind, v86::StateUpdateKind>> {
    Vec::new()
}
