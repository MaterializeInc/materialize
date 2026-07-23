// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v90 as v90;
use crate::durable::upgrade::objects_v91 as v91;

/// Adds the `CatalogItemType::MetricSink` durable enum variant. The change is purely additive:
/// no existing persisted `object_type` ever holds the new value, so no rows are rewritten.
pub fn upgrade(
    _snapshot: Vec<v90::StateUpdateKind>,
) -> Vec<MigrationAction<v90::StateUpdateKind, v91::StateUpdateKind>> {
    Vec::new()
}
