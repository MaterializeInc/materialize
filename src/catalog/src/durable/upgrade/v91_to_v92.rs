// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v91 as v91;
use crate::durable::upgrade::objects_v92 as v92;

/// No-op migration. v91->v92 only adds a `MetricSink` variant to four existing
/// enums: `CatalogItemType`, `ObjectType`, `CommentObject`, and the audit log's
/// `ObjectType`. Nothing already stored can be using a variant that didn't
/// exist, so every v91 record reads back as valid v92.
///
/// Metric sinks get no durable record of their own. They are ordinary `Item`s,
/// and `durable::objects::item_type` works out the type from `create_sql`.
pub fn upgrade(
    _snapshot: Vec<v91::StateUpdateKind>,
) -> Vec<MigrationAction<v91::StateUpdateKind, v92::StateUpdateKind>> {
    Vec::new()
}
