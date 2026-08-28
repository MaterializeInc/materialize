// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v92 as v92;
use crate::durable::upgrade::objects_v93 as v93;

/// No-op migration. v92->v93 only adds a `ForeignKey` variant to four existing
/// enums: `CatalogItemType`, `ObjectType`, `CommentObject`, and the audit log's
/// `ObjectType`. Nothing already stored can be using a variant that didn't
/// exist, so every v92 record reads back as valid v93.
///
/// Foreign keys get no durable record of their own. They are ordinary `Item`s,
/// and `durable::objects::item_type` works out the type from `create_sql`.
pub fn upgrade(
    _snapshot: Vec<v92::StateUpdateKind>,
) -> Vec<MigrationAction<v92::StateUpdateKind, v93::StateUpdateKind>> {
    Vec::new()
}
