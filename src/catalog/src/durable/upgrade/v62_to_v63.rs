// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v62 as v62, objects_v63 as v63};

/// In v63, we added the SourceReferences object kind.
pub fn upgrade(
    _snapshot: Vec<v62::StateUpdateKind>,
) -> Vec<MigrationAction<v62::StateUpdateKind, v63::StateUpdateKind>> {
    vec![]
}
