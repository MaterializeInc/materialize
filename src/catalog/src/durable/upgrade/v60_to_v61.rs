// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v60 as v60, objects_v61 as v61};

/// No-op migration. In v61, we added a new optional field to `ClusterConfig`.
pub fn upgrade(
    _snapshot: Vec<v60::StateUpdateKind>,
) -> Vec<MigrationAction<v60::StateUpdateKind, v61::StateUpdateKind>> {
    Vec::new()
}
