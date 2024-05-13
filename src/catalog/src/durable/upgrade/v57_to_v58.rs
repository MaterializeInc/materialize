// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v57 as v57, objects_v58 as v58};

/// No-op migration. In v58, we add the `scheduling_decision_reasons` field
/// to `CreateClusterReplica` and `DropClusterReplica` audit log details, but with creating a V2 of
/// these structs.
pub fn upgrade(
    _snapshot: Vec<v57::StateUpdateKind>,
) -> Vec<MigrationAction<v57::StateUpdateKind, v58::StateUpdateKind>> {
    Vec::new()
}
