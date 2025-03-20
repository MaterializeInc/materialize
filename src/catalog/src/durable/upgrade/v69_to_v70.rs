// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v69 as v69, objects_v70 as v70};

/// In v70, we updated the audit log entries when creating/dropping replicas due to refresh
/// schedules.
pub fn upgrade(
    _snapshot: Vec<v69::StateUpdateKind>,
) -> Vec<MigrationAction<v69::StateUpdateKind, v70::StateUpdateKind>> {
    Vec::new()
}
