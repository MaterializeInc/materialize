// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v64 as v64, objects_v65 as v65};

/// In v65, we add cluster_id to CREATE events in the audit log.
pub fn upgrade(
    _snapshot: Vec<v64::StateUpdateKind>,
) -> Vec<MigrationAction<v64::StateUpdateKind, v65::StateUpdateKind>> {
    Vec::new()
}
