// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v87 as v87;
use crate::durable::upgrade::objects_v88 as v88;

/// No-op migration. Every v87->v88 change is additive and confined to the
/// append-only audit log: new `CreateOrDropClusterReplicaReasonV1` reasons
/// (`Reconfiguration`, `HydrationBurst`, `Retired`) and new
/// `AlterClusterReconfigurationV1` / `ClusterHydrationBurstV1` event details.
/// No existing record changes shape, so a v87-serialized record is already
/// valid v88 and nothing is rewritten.
pub fn upgrade(
    _snapshot: Vec<v87::StateUpdateKind>,
) -> Vec<MigrationAction<v87::StateUpdateKind, v88::StateUpdateKind>> {
    Vec::new()
}
