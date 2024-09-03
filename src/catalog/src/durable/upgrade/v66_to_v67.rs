// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v66 as v66, objects_v67 as v67};

/// The key used within the "config" collection stores the deploy generation.
pub(crate) const DEPLOY_GENERATION: &str = "deploy_generation";

/// In v67, we removed the Epoch variant.
pub fn upgrade(
    snapshot: Vec<v66::StateUpdateKind>,
) -> Vec<MigrationAction<v66::StateUpdateKind, v67::StateUpdateKind>> {
    let epochs: Vec<_> = snapshot
        .iter()
        .filter_map(|update| match &update.kind {
            Some(v66::state_update_kind::Kind::Epoch(epoch)) => Some(epoch.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        epochs,
        Vec::new(),
        "epochs should have all been removed while fencing"
    );

    let deploy_generations: Vec<v66::state_update_kind::Config> = snapshot
        .iter()
        .filter_map(|update| match &update.kind {
            Some(v66::state_update_kind::Kind::Config(config))
                if config.key.as_ref().expect("missing key").key == DEPLOY_GENERATION =>
            {
                Some(config.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        deploy_generations,
        Vec::new(),
        "deploy generations should have all been removed while fencing"
    );

    Vec::new()
}
