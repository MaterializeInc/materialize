// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the storage layer. Despite their name, these can be used
//! "statically" during rendering, or dynamically within timely operators.

use mz_dyncfg::{Config, ConfigSet};

/// Whether rendering should use `mz_join_core` rather than DD's `JoinCore::join_core`.
/// Configuration for basic hydration backpressure.
pub const DELAY_SOURCES_PAST_REHYDRATION: Config<bool> = Config::new(
    "storage_dataflow_delay_sources_past_rehydration",
    // This was original `false`, but it is not enabled everywhere.
    true,
    "Whether or not to delay sources producing values in some scenarios \
        (namely, upsert) till after rehydration is finished",
);

/// When enabled, force-downgrade the controller's since handle on the shard
/// during shard finalization.
pub const STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION: Config<bool> = Config::new(
    "storage_downgrade_since_during_finalization",
    // This was original `false`, but it is not enabled everywhere.
    true,
    "When enabled, force-downgrade the controller's since handle on the shard\
    during shard finalization",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&DELAY_SOURCES_PAST_REHYDRATION)
        .add(&STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION)
}
