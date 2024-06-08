// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the adapter layer.

use mz_dyncfg::{Config, ConfigSet};

pub const ALLOW_USER_SESSIONS: Config<bool> = Config::new(
    "allow_user_sessions",
    true,
    "Whether to allow user roles to create new sessions. When false, only system roles will be permitted to create new sessions.",
);

/// Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.
pub const ENABLE_STATEMENT_LIFECYCLE_LOGGING: Config<bool> = Config::new(
    "enable_statement_lifecycle_logging",
    false,
    "Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
}
