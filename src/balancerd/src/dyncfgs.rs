// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the balancer.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

// The defaults here must be set to an appropriate value in case LaunchDarkly is down because we
// continue startup even in that case.

/// Duration to wait after SIGTERM for outstanding connections to complete.
pub const SIGTERM_WAIT: Config<Duration> = Config::new(
    "balancerd_sigterm_wait",
    Duration::from_secs(60 * 10),
    "Duration to wait after SIGTERM for outstanding connections to complete.",
);

/// Whether to log the status of pgwire connections.
pub const LOG_PGWIRE_CONNECTION_STATUS: Config<bool> = Config::new(
    "balancerd_log_pgwire_connection_status",
    false,
    "Whether to log the status of pgwire connections.",
);

/// Adds the full set of all balancer `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&SIGTERM_WAIT)
        .add(&LOG_PGWIRE_CONNECTION_STATUS)
}
