// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_build_info::BuildInfo;
use mz_ore::metrics::MetricsRegistry;

use crate::catalog::storage;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a, S> {
    /// The connection to the stash.
    pub storage: storage::Connection<S>,
    /// Whether to enable unsafe mode.
    pub unsafe_mode: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
}
