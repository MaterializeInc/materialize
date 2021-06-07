// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Test utilities.

use std::sync::Once;

use tracing_subscriber::{EnvFilter, FmtSubscriber};

static LOG_INIT: Once = Once::new();

/// Initialize global logger, using the [`tracing_subscriber`] crate, with
/// sensible defaults.
///
/// It is safe to call `init_logging` multiple times. Since `cargo test` does
/// not run tests in any particular order, each must call `init_logging`.
pub fn init_logging() {
    init_logging_default("info");
}

/// Initialize global logger, using the [`tracing_subscriber`] crate.
///
/// The default log level will be set to the value passed in.
///
/// It is safe to call `init_logging_level` multiple times. Since `cargo test` does
/// not run tests in any particular order, each must call `init_logging`.
pub fn init_logging_default(level: &str) {
    LOG_INIT.call_once(|| {
        let filter = EnvFilter::try_from_env("MZ_LOG")
            .or_else(|_| EnvFilter::try_new(level))
            .unwrap();
        FmtSubscriber::builder().with_env_filter(filter).init();
    });
}
