// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

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
