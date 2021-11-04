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

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Once;
use std::thread;
use std::time::Duration;

use anyhow::bail;
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
        let filter = EnvFilter::try_from_env("MZ_LOG_FILTER")
            .or_else(|_| EnvFilter::try_new(level))
            .unwrap();
        FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_test_writer()
            .init();
    });
}

/// Runs a function with a timeout.
///
/// The provided closure is invoked on a thread. If the thread completes
/// normally within the provided `duration`, its result is returned. If the
/// thread panics within the provided `duration`, the panic is propagated to the
/// thread calling `timeout`. Otherwise, a timeout error is returned.
///
/// Note that if the invoked function does not complete in the timeout, it is
/// not killed; it is left to wind down normally. Therefore this function is
/// only appropriate in tests, where the resource leak doesn't matter.
pub fn timeout<F, T>(duration: Duration, f: F) -> Result<T, anyhow::Error>
where
    F: FnOnce() -> Result<T, anyhow::Error> + Send + 'static,
    T: Send + 'static,
{
    // Use the drop of `tx` to indicate that the thread is finished. This
    // ensures that `tx` is dropped even if `f` panics. No actual value is ever
    // sent on `tx`.
    let (tx, rx) = mpsc::channel();
    let thread = thread::spawn(|| {
        let _tx = tx;
        f()
    });
    match rx.recv_timeout(duration) {
        Ok(()) => unreachable!(),
        Err(RecvTimeoutError::Disconnected) => thread.join().unwrap(),
        Err(RecvTimeoutError::Timeout) => bail!("thread timed out"),
    }
}
