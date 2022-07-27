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
use std::time::Duration;
use std::{env, thread};

use anyhow::bail;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;

use crate::tracing::{StderrLogConfig, TracingConfig, TracingHandle};

static TRACING_HANDLE: OnceCell<TracingHandle> = OnceCell::const_new();

/// Initialize [`tracing`](crate::tracing) in tests.
///
/// It is safe to call `init_tracing` multiple times. Since `cargo test` does
/// not run tests in any particular order, each must call `init_tracing`.
pub async fn init_tracing() -> TracingHandle {
    init_tracing_with_filter("info").await
}

/// Like [`init_tracing`], but with the specified filter level.
pub async fn init_tracing_with_filter(filter: &str) -> TracingHandle {
    TRACING_HANDLE
        .get_or_init(|| async {
            crate::tracing::configure(
                "cargo-test",
                TracingConfig {
                    stderr_log: StderrLogConfig {
                        prefix: Some("cargo-test".into()),
                        filter: env::var("MZ_LOG_FILTER")
                            .unwrap_or_else(|_| filter.into())
                            .parse()
                            .unwrap_or_else(|e| panic!("unable to parse MZ_LOG_FILTER: {e}")),
                    },
                    opentelemetry: None,
                },
            )
            .await
            .expect("failed to configure tracing")
        })
        .await
        .clone()
}

/// Like [`init_tracing`] but for use in non-async contexts.
///
/// Prefer [`init_tracing`] whenever possible.
pub fn init_tracing_sync() -> TracingHandle {
    // NOTE(benesch): this is only possible because the tracing configuration we
    // use in tests does not enable OpenTelemetry and so does not actually make
    // use of the runtime. If it did, we'd need the test to keep the runtime
    // handle alive for the duration of the test--at which point it would
    // probably be better to just convert all of our tests that use logging to
    // `async` tests (i.e., via `tokio::test`).
    Runtime::new().unwrap().block_on(init_tracing())
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
