// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Async runtime extensions.

use std::future::Future;

use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

use mz_ore::task::RuntimeExt;

/// A runtime for running CPU-heavy asynchronous tasks.
#[derive(Debug)]
pub struct CpuHeavyRuntime {
    inner: Option<Runtime>,
}

impl CpuHeavyRuntime {
    /// Creates a new CPU heavy runtime.
    pub fn new() -> CpuHeavyRuntime {
        // TODO: choose a more principled `worker_limit`. Right now we use the
        // Tokio default, which is presently the number of cores on the machine.
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("known to be valid");
        CpuHeavyRuntime {
            inner: Some(runtime),
        }
    }

    /// Spawns a CPU-heavy task onto this runtime.
    pub fn spawn_named<N, S, F>(&self, name: N, fut: F) -> JoinHandle<F::Output>
    where
        S: AsRef<str>,
        N: FnOnce() -> S,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner
            .as_ref()
            .expect("exists until drop")
            .spawn_named(name, fut)
    }
}

impl Drop for CpuHeavyRuntime {
    fn drop(&mut self) {
        // We don't need to worry about `shutdown_background` leaking
        // blocking tasks (i.e., tasks spawned with `spawn_blocking`) because
        // the `CpuHeavyRuntime` wrapper prevents access to `spawn_blocking`.
        self.inner
            .take()
            .expect("cannot drop twice")
            .shutdown_background()
    }
}
