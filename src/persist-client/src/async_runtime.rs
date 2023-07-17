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

use mz_ore::task::RuntimeExt;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

/// An isolated runtime for asynchronous tasks, particularly work
/// that may be CPU intensive such as encoding/decoding and shard
/// maintenance.
///
/// Using a separate runtime allows Persist to isolate its expensive
/// workloads on its own OS threads as an insurance policy against
/// tasks that erroneously fail to yield for a long time. By using
/// separate OS threads, the scheduler is able to context switch
/// out of any problematic tasks, preserving liveness for the rest
/// of the process.
#[derive(Debug)]
pub struct IsolatedRuntime {
    inner: Option<Runtime>,
}

impl IsolatedRuntime {
    /// Creates a new isolated runtime.
    pub fn new() -> IsolatedRuntime {
        // TODO: choose a more principled `worker_limit`. Right now we use the
        // Tokio default, which is presently the number of cores on the machine.
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("known to be valid");
        IsolatedRuntime {
            inner: Some(runtime),
        }
    }

    /// Spawns a task onto this runtime.
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

impl Drop for IsolatedRuntime {
    fn drop(&mut self) {
        // We don't need to worry about `shutdown_background` leaking
        // blocking tasks (i.e., tasks spawned with `spawn_blocking`) because
        // the `IsolatedRuntime` wrapper prevents access to `spawn_blocking`.
        self.inner
            .take()
            .expect("cannot drop twice")
            .shutdown_background()
    }
}
