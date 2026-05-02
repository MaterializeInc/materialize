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
use std::sync::atomic::{AtomicUsize, Ordering};

use mz_ore::metrics::{MetricsRegistry, register_runtime_metrics};
use mz_ore::task::{JoinHandle, RuntimeExt};
use tokio::runtime::{Builder, Runtime};

/// A reasonable number of threads to use in tests: enough to reproduce nontrivial
/// orderings if necessary and avoid blocking, but not too many.
// This was done as a workaround for https://sourceware.org/bugzilla/show_bug.cgi?id=19951
// in tests, but seems useful in general.
pub const TEST_THREADS: usize = 4;

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
///
/// Note: Even though the work done by this runtime might be "blocking" or
/// CPU bound we should not use the [`tokio::task::spawn_blocking`] API.
/// There can be issues during shutdown if tasks are currently running on the
/// blocking thread pool [1], and the blocking thread pool generally creates
/// many more threads than are physically available. This can pin CPU usage
/// to 100% starving other important threads like the Coordinator.
///
/// [1]: <https://github.com/MaterializeInc/materialize/pull/13955>
#[derive(Debug)]
pub struct IsolatedRuntime {
    inner: Option<Runtime>,
}

impl IsolatedRuntime {
    /// Creates a new isolated runtime.
    pub fn new(metrics: &MetricsRegistry, worker_threads: Option<usize>) -> IsolatedRuntime {
        let mut runtime = Builder::new_multi_thread();
        if let Some(worker_threads) = worker_threads {
            runtime.worker_threads(worker_threads);
        }
        let runtime = runtime
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                // This will wrap around eventually, which is not ideal, but it's important that
                // it stays small to fit within OS limits.
                format!("persist:{:04x}", id % 0x10000)
            })
            .enable_all()
            .build()
            .expect("known to be valid");
        register_runtime_metrics("persist", runtime.metrics(), metrics);
        IsolatedRuntime {
            inner: Some(runtime),
        }
    }

    /// Create an isolated runtime with appropriate values for tests.
    pub fn new_for_tests() -> Self {
        IsolatedRuntime::new(&MetricsRegistry::new(), Some(TEST_THREADS))
    }

    #[cfg(feature = "turmoil")]
    /// Create a no-op shim that spawns tasks on the current tokio runtime.
    ///
    /// This is useful for simulation tests where we don't want to spawn additional threads and/or
    /// tokio runtimes.
    pub fn new_disabled() -> Self {
        IsolatedRuntime { inner: None }
    }

    /// Spawns a task onto this runtime.
    ///
    /// Note: We purposefully do not use the [`tokio::task::spawn_blocking`] API here, see the doc
    /// comment on [`IsolatedRuntime`] for explanation.
    pub fn spawn_named<N, S, F>(&self, name: N, fut: F) -> JoinHandle<F::Output>
    where
        S: AsRef<str>,
        N: FnOnce() -> S,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if let Some(runtime) = &self.inner {
            runtime.spawn_named(name, fut)
        } else {
            mz_ore::task::spawn(name, fut)
        }
    }
}

impl Drop for IsolatedRuntime {
    fn drop(&mut self) {
        // We don't need to worry about `shutdown_background` leaking
        // blocking tasks (i.e., tasks spawned with `spawn_blocking`) because
        // the `IsolatedRuntime` wrapper prevents access to `spawn_blocking`.
        if let Some(runtime) = self.inner.take() {
            runtime.shutdown_background();
        }
    }
}
