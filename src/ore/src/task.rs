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

//! Tokio task utilities.
//!
//! ## Named task spawning
//!
//! The [`spawn`] and [`spawn_blocking`] methods are wrappers around
//! [`tokio::task::spawn`] and [`tokio::task::spawn_blocking`] that attach a
//! name the spawned task.
//!
//! If Clippy sent you here, replace:
//!
//! ```ignore
//! tokio::task::spawn(my_future)
//! tokio::task::spawn_blocking(my_blocking_closure)
//! ```
//!
//! with:
//!
//! ```ignore
//! mz_ore::task::spawn(|| format!("taskname:{}", info), my_future)
//! mz_ore::task::spawn_blocking(|| format!("name:{}", info), my_blocking_closure)
//! ```
//!
//! If you are using methods of the same names on a [`Runtime`] or [`Handle`],
//! import [`RuntimeExt`] and replace `spawn` with [`RuntimeExt::spawn_named`]
//! and `spawn_blocking` with [`RuntimeExt::spawn_blocking_named`], adding
//! naming closures like above.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;
use tokio::runtime::{Handle, Runtime};
use tokio::task::{self, JoinHandle as TokioJoinHandle};

/// Wraps a [`JoinHandle`] to abort the underlying task when dropped.
#[derive(Debug)]
pub struct AbortOnDropHandle<T>(JoinHandle<T>);

impl<T> AbortOnDropHandle<T> {
    /// Checks if the task associated with this [`AbortOnDropHandle`] has finished.a
    pub fn is_finished(&self) -> bool {
        self.0.inner.is_finished()
    }

    // Note: adding an `abort(&self)` method here is incorrect; see the comment in JoinHandle::poll.
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.inner.abort();
    }
}

impl<T> Future for AbortOnDropHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Wraps a tokio `JoinHandle` that has never been cancelled.
/// This allows it to have an infallible implementation of [Future],
/// and provides some exclusive (i.e. they take `self` ownership)
/// operations:
///
/// - `abort_on_drop`: create an `AbortOnDropHandle` that will automatically abort the task
/// when the handle is dropped.
/// - `JoinHandleExt::abort_and_wait`: abort the task and wait for it to be finished.
/// - `into_tokio_handle`: turn it into an ordinary tokio `JoinHandle`.
#[derive(Debug)]
pub struct JoinHandle<T> {
    inner: TokioJoinHandle<T>,
    runtime_shutting_down: bool,
}

impl<T> JoinHandle<T> {
    /// Wrap a tokio join handle. This is intentionally private, so we can statically guarantee
    /// that the inner join handle has not been aborted.
    fn new(handle: TokioJoinHandle<T>) -> Self {
        Self {
            inner: handle,
            runtime_shutting_down: false,
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.runtime_shutting_down {
            return Poll::Pending;
        }
        match self.inner.poll_unpin(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(err)) => {
                match err.try_into_panic() {
                    Ok(panic) => std::panic::resume_unwind(panic),
                    Err(err) => {
                        assert!(
                            err.is_cancelled(),
                            "join errors are either cancellations or panics"
                        );
                        // Because `JoinHandle` and `AbortOnDropHandle` don't
                        // offer an `abort` method, this can only happen if the runtime is
                        // shutting down, which means this `pending` won't cause a deadlock
                        // because Tokio drops all outstanding futures on shutdown.
                        // (In multi-threaded runtimes, not all threads drop futures simultaneously,
                        // so it is possible for a future on one thread to observe the drop of a future
                        // on another thread, before it itself is dropped.)
                        self.runtime_shutting_down = true;
                        Poll::Pending
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> JoinHandle<T> {
    /// Create an [`AbortOnDropHandle`] from this [`JoinHandle`].
    pub fn abort_on_drop(self) -> AbortOnDropHandle<T> {
        AbortOnDropHandle(self)
    }

    /// Checks if the task associated with this [`JoinHandle`] has finished.
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// Aborts the task, then waits for it to complete.
    pub async fn abort_and_wait(self) {
        self.inner.abort();
        let _ = self.inner.await;
    }

    /// Unwrap this handle into a standard [tokio::task::JoinHandle].
    pub fn into_tokio_handle(self) -> TokioJoinHandle<T> {
        self.inner
    }

    // Note: adding an `abort(&self)` method here is incorrect; see the comment in JoinHandle::poll.
}

/// Spawns a new asynchronous task with a name.
///
/// See [`tokio::task::spawn`] and the [module][`self`] docs for more
/// information.
#[cfg(not(tokio_unstable))]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(_nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[allow(clippy::disallowed_methods)]
    JoinHandle::new(tokio::spawn(future))
}

/// Spawns a new asynchronous task with a name.
///
/// See [`tokio::task::spawn`] and the [module][`self`] docs for more
/// information.
#[cfg(tokio_unstable)]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[allow(clippy::disallowed_methods)]
    JoinHandle::new(
        task::Builder::new()
            .name(&format!("{}:{}", Handle::current().id(), nc().as_ref()))
            .spawn(future)
            .expect("task spawning cannot fail"),
    )
}

/// Runs the provided closure with a name on a thread where blocking is
/// acceptable.
///
/// See [`tokio::task::spawn_blocking`] and the [module][`self`] docs for more
/// information.
#[cfg(not(tokio_unstable))]
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn spawn_blocking<Function, Output, Name, NameClosure>(
    _nc: NameClosure,
    function: Function,
) -> JoinHandle<Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Function: FnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    JoinHandle::new(task::spawn_blocking(function))
}

/// Runs the provided closure with a name on a thread where blocking is
/// acceptable.
///
/// See [`tokio::task::spawn_blocking`] and the [module][`self`] docs for more
/// information.
#[cfg(tokio_unstable)]
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn spawn_blocking<Function, Output, Name, NameClosure>(
    nc: NameClosure,
    function: Function,
) -> JoinHandle<Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Function: FnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    JoinHandle::new(
        task::Builder::new()
            .name(&format!("{}:{}", Handle::current().id(), nc().as_ref()))
            .spawn_blocking(function)
            .expect("task spawning cannot fail"),
    )
}

/// Extension methods for [`Runtime`] and [`Handle`].
///
/// See the [module][`self`] docs for more information.
pub trait RuntimeExt {
    /// Runs the provided closure with a name on a thread where blocking is
    /// acceptable.
    ///
    /// See [`tokio::task::spawn_blocking`] and the [module][`self`] docs for more
    /// information.
    #[track_caller]
    fn spawn_blocking_named<Function, Output, Name, NameClosure>(
        &self,
        nc: NameClosure,
        function: Function,
    ) -> JoinHandle<Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Function: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static;

    /// Spawns a new asynchronous task with a name.
    ///
    /// See [`tokio::task::spawn`] and the [module][`self`] docs for more
    /// information.
    #[track_caller]
    fn spawn_named<Fut, Name, NameClosure>(
        &self,
        _nc: NameClosure,
        future: Fut,
    ) -> JoinHandle<Fut::Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static;
}

impl RuntimeExt for &Runtime {
    fn spawn_blocking_named<Function, Output, Name, NameClosure>(
        &self,
        nc: NameClosure,
        function: Function,
    ) -> JoinHandle<Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Function: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        let _g = self.enter();
        spawn_blocking(nc, function)
    }

    fn spawn_named<Fut, Name, NameClosure>(
        &self,
        nc: NameClosure,
        future: Fut,
    ) -> JoinHandle<Fut::Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let _g = self.enter();
        spawn(nc, future)
    }
}

impl RuntimeExt for Arc<Runtime> {
    fn spawn_blocking_named<Function, Output, Name, NameClosure>(
        &self,
        nc: NameClosure,
        function: Function,
    ) -> JoinHandle<Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Function: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        (&**self).spawn_blocking_named(nc, function)
    }

    fn spawn_named<Fut, Name, NameClosure>(
        &self,
        nc: NameClosure,
        future: Fut,
    ) -> JoinHandle<Fut::Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        (&**self).spawn_named(nc, future)
    }
}

impl RuntimeExt for Handle {
    fn spawn_blocking_named<Function, Output, Name, NameClosure>(
        &self,
        nc: NameClosure,
        function: Function,
    ) -> JoinHandle<Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Function: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        let _g = self.enter();
        spawn_blocking(nc, function)
    }

    fn spawn_named<Fut, Name, NameClosure>(
        &self,
        nc: NameClosure,
        future: Fut,
    ) -> JoinHandle<Fut::Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let _g = self.enter();
        spawn(nc, future)
    }
}

/// Extension methods for [`tokio::task::JoinSet`].
///
/// See the [module][`self`] docs for more information.
pub trait JoinSetExt<T> {
    /// Spawns a new asynchronous task with a name.
    ///
    /// See [`tokio::task::spawn`] and the [module][`self`] docs for more
    /// information.
    #[track_caller]
    fn spawn_named<Fut, Name, NameClosure>(
        &mut self,
        nc: NameClosure,
        future: Fut,
    ) -> tokio::task::AbortHandle
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

impl<T> JoinSetExt<T> for tokio::task::JoinSet<T> {
    // Allow unused variables until everything in ci uses `tokio_unstable`.
    #[allow(unused_variables)]
    fn spawn_named<Fut, Name, NameClosure>(
        &mut self,
        nc: NameClosure,
        future: Fut,
    ) -> tokio::task::AbortHandle
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(tokio_unstable)]
        #[allow(clippy::disallowed_methods)]
        {
            self.build_task()
                .name(&format!("{}:{}", Handle::current().id(), nc().as_ref()))
                .spawn(future)
                .expect("task spawning cannot fail")
        }
        #[cfg(not(tokio_unstable))]
        #[allow(clippy::disallowed_methods)]
        {
            self.spawn(future)
        }
    }
}
