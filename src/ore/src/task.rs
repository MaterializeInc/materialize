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
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};
use tokio::task::{self, JoinHandle};

/// Wraps a [`JoinHandle`] to abort the underlying task when dropped.
#[derive(Debug)]
pub struct AbortOnDropHandle<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Extension methods for [`JoinHandle`].
pub trait JoinHandleExt<T> {
    /// Converts a [`JoinHandle`] into a [`AbortOnDropHandle`].
    fn abort_on_drop(self) -> AbortOnDropHandle<T>;
}

impl<T> JoinHandleExt<T> for JoinHandle<T> {
    fn abort_on_drop(self) -> AbortOnDropHandle<T> {
        AbortOnDropHandle(self)
    }
}

/// Spawns a new asynchronous task with a name.
///
/// See [`tokio::task::spawn`] and the [module][`self`] docs for more
/// information.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(_nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[allow(clippy::disallowed_methods)]
    tokio::spawn(future)
}

/// Spawns a new asynchronous task with a name.
///
/// See [`tokio::task::spawn`] and the [module][`self`] docs for more
/// information.
#[cfg(all(tokio_unstable, feature = "task"))]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[allow(clippy::disallowed_methods)]
    task::Builder::new().name(nc().as_ref()).spawn(future)
}

/// Runs the provided closure with a name on a thread where blocking is
/// acceptable.
///
/// See [`tokio::task::spawn_blocking`] and the [module][`self`] docs for more
/// information.
#[cfg(not(all(tokio_unstable, feature = "task")))]
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
    task::spawn_blocking(function)
}

/// Runs the provided closure with a name on a thread where blocking is
/// acceptable.
///
/// See [`tokio::task::spawn_blocking`] and the [module][`self`] docs for more
/// information.
#[cfg(all(tokio_unstable, feature = "task"))]
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
    task::Builder::new()
        .name(nc().as_ref())
        .spawn_blocking(function)
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
