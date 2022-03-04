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

//! Wrappers around [`tokio::spawn`] and [`tokio::task::spawn_blocking`] that require
//! names.
//!
//! If `clippy` sent you here, replace:
//! ```ignore
//!     tokio::spawn(my_future)
//!     tokio::task::spawn_blocking(my_blocking_closure)
//! ```
//! with
//! ```ignore
//!     mz_ore::task::spawn(|| format!("taskname:{}", info), my_future)
//!     mz_ore::task::spawn_blocking(|| format!("name:{}", info), my_blocking_closure)
//! ```
//!
//! If you are using [`Runtime::spawn`][`tokio::runtime::Runtime::spawn`]
//! or [`Runtime::spawn_blocking`][`tokio::runtime::Runtime::spawn_blocking`],
//! or the similar methods on [`tokio::runtime::Handle`], import [`RuntimeExt`] instead
//! and replace `spawn` with [`RuntimeExt::spawn_named`] and `spawn_blocking` with
//! [`RuntimeExt::spawn_blocking_named`], adding naming closures like above.
//!

use std::future::Future;
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};
use tokio::task::{self, JoinHandle};

/// Spawns a task on the runtime, with name.
///
/// See [`spawn`](::tokio::spawn), and the
/// [module][`self`] docs for more info.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn spawn<Fut, Name, NameClosure>(_nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    tokio::spawn(future)
}

/// Spawns a task on the runtime, with name.
///
/// See [`spawn`](::tokio::spawn), and the
/// [module][`self`] docs for more info.
#[cfg(all(tokio_unstable, feature = "task"))]
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn spawn<Fut, Name, NameClosure>(nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    task::Builder::new().name(nc().as_ref()).spawn(future)
}

/// Spawns blocking code on the blocking threadpool, with name.
///
/// See [`spawn_blocking`](::tokio::task::spawn_blocking), and the
/// [module][`self`] docs for more info.
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

/// Spawns blocking code on the blocking threadpool, with name.
///
/// See [`spawn_blocking`](::tokio::task::spawn_blocking), and the
/// [module][`self`] docs for more info.
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

/// Provides extension methods to [`tokio`] runtime handles that
/// allow you to call into [`spawn`] and [`spawn_blocking`].
///
/// See the [module][`self`] docs for more info.
pub trait RuntimeExt {
    /// Replaces [`tokio::runtime::Runtime::spawn_blocking`].
    ///
    /// See the [module][`self`] docs for more info.
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

    /// Replaces [`tokio::runtime::Runtime::spawn`].
    ///
    /// See the [module][`self`] docs for more info.
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
