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

use std::future::Future;
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};
use tokio::task::{self, JoinHandle};

/// Spawns a task on the executor.
///
/// See [`spawn`](::tokio::spawn) for
/// more details.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(_nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    tokio::spawn(future)
}

/// Spawns a task on the executor.
///
/// See [`spawn`](tokio::spawn) for
/// more details.
#[cfg(all(tokio_unstable, feature = "task"))]
#[track_caller]
pub fn spawn<Fut, Name, NameClosure>(nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    task::Builder::new().name(nc().as_ref()).spawn(future)
}

/// Spawns blocking code on the blocking threadpool.
///
/// See [`spawn_blocking`](::tokio::task::spawn_blocking)
/// for more details.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
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

/// Spawns blocking code on the blocking threadpool.
///
/// See [`spawn_blocking`](::tokio::task::spawn_blocking)
/// for more details.
#[cfg(all(tokio_unstable, feature = "task"))]
#[track_caller]
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

pub trait RuntimeExt {
    #[track_caller]
    fn spawn_blocking<Function, Output, Name, NameClosure>(
        &self,
        nc: NameClosure,
        function: Function,
    ) -> JoinHandle<Output>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name,
        Function: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static;

    #[track_caller]
    fn spawn<Fut, Name, NameClosure>(
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
    fn spawn_blocking<Function, Output, Name, NameClosure>(
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

    fn spawn<Fut, Name, NameClosure>(&self, nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
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
    fn spawn_blocking<Function, Output, Name, NameClosure>(
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

    fn spawn<Fut, Name, NameClosure>(&self, nc: NameClosure, future: Fut) -> JoinHandle<Fut::Output>
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
