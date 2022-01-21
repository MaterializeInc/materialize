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

use tokio::task::{self, JoinHandle};

/// Spawns a task on the executor.
///
/// See [`spawn`](::tokio::spawn) for
/// more details.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
pub fn spawn<Fut>(_name: &str, future: Fut) -> JoinHandle<Fut::Output>
where
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
pub fn spawn<Fut>(name: &str, future: Fut) -> JoinHandle<Fut::Output>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    task::Builder::new().name(name).spawn(future)
}

/// Spawns blocking code on the blocking threadpool.
///
/// See [`spawn_blocking`](::tokio::task::spawn_blocking)
/// for more details.
#[cfg(not(all(tokio_unstable, feature = "task")))]
#[track_caller]
pub fn spawn_blocking<Function, Output>(_name: &str, function: Function) -> JoinHandle<Output>
where
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
pub fn spawn_blocking<Function, Output>(name: &str, function: Function) -> JoinHandle<Output>
where
    Function: FnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    task::Builder::new().name(name).spawn_blocking(function)
}
