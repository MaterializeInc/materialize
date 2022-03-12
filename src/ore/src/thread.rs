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

//! Thread utilities.

use std::thread::JoinHandle;

/// Wraps a [`JoinHandle`] so that the child thread is joined when the handle is
/// dropped, rather than detached. If the child thread panics,
/// `JoinOnDropHandle` will panic when dropped.
#[derive(Debug)]
pub struct JoinOnDropHandle<T>(Option<JoinHandle<T>>);

impl<T> Drop for JoinOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.take().unwrap().join().unwrap();
    }
}

/// Wraps a [`JoinHandle`] so that the child thread is unparked (and then
/// detached as usual) when the handle is dropped.
#[derive(Debug)]
pub struct UnparkOnDropHandle<T>(JoinHandle<T>);

impl<T> Drop for UnparkOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.thread().unpark();
    }
}

/// Extension methods for [`JoinHandle`].
pub trait JoinHandleExt<T> {
    /// Converts a [`JoinHandle`] into a [`JoinOnDropHandle`].
    fn join_on_drop(self) -> JoinOnDropHandle<T>;

    /// Converts a [`JoinHandle`] into an [`UnparkOnDropHandle`].
    fn unpark_on_drop(self) -> UnparkOnDropHandle<T>;
}

impl<T> JoinHandleExt<T> for JoinHandle<T> {
    fn join_on_drop(self) -> JoinOnDropHandle<T> {
        JoinOnDropHandle(Some(self))
    }

    fn unpark_on_drop(self) -> UnparkOnDropHandle<T> {
        UnparkOnDropHandle(self)
    }
}
