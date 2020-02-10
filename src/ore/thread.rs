// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

/// Extension methods for [`JoinHandle`].
pub trait JoinHandleExt<T> {
    /// Converts a [`JoinHandle`] into a [`JoinOnDropHandle`].
    fn join_on_drop(self) -> JoinOnDropHandle<T>;
}

impl<T> JoinHandleExt<T> for JoinHandle<T> {
    fn join_on_drop(self) -> JoinOnDropHandle<T> {
        JoinOnDropHandle(Some(self))
    }
}
