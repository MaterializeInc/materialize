// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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
