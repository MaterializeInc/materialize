// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Result utilities.

/// Extension methods for [`std::result::Result`].
pub trait ResultExt<T, E> {
    /// Applies [`Into::into`] to a contained [`Err`] value, leaving an [`Ok`]
    /// value untouched.
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>,
    {
        self.map_err(|e| e.into())
    }
}
