// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence related errors.

use std::{error, fmt, io, sync};

/// A persistence related error.
#[derive(Debug)]
pub enum Error {
    /// A persistence related error resulting from an io failure.
    IO(io::Error),
    /// An unstructured persistence related error.
    String(String),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IO(e)
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Error::String(e)
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(e: sync::PoisonError<T>) -> Self {
        Error::String(format!("poison: {}", e))
    }
}
