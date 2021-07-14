// Copyright Materialize, Inc. and contributors. All rights reserved.
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
    /// An error returned when a command is sent to a persistence runtime that
    /// was previously stopped.
    RuntimeShutdown,
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::IO(e) => fmt::Display::fmt(e, f),
            Error::String(e) => f.write_str(e),
            Error::RuntimeShutdown => f.write_str("runtime shutdown"),
        }
    }
}

// Hack so we can debug_assert_eq against Result<(), Error>.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        if let Error::String(s) = self {
            if let Error::String(o) = other {
                return s == o;
            }
        }
        return false;
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

impl<'a> From<&'a str> for Error {
    fn from(e: &'a str) -> Self {
        Error::String(e.into())
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(e: sync::PoisonError<T>) -> Self {
        Error::String(format!("poison: {}", e))
    }
}
