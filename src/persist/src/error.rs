// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence related errors.

use std::sync::Arc;
use std::{error, fmt, io, sync};

use crate::location::{ExternalError, SeqNo};

/// A persistence related error.
#[derive(Debug, Clone)]
pub enum Error {
    /// A persistence related error resulting from an io failure.
    IO(Arc<io::Error>),
    /// An operation failed because storage was out of space or quota.
    OutOfQuota(String),
    /// An unstructured persistence related error.
    String(String),
    /// There is no stream registered under the given name.
    UnknownRegistration(String),
    /// The associated write request was sequenced (given a SeqNo) and applied
    /// to the persist state machine, but that application was deterministically
    /// made into a no-op because it was contextually invalid (a write or seal
    /// at a sealed timestamp, an allow_compactions at an unsealed timestamp,
    /// etc).
    Noop(SeqNo, String),
    /// An error returned when a command is sent to a persistence runtime that
    /// was previously stopped.
    RuntimeShutdown,
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::IO(e) => fmt::Display::fmt(e, f),
            Error::OutOfQuota(e) => f.write_str(e),
            Error::String(e) => f.write_str(e),
            Error::UnknownRegistration(id) => write!(f, "unknown registration: {}", id),
            Error::Noop(_, e) => f.write_str(e),
            Error::RuntimeShutdown => f.write_str("runtime shutdown"),
        }
    }
}

// Hack so we can debug_assert_eq against Result<(), Error>.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::String(s), Error::String(o)) => s == o,
            (Error::OutOfQuota(s), Error::OutOfQuota(o)) => s == o,
            (Error::UnknownRegistration(s), Error::UnknownRegistration(o)) => s == o,
            (Error::RuntimeShutdown, Error::RuntimeShutdown) => true,
            _ => false,
        }
    }
}

impl From<ExternalError> for Error {
    fn from(e: ExternalError) -> Self {
        Error::String(e.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        // This only works on unix/macos, but I don't see a great alternative.
        if let Some(28) = e.raw_os_error() {
            return Error::OutOfQuota(e.to_string());
        }
        Error::IO(Arc::new(e))
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

impl From<arrow2::error::Error> for Error {
    fn from(e: arrow2::error::Error) -> Self {
        Error::String(e.to_string())
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(e: sync::PoisonError<T>) -> Self {
        Error::String(format!("poison: {}", e))
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::String(format!("sqlite: {}", e))
    }
}
