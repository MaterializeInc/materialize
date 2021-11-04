// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence related errors.

use std::ops::Range;
use std::sync::Arc;
use std::{error, fmt, io, sync};

use crate::storage::{Log, SeqNo};

/// A persistence related error.
#[derive(Debug, Clone)]
pub enum Error {
    /// A persistence related error resulting from an io failure.
    IO(Arc<io::Error>),
    /// An operation failed because storage was out of space or quota.
    OutOfQuota(String),
    /// An unstructured persistence related error.
    String(String),
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
            (Error::RuntimeShutdown, Error::RuntimeShutdown) => true,
            _ => false,
        }
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

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(e: sync::PoisonError<T>) -> Self {
        Error::String(format!("poison: {}", e))
    }
}

/// A stub implementation of [Log] that always returns errors.
///
/// This exists to let us keep the (surprisingly non-trivial) Log plumbing while
/// we don't actually use it, but without the risk of accidentally using it.
#[derive(Debug)]
pub struct ErrorLog;

impl Log for ErrorLog {
    fn write_sync(&mut self, _buf: Vec<u8>) -> Result<SeqNo, Error> {
        Err(Error::from(
            "ErrorLog method unexpectedly called, please report a bug: write_sync",
        ))
    }

    fn snapshot<F>(&self, _logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        Err(Error::from(
            "ErrorLog method unexpectedly called, please report a bug: snapshot",
        ))
    }

    fn truncate(&mut self, _upper: SeqNo) -> Result<(), Error> {
        Err(Error::from(
            "ErrorLog method unexpectedly called, please report a bug: snapshot",
        ))
    }

    fn close(&mut self) -> Result<bool, Error> {
        // This one should actually be used, so implement it. The bool result is
        // only used for logging when a Log impl was dropped without being
        // closed, so it's safe to always return false.
        Ok(false)
    }
}
