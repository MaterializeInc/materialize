// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the `mz-stash*` crates

use std::error::Error;
use std::fmt::{self, Debug};

use mz_proto::TryFromProtoError;
use tokio_postgres::error::SqlState;

pub mod metrics;

/// An error that can occur while interacting with a `Stash`.
///
/// Stash errors are deliberately opaque. They generally indicate unrecoverable
/// conditions, like running out of disk space.
#[derive(Debug)]
pub struct StashError {
    /// Not a public API, only exposed for mz-stash.
    pub inner: InternalStashError,
}

impl StashError {
    /// Reports whether the error is unrecoverable (retrying will never succeed,
    /// or a retry is not safe due to an indeterminate state).
    pub fn is_unrecoverable(&self) -> bool {
        match &self.inner {
            InternalStashError::Fence(_) | InternalStashError::StashNotWritable(_) => true,
            _ => false,
        }
    }

    /// Reports whether the error can be recovered if we opened the stash in writeable
    pub fn can_recover_with_write_mode(&self) -> bool {
        match &self.inner {
            InternalStashError::StashNotWritable(_) => true,
            _ => false,
        }
    }

    /// The underlying transaction failed in a way that must be resolved by retrying
    pub fn should_retry(&self) -> bool {
        match &self.inner {
            InternalStashError::Postgres(e) => {
                matches!(e.code(), Some(&SqlState::T_R_SERIALIZATION_FAILURE))
            }
            _ => false,
        }
    }
}

/// Not a public API, only exposed for mz-stash.
#[derive(Debug)]
pub enum InternalStashError {
    Postgres(::tokio_postgres::Error),
    Fence(String),
    PeekSinceUpper(String),
    IncompatibleVersion {
        found_version: u64,
        min_stash_version: u64,
        stash_version: u64,
    },
    Proto(TryFromProtoError),
    Decoding(prost::DecodeError),
    Uninitialized,
    StashNotWritable(String),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Postgres(e) => write!(f, "postgres: {e}"),
            InternalStashError::Proto(e) => write!(f, "proto: {e}"),
            InternalStashError::Decoding(e) => write!(f, "prost decoding: {e}"),
            InternalStashError::Fence(e) => f.write_str(e),
            InternalStashError::PeekSinceUpper(e) => f.write_str(e),
            InternalStashError::IncompatibleVersion {
                found_version,
                min_stash_version,
                stash_version,
            } => {
                write!(f, "incompatible Stash version {found_version}, minimum: {min_stash_version}, current: {stash_version}")
            }
            InternalStashError::Uninitialized => write!(f, "uninitialized"),
            InternalStashError::StashNotWritable(e) => f.write_str(e),
            InternalStashError::Other(e) => f.write_str(e),
        }
    }
}

impl Error for StashError {}

impl From<InternalStashError> for StashError {
    fn from(inner: InternalStashError) -> StashError {
        StashError { inner }
    }
}

impl From<prost::DecodeError> for StashError {
    fn from(e: prost::DecodeError) -> Self {
        StashError {
            inner: InternalStashError::Decoding(e),
        }
    }
}

impl From<TryFromProtoError> for StashError {
    fn from(e: TryFromProtoError) -> Self {
        StashError {
            inner: InternalStashError::Proto(e),
        }
    }
}

impl From<String> for StashError {
    fn from(e: String) -> StashError {
        StashError {
            inner: InternalStashError::Other(e),
        }
    }
}

impl From<&str> for StashError {
    fn from(e: &str) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.into()),
        }
    }
}

impl From<std::io::Error> for StashError {
    fn from(e: std::io::Error) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.to_string()),
        }
    }
}

impl From<anyhow::Error> for StashError {
    fn from(e: anyhow::Error) -> Self {
        StashError {
            inner: InternalStashError::Other(e.to_string()),
        }
    }
}

impl From<tokio_postgres::Error> for StashError {
    fn from(e: tokio_postgres::Error) -> StashError {
        StashError {
            inner: InternalStashError::Postgres(e),
        }
    }
}
