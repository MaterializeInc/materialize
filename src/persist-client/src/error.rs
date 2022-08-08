// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Errors for the crate

use std::fmt::Debug;

use mz_persist::location::{Determinate, ExternalError, Indeterminate};
use timely::progress::Antichain;

use crate::r#impl::paths::PartialBlobKey;
use crate::{ShardId, WriterId};

/// An indication of whether the given error type indicates an operation
/// _definitely_ failed or if it _maybe_ failed.
///
/// This is more commonly called definite and indefinite, but "definite" already
/// means something very specific within Materialize.
pub trait Determinacy {
    /// Whether errors of this type are determinate or indeterminate.
    ///
    /// True indicates a determinate error: one where the operation definitely
    /// failed.
    ///
    /// False indicates an indeterminate error: one where the operation may have
    /// failed, but may have succeeded (the most common example being a
    /// timeout).
    const DETERMINANT: bool;
}

impl Determinacy for Determinate {
    const DETERMINANT: bool = true;
}

impl Determinacy for Indeterminate {
    const DETERMINANT: bool = false;
}

// An external error's variant declares whether it's determinate, but at the
// type level we have to fall back to indeterminate.
impl Determinacy for ExternalError {
    const DETERMINANT: bool = false;
}

/// An error resulting from invalid usage of the API.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum InvalidUsage<T> {
    /// Append bounds were invalid
    InvalidBounds {
        /// The given lower bound
        lower: Antichain<T>,
        /// The given upper bound
        upper: Antichain<T>,
    },
    /// An update was sent at an empty interval of times.
    InvalidEmptyTimeInterval {
        /// The given lower bound
        lower: Antichain<T>,
        /// The given upper bound
        upper: Antichain<T>,
        /// Set of keys containing updates.
        keys: Vec<PartialBlobKey>,
    },
    /// Bounds of a [crate::batch::Batch] are not valid for the attempted append call
    InvalidBatchBounds {
        /// The lower of the batch
        batch_lower: Antichain<T>,
        /// The upper of the batch
        batch_upper: Antichain<T>,
        /// The lower bound given to the append call
        append_lower: Antichain<T>,
        /// The upper bound given to the append call
        append_upper: Antichain<T>,
    },
    /// An update was not beyond the expected lower of the batch
    UpdateNotBeyondLower {
        /// Timestamp of the update
        ts: T,
        /// The given lower bound
        lower: Antichain<T>,
    },
    /// An update in the batch was beyond the expected upper
    UpdateBeyondUpper {
        /// The maximum timestamp of updates added to the batch.
        max_ts: T,
        /// The expected upper of the batch
        expected_upper: Antichain<T>,
    },
    /// A [crate::batch::Batch] or [crate::read::ReaderEnrichedHollowBatch] was
    /// given to a [crate::write::WriteHandle] from a different shard
    BatchNotFromThisShard {
        /// The shard of the batch
        batch_shard: ShardId,
        /// The shard of the handle
        handle_shard: ShardId,
    },
    /// The requested codecs don't match the actual ones in durable storage.
    CodecMismatch(CodecMismatch),
    /// An unregistered or expired [crate::write::WriterId] was used by [crate::write::WriteHandle]
    UnknownWriter(WriterId),
}

impl<T: Debug> std::fmt::Display for InvalidUsage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidUsage::InvalidBounds { lower, upper } => {
                write!(f, "invalid bounds [{:?}, {:?})", lower, upper)
            }
            InvalidUsage::InvalidEmptyTimeInterval { lower, upper, keys } => {
                write!(
                    f,
                    "invalid empty time interval [{:?}, {:?} {:?})",
                    lower, upper, keys
                )
            }
            InvalidUsage::InvalidBatchBounds {
                batch_lower,
                batch_upper,
                append_lower,
                append_upper,
            } => {
                write!(
                    f,
                    "invalid batch bounds [{:?}, {:?}) for append call with [{:?}, {:?})",
                    batch_lower, batch_upper, append_lower, append_upper
                )
            }
            InvalidUsage::UpdateNotBeyondLower { ts, lower } => {
                write!(f, "timestamp {:?} not beyond batch lower {:?}", ts, lower)
            }
            InvalidUsage::UpdateBeyondUpper {
                max_ts,
                expected_upper,
            } => write!(
                f,
                "maximum timestamp {:?} is beyond the expected batch upper: {:?}",
                max_ts, expected_upper
            ),
            InvalidUsage::BatchNotFromThisShard {
                batch_shard,
                handle_shard,
            } => write!(f, "batch was from {} not {}", batch_shard, handle_shard),
            InvalidUsage::CodecMismatch(err) => std::fmt::Display::fmt(err, f),
            InvalidUsage::UnknownWriter(writer_id) => {
                write!(f, "writer id {} is not registered", writer_id)
            }
        }
    }
}

impl<T: Debug> std::error::Error for InvalidUsage<T> {}

impl<T> Determinacy for InvalidUsage<T> {
    const DETERMINANT: bool = true;
}

/// The requested codecs don't match the actual ones in durable storage.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CodecMismatch {
    /// The requested (K, V, T, D) codecs.
    pub(crate) requested: (String, String, String, String),
    /// The actual (K, V, T, D) codecs in durable storage.
    pub(crate) actual: (String, String, String, String),
}

impl std::error::Error for CodecMismatch {}

impl Determinacy for CodecMismatch {
    const DETERMINANT: bool = true;
}

impl std::fmt::Display for CodecMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "requested codecs {:?} did not match ones in durable storage {:?}",
            self.requested, self.actual
        )
    }
}

impl<T> From<CodecMismatch> for InvalidUsage<T> {
    fn from(x: CodecMismatch) -> Self {
        InvalidUsage::CodecMismatch(x)
    }
}
