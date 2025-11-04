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

use timely::progress::{Antichain, Timestamp};

use crate::ShardId;

/// An error resulting from invalid usage of the API.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum InvalidUsage<T> {
    /// The data format of the shard is not compatible with the current code version.
    IncompatibleVersion {
        /// The version of the metadata.
        version: semver::Version,
    },
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
        keys: Vec<String>,
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
        /// The timestamp of the update
        ts: T,
        /// The expected upper of the batch
        expected_upper: Antichain<T>,
    },
    /// A [crate::batch::Batch] or [crate::fetch::LeasedBatchPart] was
    /// given to a [crate::write::WriteHandle] from a different shard
    BatchNotFromThisShard {
        /// The shard of the batch
        batch_shard: ShardId,
        /// The shard of the handle
        handle_shard: ShardId,
    },
    /// Attempted to finalize a shard without advancing frontiers.
    FinalizationError {
        /// The current since of the shard.
        since: Antichain<T>,
        /// The current upper of the shard.
        upper: Antichain<T>,
    },
    /// The requested codecs don't match the actual ones in durable storage.
    CodecMismatch(Box<CodecMismatch>),
    /// An invalid usage of [crate::batch::Batch::rewrite_ts].
    InvalidRewrite(String),
}

impl<T: Debug> std::fmt::Display for InvalidUsage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidUsage::IncompatibleVersion { version } => {
                write!(f, "incompatible with data version {}", version)
            }
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
            InvalidUsage::UpdateBeyondUpper { ts, expected_upper } => write!(
                f,
                "timestamp {:?} is beyond the expected batch upper: {:?}",
                ts, expected_upper
            ),
            InvalidUsage::BatchNotFromThisShard {
                batch_shard,
                handle_shard,
            } => write!(f, "batch was from {} not {}", batch_shard, handle_shard),
            InvalidUsage::FinalizationError { since, upper } => {
                write!(
                    f,
                    "finalized without fully advancing since {since:?} and upper {upper:?}"
                )
            }
            InvalidUsage::CodecMismatch(err) => std::fmt::Display::fmt(err, f),
            InvalidUsage::InvalidRewrite(err) => write!(f, "invalid rewrite: {err}"),
        }
    }
}

impl<T: Debug> std::error::Error for InvalidUsage<T> {}

/// The requested codecs don't match the actual ones in durable storage.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct CodecMismatch {
    /// The requested (K, V, T, D) codecs.
    ///
    /// The last element in the tuple is Some when the name of the codecs match,
    /// but the concrete types don't: e.g. mz_repr::Timestamp and u64.
    pub(crate) requested: (String, String, String, String, Option<CodecConcreteType>),
    /// The actual (K, V, T, D) codecs in durable storage.
    ///
    /// The last element in the tuple is Some when the name of the codecs match,
    /// but the concrete types don't: e.g. mz_repr::Timestamp and u64.
    pub(crate) actual: (String, String, String, String, Option<CodecConcreteType>),
}

impl std::error::Error for CodecMismatch {}

impl std::fmt::Display for CodecMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "requested codecs {:?} did not match ones in durable storage {:?}",
            self.requested, self.actual
        )
    }
}

/// The concrete type of a [mz_persist_types::Codec] or
/// [mz_persist_types::Codec64] impl.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct CodecConcreteType(#[allow(dead_code)] pub(crate) &'static str);

impl<T> From<CodecMismatch> for InvalidUsage<T> {
    fn from(x: CodecMismatch) -> Self {
        InvalidUsage::CodecMismatch(Box::new(x))
    }
}

impl<T> From<Box<CodecMismatch>> for InvalidUsage<T> {
    fn from(x: Box<CodecMismatch>) -> Self {
        InvalidUsage::CodecMismatch(x)
    }
}

#[derive(Debug)]
pub(crate) struct CodecMismatchT {
    /// The requested T codec.
    pub(crate) requested: String,
    /// The actual T codec in durable storage.
    pub(crate) actual: String,
}

impl std::error::Error for CodecMismatchT {}

impl std::fmt::Display for CodecMismatchT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "requested ts codec {:?} did not match one in durable storage {:?}",
            self.requested, self.actual
        )
    }
}

/// An error returned from [crate::write::WriteHandle::compare_and_append] (and
/// variants) when the expected upper didn't match the actual current upper of
/// the shard.
#[derive(Debug, PartialEq)]
pub struct UpperMismatch<T> {
    /// The expected upper given by the caller.
    pub expected: Antichain<T>,
    /// The actual upper of the shard at the time compare_and_append evaluated.
    pub current: Antichain<T>,
}

impl<T: Timestamp> std::fmt::Display for UpperMismatch<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "expected upper {:?} did not match current upper {:?}",
            self.expected, self.current,
        )
    }
}
