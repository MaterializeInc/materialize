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

use mz_persist::location::ExternalError;
use timely::progress::Antichain;

use crate::ShardId;

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

// TODO: Some ExternalErrors actually are determinate (e.g. Postgres errors on
// txn conflict). We should split it so that these operations can be retried in
// more places.
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
    /// An update was not within valid bounds
    UpdateNotWithinBounds {
        /// Timestamp of the update
        ts: T,
        /// The given lower bound
        lower: Antichain<T>,
        /// The given upper bound
        upper: Antichain<T>,
    },
    /// A [crate::read::SnapshotSplit] was given to
    /// [crate::read::ReadHandle::snapshot_iter] from a different shard
    SnapshotNotFromThisShard {
        /// The shard of the snapshot
        snapshot_shard: ShardId,
        /// The shard of the handle
        handle_shard: ShardId,
    },
    /// The requested codecs don't match the actual ones in durable storage.
    CodecMismatch {
        /// The requested (K, V, T, D) codecs.
        requested: (String, String, String, String),
        /// The actual (K, V, T, D) codecs in durable storage.
        actual: (String, String, String, String),
    },
}

impl<T: Debug> std::fmt::Display for InvalidUsage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidUsage::InvalidBounds { lower, upper } => {
                write!(f, "invalid bounds [{:?}, {:?})", lower, upper)
            }
            InvalidUsage::UpdateNotWithinBounds { ts, lower, upper } => write!(
                f,
                "timestamp {:?} not with bounds [{:?}, {:?})",
                ts, lower, upper
            ),
            InvalidUsage::SnapshotNotFromThisShard {
                snapshot_shard,
                handle_shard,
            } => write!(
                f,
                "snapshot was from {} not {}",
                snapshot_shard, handle_shard
            ),
            InvalidUsage::CodecMismatch { requested, actual } => write!(
                f,
                "requested codecs {:?} did not match ones in durable storage {:?}",
                requested, actual
            ),
        }
    }
}

impl<T: Debug> std::error::Error for InvalidUsage<T> {}

impl<T> Determinacy for InvalidUsage<T> {
    const DETERMINANT: bool = true;
}
