// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
// TODO: Remove this once we've got at least a placeholder implementation.
#![allow(clippy::todo)]

//! An abstraction presenting as a durable time-varying collection (aka shard)

use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use uuid::Uuid;

use crate::error::LocationError;
use crate::read::ReadHandle;
use crate::write::WriteHandle;

pub mod error;
pub mod read;
pub mod write;

// Notes
// - Pretend that everything marked with Serialize and Deserialize instead has
//   some sort of encode/decode API that uses byte slices, Buf+BufMut, or proto,
//   depending on what we decide.

// TODOs
// - Decide if the inner and outer error of the two-level errors should be
//   swapped.

/// A location in s3, other cloud storage, or otherwise "durable storage" used
/// by persist. This location can contain any number of persist shards.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Location {
    bucket: String,
    prefix: String,
}

/// An opaque identifier for a persist durable TVC (aka shard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Id([u8; 16]);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

impl Id {
    /// Returns a random [Id] that is reasonably likely to have never been
    /// generated before.
    pub fn new() -> Self {
        Id(Uuid::new_v4().as_bytes().to_owned())
    }
}

/// A handle for interacting with the set of persist shard made durable at a
/// single [Location].
pub struct Client {
    _phantom: PhantomData<()>,
}

impl Client {
    /// Returns a new client for interfacing with persist shards made durable to
    /// the given `location`.
    ///
    /// The same `location` may be used concurrently from multiple processes.
    /// Concurrent usage is subject to the constraints documented on individual
    /// methods (mostly [WriteHandle::write_batch]).
    pub async fn new(
        timeout: Duration,
        location: Location,
        role_arn: Option<String>,
    ) -> Result<Self, LocationError> {
        todo!("{:?}{:?}{:?}", timeout, location, role_arn)
    }

    /// Provides capabilities for the durable TVC identified by `id` at its
    /// current since and upper frontiers.
    ///
    /// This method is a best-effort attempt to regain control of the frontiers
    /// of a shard. Its most common uses are to recover capabilities that have
    /// expired (leases) or to attempt to read a TVC that one did not create (or
    /// otherwise receive capabilities for). If the frontiers have been fully
    /// released by all other parties, this call may result in capabilities with
    /// empty frontiers (which are useless).
    ///
    /// If `id` has never been used before, initializes a new shard and returns
    /// handles with `since` and `upper` frontiers set to initial values of
    /// `Antichain::from_elem(T::minimum())`.
    pub async fn open<K, V, T, D>(
        &self,
        timeout: Duration,
        id: Id,
    ) -> Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), LocationError>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        todo!("{:?}{:?}", timeout, id)
    }
}
