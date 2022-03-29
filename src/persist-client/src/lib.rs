// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

//! An abstraction presenting as a durable time-varying collection (aka shard)

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use tracing::trace;
use uuid::Uuid;

use crate::error::LocationError;
use crate::r#impl::shard::{Shard, State};
use crate::read::ReadHandle;
use crate::write::WriteHandle;

pub mod error;
pub mod read;
pub mod write;

/// An implementation of the public crate interface.
///
/// TODO: Move this to another crate.
pub(crate) mod r#impl {
    pub mod shard;
}

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
#[derive(Debug)]
pub struct Client {
    shards: Arc<Mutex<HashMap<Id, Arc<Mutex<State>>>>>,
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
        trace!(
            "Client::new timeout={:?} location={:?} role_arn={:?}",
            timeout,
            location,
            role_arn
        );
        Ok(Client {
            shards: Arc::new(Mutex::new(HashMap::new())),
        })
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
        trace!("Client::open timeout={:?} id={:?}", timeout, id);
        let mut shards = self.shards.lock().await;
        let state = shards
            .entry(id)
            .or_insert_with(|| Shard::<K, V, T, D>::new(id.clone()).into_inner());
        let state = Shard::decode(Arc::clone(state))
            .await
            .expect("internal error: newly created shard's codecs don't match");
        Ok(state.recover_capabilities().await)
    }
}

#[cfg(test)]
const NO_TIMEOUT: Duration = Duration::from_secs(1_000_000);

#[cfg(test)]
mod tests {
    use timely::progress::Antichain;

    use crate::read::ListenEvent;

    use super::*;

    async fn new_test_client() -> Result<Client, LocationError> {
        let location = Location {
            bucket: "unused".into(),
            prefix: "unused".into(),
        };
        Client::new(NO_TIMEOUT, location, None).await
    }

    fn all_ok<'a, K, V, T, D, I>(iter: I) -> Vec<((Result<K, String>, Result<V, String>), T, D)>
    where
        K: Clone + 'a,
        V: Clone + 'a,
        T: Clone + 'a,
        D: Clone + 'a,
        I: IntoIterator<Item = &'a ((K, V), T, D)>,
    {
        iter.into_iter()
            .map(|((k, v), t, d)| ((Ok(k.clone()), Ok(v.clone())), t.clone(), d.clone()))
            .collect()
    }

    #[tokio::test]
    async fn sanity_check() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging_default("warn");

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await?
            .open::<String, String, u64, i64>(NO_TIMEOUT, Id::new())
            .await?;
        assert_eq!(write.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));

        // Write a [0,3) batch.
        write.write_batch_slice(&data[..2], 3).await??;
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        // Grab a snapshot and listener as_of 2.
        let mut snap = read.snapshot_one(1).await??;
        let mut listen = read.listen(NO_TIMEOUT, Antichain::from_elem(1)).await??;

        // Snapshot should only have part of what we wrote.
        assert_eq!(snap.read_all().await?, all_ok(&data[..1]));

        // Write a [3,4) batch.
        write.write_batch_slice(&data[2..], 4).await??;
        assert_eq!(write.upper(), &Antichain::from_elem(4));

        // Listen should have part of the initial write plus the new one.
        let expected_events = vec![
            ListenEvent::Updates(all_ok(&data[1..])),
            ListenEvent::Progress(Antichain::from_elem(4)),
        ];
        assert_eq!(listen.poll_next(NO_TIMEOUT).await?, expected_events);

        // Downgrading the since is tracked locally (but otherwise is a no-op).
        read.downgrade_since(NO_TIMEOUT, Antichain::from_elem(2))
            .await??;
        assert_eq!(read.since(), &Antichain::from_elem(2));

        Ok(())
    }
}
