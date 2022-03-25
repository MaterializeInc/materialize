// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Write capabilities and handles

use std::fmt::Debug;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::ExternalError;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::trace;
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::shard::Shard;

/// An opaque identifier for a writer of a persist durable TVC (aka shard).
///
/// Unlike [crate::read::ReaderId], this is intentionally not Serialize and
/// Deserialize. It's difficult to reason about the behavior if multiple writers
/// accidentally end up concurrently using the same [WriterId] and we haven't
/// (yet) found a need for it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriterId(pub(crate) [u8; 16]);

impl std::fmt::Display for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

impl WriterId {
    pub(crate) fn new() -> Self {
        WriterId(*Uuid::new_v4().as_bytes())
    }
}

/// A "capability" granting the ability to apply updates to some shard at times
/// greater or equal to `self.upper()`.
#[derive(Debug)]
pub struct WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    pub(crate) writer_id: WriterId,
    pub(crate) shard: Shard<K, V, T, D>,

    pub(crate) upper: Antichain<T>,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// This handle's `upper` frontier.
    ///
    /// This will always be greater or equal to the shard-global `upper`.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `new_upper`.
    ///
    /// All times in `updates` must be greater or equal to `self.upper()` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is unexpected to call this with `new_upper`
    /// equal to `self.upper()`, as it would mean `updates` must be empty
    /// (making the entire call a no-op).
    ///
    /// Multiple [WriteHandle]s (with different [WriterId]s) may be used
    /// concurrently to write to the same shard, but in this case, the data
    /// being written must be identical (in the sense of "definite"-ness).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// TODO: Introduce an AsyncIterator (futures::Stream) variant of this. Or,
    /// given that the AsyncIterator version would be strictly more general,
    /// alter this one if it turns out that the compiler can optimize out the
    /// overhead.
    pub async fn write_batch<'a, I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>>(
        &mut self,
        timeout: Duration,
        updates: I,
        new_upper: Antichain<T>,
    ) -> Result<Result<(), InvalidUsage>, ExternalError> {
        trace!(
            "WriteHandle::write_batch timeout={:?} new_upper={:?}",
            timeout,
            new_upper
        );
        let res = self
            .shard
            .write_batch(&self.writer_id, updates, new_upper)
            .await;
        match res {
            Ok(x) => self.upper = x,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(()))
    }

    /// Test helper for writing a slice of owned updates.
    #[cfg(test)]
    pub async fn write_batch_slice(
        &mut self,
        updates: &[((K, V), T, D)],
        new_upper: T,
    ) -> Result<Result<(), InvalidUsage>, ExternalError> {
        use crate::NO_TIMEOUT;

        self.write_batch(
            NO_TIMEOUT,
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(new_upper),
        )
        .await
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `new_upper` iff the current global upper of this shard is `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Err` containing the current global upper is returned.
    ///
    /// All times in `updates` must be greater or equal to the global upper and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is unexpected to call this with `new_upper`
    /// equal to `self.upper()`, as it would mean `updates` must be empty
    /// (making the entire call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky two-level Result is to enable more obvious error handling in
    /// the caller. See <http://sled.rs/errors.html> for details.
    pub async fn compare_and_append<'a, I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>>(
        &mut self,
        timeout: Duration,
        updates: I,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<Result<(), Antichain<T>>, InvalidUsage>, ExternalError> {
        trace!(
            "WriteHandle::compare_and_append timeout={:?} new_upper={:?}",
            timeout,
            new_upper
        );
        let res = self
            .shard
            .compare_and_append(&self.writer_id, updates, expected_upper, new_upper)
            .await;
        let res = match res {
            Ok(x) => x.map(|new_upper| {
                self.upper = new_upper;
                ()
            }),
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(res))
    }

    /// Test helper for writing a slice of owned updates.
    #[cfg(test)]
    pub async fn compare_and_append_slice(
        &mut self,
        updates: &[((K, V), T, D)],
        expected_upper: T,
        new_upper: T,
    ) -> Result<Result<Result<(), Antichain<T>>, InvalidUsage>, ExternalError> {
        use crate::NO_TIMEOUT;

        self.compare_and_append(
            NO_TIMEOUT,
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
        )
        .await
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        // TODO: Use tokio instead of futures_executor.
        futures_executor::block_on(self.shard.deregister_writer(&self.writer_id));
    }
}
