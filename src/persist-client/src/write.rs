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
use std::marker::PhantomData;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use crate::error::{InvalidUsage, LocationError};

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

/// A "capability" granting the ability to apply updates to some shard at times
/// greater or equal to `self.upper()`.
pub struct WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    _phantom: PhantomData<(K, V, T, D)>,
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
        todo!()
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
    ) -> Result<Result<(), InvalidUsage>, LocationError> {
        todo!(
            "{:?}{:?}{:?}",
            timeout,
            updates.into_iter().size_hint(),
            new_upper
        );
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
{
    fn drop(&mut self) {
        todo!()
    }
}
