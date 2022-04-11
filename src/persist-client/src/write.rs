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
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::indexed::columnar::ColumnarRecordsVecBuilder;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, BlobMulti, ExternalError};
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::{info, trace};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::Machine;

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
    // TODO: Only the T bound should exist, the rest are a temporary artifact of
    // the current implementation (the ones on Machine infect everything).
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    pub(crate) writer_id: WriterId,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) blob: Arc<dyn BlobMulti + Send + Sync>,

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
    /// In contrast to [Self::compare_and_append], multiple [WriteHandle]s (with
    /// different [WriterId]s) may be used concurrently to write to the same
    /// shard, but in this case, the data being written must be identical (in
    /// the sense of "definite"-ness).  It's intended for replicated use by
    /// source ingestion, sinks, etc.
    ///
    /// All times in `updates` must be greater or equal to `self.upper()` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
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
    pub async fn append<'a, I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>>(
        &mut self,
        timeout: Duration,
        updates: I,
        new_upper: Antichain<T>,
    ) -> Result<Result<(), InvalidUsage>, ExternalError> {
        trace!(
            "WriteHandle::append timeout={:?} new_upper={:?}",
            timeout,
            new_upper
        );
        let deadline = Instant::now() + timeout;

        let lower = self.upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        // TODO: Instead construct a Vec of blob keys here so it can be empty
        // (if there are no updates) and bounded memory usage (if updates is
        // large).
        let key = Uuid::new_v4().to_string();
        let mut value = Vec::new();
        if let Err(err) = Self::encode_batch(&mut value, &desc, updates) {
            return Ok(Err(err));
        }
        self.blob
            .set(deadline, &key, value, Atomicity::RequireAtomic)
            .await?;

        let res = self
            .machine
            .append(deadline, &self.writer_id, &[key], &desc)
            .await?;
        match res {
            Ok(_) => self.upper = desc.upper().clone(),
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(()))
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `new_upper` iff the current global upper of this shard is
    /// `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Err` containing the current global upper is returned.
    ///
    /// In contrast to [Self::append], this linearizes mutations from all
    /// writers. It's intended for use as an atomic primitive for timestamp
    /// bindings, SQL tables, etc.
    ///
    /// All times in `updates` must be greater or equal to `expected_upper` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
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
            "WriteHandle::write_batch timeout={:?} new_upper={:?}",
            timeout,
            new_upper
        );
        let deadline = Instant::now() + timeout;

        let lower = expected_upper;
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        // TODO: Instead construct a Vec of blob keys here so it can be empty
        // (if there are no updates) and bounded memory usage (if updates is
        // large).
        let key = Uuid::new_v4().to_string();
        let mut value = Vec::new();
        if let Err(err) = Self::encode_batch(&mut value, &desc, updates) {
            return Ok(Err(err));
        }
        self.blob
            .set(deadline, &key, value, Atomicity::RequireAtomic)
            .await?;

        let res = self
            .machine
            .compare_and_append(deadline, &self.writer_id, &[key], &desc)
            .await?;
        match res {
            Ok(Ok(_seqno)) => self.upper = desc.upper().clone(),
            Ok(Err(current_upper)) => return Ok(Ok(Err(current_upper))),
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(Ok(())))
    }

    fn encode_batch<'a, B, I>(
        buf: &mut B,
        desc: &Description<T>,
        updates: I,
    ) -> Result<(), InvalidUsage>
    where
        B: BufMut,
        I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>,
    {
        let iter = updates.into_iter();
        let size_hint = iter.size_hint();

        let (mut key_buf, mut val_buf) = (Vec::new(), Vec::new());
        let mut builder = ColumnarRecordsVecBuilder::default();
        for ((k, v), t, d) in iter {
            if !desc.lower().less_equal(&t) || desc.upper().less_equal(&t) {
                return Err(InvalidUsage(anyhow!(
                    "entry timestamp {:?} doesn't fit in batch desc: {:?}",
                    t,
                    desc
                )));
            }

            trace!("writing update {:?}", ((k, v), t, d));
            key_buf.clear();
            val_buf.clear();
            k.encode(&mut key_buf);
            v.encode(&mut val_buf);
            // TODO: Get rid of the from_le_bytes.
            let t = u64::from_le_bytes(T::encode(t));
            let d = i64::from_le_bytes(D::encode(d));

            if builder.len() == 0 {
                // Use the first record to attempt to pre-size the builder
                // allocations. This uses the iter's size_hint's lower+1 to
                // match the logic in Vec.
                let (lower, _) = size_hint;
                let additional = usize::saturating_add(lower, 1);
                builder.reserve(additional, key_buf.len(), val_buf.len());
            }
            builder.push(((&key_buf, &val_buf), t, d))
        }

        // TODO: Get rid of the from_le_bytes.
        let desc = Description::new(
            Antichain::from(
                desc.lower()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.upper()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.since()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
        );

        let batch = BlobTraceBatchPart {
            desc,
            updates: builder.finish(),
            index: 0,
        };
        batch.encode(buf);
        Ok(())
    }

    /// Test helper for [Self::append]-ing a slice of owned updates.
    #[cfg(test)]
    pub async fn append_slice(
        &mut self,
        updates: &[((K, V), T, D)],
        new_upper: T,
    ) -> Result<Result<(), InvalidUsage>, ExternalError> {
        use crate::NO_TIMEOUT;

        self.append(
            NO_TIMEOUT,
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(new_upper),
        )
        .await
    }

    /// Test helper for [Self::compare_and_append]-ing a slice of owned updates.
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
    // TODO: Only the T bound should exist, the rest are a temporary artifact of
    // the current implementation (the ones on Machine infect everything).
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64,
{
    fn drop(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        // TODO: Use tokio instead of futures_executor.
        let res = futures_executor::block_on(self.machine.expire_writer(deadline, &self.writer_id));
        if let Err(err) = res {
            info!(
                "drop failed to expire writer {}, falling back to lease timeout: {:?}",
                self.writer_id, err
            );
        }
    }
}
