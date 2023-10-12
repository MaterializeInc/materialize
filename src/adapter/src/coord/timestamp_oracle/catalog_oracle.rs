// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle that relies on the [`Catalog`] for persistence/durability
//! and reserves ranges of timestamps.

use std::sync::Arc;
use std::time::Duration;
use std::{cmp, thread};

use async_trait::async_trait;
use mz_ore::now::NowFn;
use mz_repr::{Timestamp, TimestampManipulation};
use mz_storage_types::sources::Timeline;
use once_cell::sync::Lazy;
use tracing::error;

use crate::catalog::{Catalog, Error};
use crate::coord::timeline::WriteTimestamp;
use crate::coord::timestamp_oracle::{catalog_oracle, TimestampOracle};
use crate::util::ResultExt;

/// A type that provides write and read timestamps, reads observe exactly their
/// preceding writes.
///
/// Specifically, all read timestamps will be greater or equal to all previously
/// reported completed write timestamps, and strictly less than all subsequently
/// emitted write timestamps.
///
/// A timeline can perform reads and writes. Reads happen at the read timestamp
/// and writes happen at the write timestamp. After the write has completed, but
/// before a response is sent, the read timestamp must be updated to a value
/// greater than or equal to `self.write_ts`.
struct InMemoryTimestampOracle<T> {
    read_ts: T,
    write_ts: T,
    next: Box<dyn Fn() -> T>,
}

impl<T: TimestampManipulation> InMemoryTimestampOracle<T> {
    /// Create a new timeline, starting at the indicated time. `next` generates
    /// new timestamps when invoked. The timestamps have no requirements, and
    /// can retreat from previous invocations.
    pub fn new<F>(initially: T, next: F) -> Self
    where
        F: Fn() -> T + 'static,
    {
        Self {
            read_ts: initially.clone(),
            write_ts: initially,
            next: Box::new(next),
        }
    }

    /// Acquire a new timestamp for writing.
    ///
    /// This timestamp will be strictly greater than all prior values of
    /// `self.read_ts()` and `self.write_ts()`.
    fn write_ts(&mut self) -> WriteTimestamp<T> {
        let mut next = (self.next)();
        if next.less_equal(&self.write_ts) {
            next = self.write_ts.step_forward();
        }
        assert!(self.read_ts.less_than(&next));
        assert!(self.write_ts.less_than(&next));
        self.write_ts = next.clone();
        assert!(self.read_ts.less_equal(&self.write_ts));
        let advance_to = next.step_forward();
        WriteTimestamp {
            timestamp: next,
            advance_to,
        }
    }

    /// Peek the current write timestamp.
    fn peek_write_ts(&self) -> T {
        self.write_ts.clone()
    }

    /// Acquire a new timestamp for reading.
    ///
    /// This timestamp will be greater or equal to all prior values of
    /// `self.apply_write(write_ts)`, and strictly less than all subsequent
    /// values of `self.write_ts()`.
    fn read_ts(&self) -> T {
        self.read_ts.clone()
    }

    /// Mark a write at `write_ts` completed.
    ///
    /// All subsequent values of `self.read_ts()` will be greater or equal to
    /// `write_ts`.
    fn apply_write(&mut self, write_ts: T) {
        if self.read_ts.less_than(&write_ts) {
            self.read_ts = write_ts;

            if self.write_ts.less_than(&self.read_ts) {
                self.write_ts = self.read_ts.clone();
            }
        }
        assert!(self.read_ts.less_equal(&self.write_ts));
    }
}

/// Interval used to persist durable timestamps. See [`CatalogTimestampOracle`]
/// for more details.
pub static TIMESTAMP_PERSIST_INTERVAL: Lazy<mz_repr::Timestamp> = Lazy::new(|| {
    Duration::from_secs(5)
        .as_millis()
        .try_into()
        .expect("5 seconds can fit into `Timestamp`")
});

/// The Coordinator tries to prevent the persisted timestamp from exceeding a
/// value [`TIMESTAMP_INTERVAL_UPPER_BOUND`] times
/// [`TIMESTAMP_PERSIST_INTERVAL`] larger than the current system time.
pub const TIMESTAMP_INTERVAL_UPPER_BOUND: u64 = 2;

/// A type that wraps a [`InMemoryTimestampOracle`] and provides durable timestamps.
/// This allows us to recover a timestamp that is larger than all previous
/// timestamps on restart. The protocol is based on timestamp recovery from
/// Percolator <https://research.google/pubs/pub36726/>. We "pre-allocate" a
/// group of timestamps at once, and only durably store the largest of those
/// timestamps. All timestamps within that interval can be served directly from
/// memory, without going to disk. On restart, we re-initialize the current
/// timestamp to a value one larger than the persisted timestamp.
///
/// See [`TimestampOracle`] for more details on the properties of the
/// timestamps.
pub struct CatalogTimestampOracle<T> {
    timestamp_oracle: InMemoryTimestampOracle<T>,
    durable_timestamp: T,
    persist_interval: T,
    timestamp_persistence: Box<dyn TimestampPersistence<T>>,
}

impl<T: TimestampManipulation> CatalogTimestampOracle<T> {
    /// Create a new durable timeline, starting at the indicated time.
    /// Timestamps will be allocated in groups of size `persist_interval`. Also
    /// returns the new timestamp that needs to be persisted to disk.
    ///
    /// See [`InMemoryTimestampOracle::new`] for more details.
    pub(crate) async fn new<F, P>(
        initially: T,
        next: F,
        persist_interval: T,
        timestamp_persistence: P,
    ) -> Self
    where
        F: Fn() -> T + 'static,
        P: TimestampPersistence<T> + 'static,
    {
        let mut oracle = Self {
            timestamp_oracle: InMemoryTimestampOracle::new(initially.clone(), next),
            durable_timestamp: initially.clone(),
            persist_interval,
            timestamp_persistence: Box::new(timestamp_persistence),
        };
        oracle.maybe_allocate_new_timestamps(&initially).await;
        oracle
    }

    /// Checks to see if we can serve the timestamp from memory, or if we need
    /// to durably store a new timestamp.
    ///
    /// If `ts` is less than the persisted timestamp then we can serve `ts` from
    /// memory, otherwise we need to durably store some timestamp greater than
    /// `ts`.
    async fn maybe_allocate_new_timestamps(&mut self, ts: &T) {
        if self.durable_timestamp.less_equal(ts)
            // Since the timestamp is at its max value, we know that no other Coord can
            // allocate a higher value.
            && self.durable_timestamp.less_than(&T::maximum())
        {
            self.durable_timestamp = ts.step_forward_by(&self.persist_interval);
            let res = self
                .timestamp_persistence
                .persist_timestamp(self.durable_timestamp.clone())
                .await;

            res.unwrap_or_terminate("can't persist timestamp");
        }
    }
}

#[async_trait(?Send)]
impl<T: TimestampManipulation> TimestampOracle<T> for CatalogTimestampOracle<T> {
    async fn write_ts(&mut self) -> WriteTimestamp<T> {
        let ts = self.timestamp_oracle.write_ts();
        self.maybe_allocate_new_timestamps(&ts.timestamp).await;
        ts
    }

    async fn peek_write_ts(&self) -> T {
        self.timestamp_oracle.peek_write_ts()
    }

    async fn read_ts(&self) -> T {
        let ts = self.timestamp_oracle.read_ts();
        assert!(
            ts.less_than(&self.durable_timestamp),
            "read_ts should not advance the global timestamp"
        );
        ts
    }

    async fn apply_write(&mut self, write_ts: T) {
        self.timestamp_oracle.apply_write(write_ts.clone());
        self.maybe_allocate_new_timestamps(&write_ts).await;
    }
}

/// Provides persistence of timestamps for [`CatalogTimestampOracle`].
#[async_trait::async_trait]
pub trait TimestampPersistence<T> {
    /// Persist new global timestamp to disk.
    async fn persist_timestamp(&self, timestamp: T) -> Result<(), Error>;
}

/// A [`TimestampPersistence`] that is backed by a [`Catalog`].
pub(crate) struct CatalogTimestampPersistence {
    timeline: Timeline,
    catalog: Arc<Catalog>,
}

impl CatalogTimestampPersistence {
    pub(crate) fn new(timeline: Timeline, catalog: Arc<Catalog>) -> Self {
        Self { timeline, catalog }
    }
}

#[async_trait::async_trait]
impl TimestampPersistence<mz_repr::Timestamp> for CatalogTimestampPersistence {
    async fn persist_timestamp(&self, timestamp: mz_repr::Timestamp) -> Result<(), Error> {
        self.catalog
            .persist_timestamp(&self.timeline, timestamp)
            .await
    }
}

/// Convenience function for calculating the current upper bound that we want to
/// prevent the global timestamp from exceeding.
// TODO(aljoscha): These internal details of the oracle are leaking through to
// multiple places in the coordinator.
pub(crate) fn upper_bound(now: &mz_repr::Timestamp) -> mz_repr::Timestamp {
    now.saturating_add(
        TIMESTAMP_PERSIST_INTERVAL.saturating_mul(Timestamp::from(TIMESTAMP_INTERVAL_UPPER_BOUND)),
    )
}

/// Returns the current system time while protecting against backwards time
/// jumps.
///
/// The caller is responsible for providing the previously recorded system time
/// via the `previous_now` parameter.
///
/// If `previous_now` is more than `TIMESTAMP_INTERVAL_UPPER_BOUND *
/// TIMESTAMP_PERSIST_INTERVAL` milliseconds ahead of the current system time
/// (i.e., due to a backwards time jump), this function will block until the
/// system time advances.
///
/// The returned time is guaranteed to be greater than or equal to
/// `previous_now`.
// TODO(aljoscha): These internal details of the oracle are leaking through to
// multiple places in the coordinator.
pub(crate) fn monotonic_now(now: NowFn, previous_now: mz_repr::Timestamp) -> mz_repr::Timestamp {
    let mut now_ts = now();
    let monotonic_now = cmp::max(previous_now, now_ts.into());
    let mut upper_bound = catalog_oracle::upper_bound(&mz_repr::Timestamp::from(now_ts));
    while monotonic_now > upper_bound {
        // Cap retry time to 1s. In cases where the system clock has retreated
        // by some large amount of time, this prevents against then waiting for
        // that large amount of time in case the system clock then advances back
        // to near what it was.
        let remaining_ms = cmp::min(monotonic_now.saturating_sub(upper_bound), 1_000.into());
        error!(
            "Coordinator tried to start with initial timestamp of \
            {monotonic_now}, which is more than \
            {TIMESTAMP_INTERVAL_UPPER_BOUND} intervals of size {} larger than \
            now, {now_ts}. Sleeping for {remaining_ms} ms.",
            *TIMESTAMP_PERSIST_INTERVAL
        );
        thread::sleep(Duration::from_millis(remaining_ms.into()));
        now_ts = now();
        upper_bound = catalog_oracle::upper_bound(&mz_repr::Timestamp::from(now_ts));
    }
    monotonic_now
}
