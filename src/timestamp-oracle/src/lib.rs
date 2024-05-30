// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interface/trait that provides write and read timestamps, reads observe
//! exactly their preceding writes.
//!
//! Specifically, all read timestamps will be greater or equal to all previously
//! reported completed write timestamps, and strictly less than all subsequently
//! emitted write timestamps.

use async_trait::async_trait;
use mz_ore::now::{EpochMillis, NowFn};

pub mod batching_oracle;
pub mod metrics;
pub mod postgres_oracle;
pub mod retry;

/// Timestamps used by writes in an Append command.
#[derive(Debug)]
pub struct WriteTimestamp<T = mz_repr::Timestamp> {
    /// Timestamp that the write will take place on.
    pub timestamp: T,
    /// Timestamp to advance the appended table to.
    pub advance_to: T,
}

/// A type that provides write and read timestamps, reads observe exactly their
/// preceding writes.
///
/// Specifically, all read timestamps will be greater or equal to all previously
/// reported completed write timestamps, and strictly less than all subsequently
/// emitted write timestamps.
#[async_trait]
pub trait TimestampOracle<T>: std::fmt::Debug {
    /// Acquire a new timestamp for writing.
    ///
    /// This timestamp will be strictly greater than all prior values of
    /// `self.read_ts()` and `self.write_ts()`.
    async fn write_ts(&self) -> WriteTimestamp<T>;

    /// Peek the current write timestamp.
    async fn peek_write_ts(&self) -> T;

    /// Acquire a new timestamp for reading.
    ///
    /// This timestamp will be greater or equal to all prior values of
    /// `self.apply_write(write_ts)`, and strictly less than all subsequent
    /// values of `self.write_ts()`.
    async fn read_ts(&self) -> T;

    /// Mark a write at `write_ts` completed.
    ///
    /// All subsequent values of `self.read_ts()` will be greater or equal to
    /// `write_ts`.
    async fn apply_write(&self, lower_bound: T);
}

/// A [`NowFn`] that is generic over the timestamp.
///
/// The oracle operations work in terms of [`mz_repr::Timestamp`] and we could
/// work around it by bridging between the two in the oracle implementation
/// itself. This wrapper type makes that slightly easier, though.
pub trait GenericNowFn<T>: Clone + Send + Sync {
    fn now(&self) -> T;
}

impl GenericNowFn<mz_repr::Timestamp> for NowFn<EpochMillis> {
    fn now(&self) -> mz_repr::Timestamp {
        (self)().into()
    }
}

impl<T: Clone + Send + Sync> GenericNowFn<T> for NowFn<T> {
    fn now(&self) -> T {
        (self)()
    }
}

// TODO: Gate this with a `#[cfg(test)]` again once the legacy catalog impl goes
// away.
pub mod tests {
    use std::sync::Arc;

    use futures::Future;
    use mz_repr::Timestamp;

    use super::*;

    // These test methods are meant to be used by tests for timestamp oracle
    // implementations.

    pub async fn timestamp_oracle_impl_test<F, NewFn>(
        mut new_fn: NewFn,
    ) -> Result<(), anyhow::Error>
    where
        F: Future<Output = Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
        NewFn: FnMut(String, NowFn, Timestamp) -> F,
    {
        // Normally, these could all be separate test methods but we bundle them
        // all together so that it's easier to call this one test method from
        // the implementation tests.

        // Timestamp::MIN as initial timestamp
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), Timestamp::MIN).await;
        assert_eq!(oracle.read_ts().await, Timestamp::MIN);
        assert_eq!(oracle.peek_write_ts().await, Timestamp::MIN);

        // Timestamp::MAX as initial timestamp
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), Timestamp::MAX).await;
        assert_eq!(oracle.read_ts().await, Timestamp::MAX);
        assert_eq!(oracle.peek_write_ts().await, Timestamp::MAX);

        // Timestamp::MAX-1 from NowFn. We have to step back by one, otherwise
        // `write_ts` can't determine the "advance_to" timestamp.
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(
            timeline,
            NowFn::from(|| Timestamp::MAX.step_back().expect("known to work").into()),
            Timestamp::MIN,
        )
        .await;
        // At first, read_ts and peek_write_ts stay where they are.
        assert_eq!(oracle.read_ts().await, Timestamp::MIN);
        assert_eq!(oracle.peek_write_ts().await, Timestamp::MIN);
        assert_eq!(
            oracle.write_ts().await.timestamp,
            Timestamp::MAX.step_back().expect("known to work")
        );
        // Now peek_write_ts jump to MAX-1 but read_ts stays.
        assert_eq!(oracle.read_ts().await, Timestamp::MIN);
        assert_eq!(
            oracle.peek_write_ts().await,
            Timestamp::MAX.step_back().expect("known to work")
        );

        // Repeated write_ts calls advance the timestamp.
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), Timestamp::MIN).await;
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(1u64));
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(2u64));

        // Repeated peek_write_ts calls _DON'T_ advance the timestamp.
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), Timestamp::MIN).await;
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(0u64));
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(0u64));

        // Interesting scenarios around apply_write, from its rustdoc.
        //
        // Scenario #1:
        // input <= r_0 <= w_0 -> r_1 = r_0 and w_1 = w_0
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), 10u64.into()).await;
        oracle.apply_write(5u64.into()).await;
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(10u64));
        assert_eq!(oracle.read_ts().await, Timestamp::from(10u64));

        // Scenario #2:
        // r_0 <= input <= w_0 -> r_1 = input and w_1 = w_0
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), 0u64.into()).await;
        // Have to bump the write_ts up manually:
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(1u64));
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(2u64));
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(3u64));
        assert_eq!(oracle.write_ts().await.timestamp, Timestamp::from(4u64));
        oracle.apply_write(2u64.into()).await;
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(4u64));
        assert_eq!(oracle.read_ts().await, Timestamp::from(2u64));

        // Scenario #3:
        // r_0 <= w_0 <= input -> r_1 = input and w_1 = input
        let timeline = uuid::Uuid::new_v4().to_string();
        let oracle = new_fn(timeline, NowFn::from(|| 0u64), 0u64.into()).await;
        oracle.apply_write(2u64.into()).await;
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(2u64));
        assert_eq!(oracle.read_ts().await, Timestamp::from(2u64));
        oracle.apply_write(4u64.into()).await;
        assert_eq!(oracle.peek_write_ts().await, Timestamp::from(4u64));
        assert_eq!(oracle.read_ts().await, Timestamp::from(4u64));

        Ok(())
    }
}
