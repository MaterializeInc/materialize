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
use mz_ore::now::NowFn;

use crate::coord::timeline::WriteTimestamp;

pub mod catalog_oracle;
pub mod metrics;
pub mod postgres_oracle;

/// A type that provides write and read timestamps, reads observe exactly their
/// preceding writes.
///
/// Specifically, all read timestamps will be greater or equal to all previously
/// reported completed write timestamps, and strictly less than all subsequently
/// emitted write timestamps.
#[async_trait(?Send)]
pub trait TimestampOracle<T> {
    /// Acquire a new timestamp for writing.
    ///
    /// This timestamp will be strictly greater than all prior values of
    /// `self.read_ts()` and `self.write_ts()`.
    async fn write_ts(&mut self) -> WriteTimestamp<T>;

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
    async fn apply_write(&mut self, write_ts: T);
}

/// A [`NowFn`] that is generic over the timestamp.
///
/// The oracle operations work in terms of [`mz_repr::Timestamp`] and we could
/// work around it by bridging between the two in the oracle implementation
/// itself. This wrapper type makes that slightly easier, though.
pub trait GenericNowFn<T>: Clone + Send + Sync {
    fn now(&self) -> T;
}

impl GenericNowFn<mz_repr::Timestamp> for NowFn {
    fn now(&self) -> mz_repr::Timestamp {
        (self)().into()
    }
}
