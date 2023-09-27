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

use crate::coord::timeline::WriteTimestamp;

pub mod catalog_oracle;
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

    /// Get a shared, shallow clone of the oracle. Returns `None` if this oracle
    /// is not shareable.
    fn get_shared(&self) -> Option<Box<dyn ShareableTimestampOracle<T> + Send + Sync>>;
}

/// A shareable version of [`TimestampOracle`] that is `Send` and `Sync`.
///
/// We have this as a stop-gap solution while we still keep the legacy
/// in-memory/backed-by-Stash TimestampOracle around. Once we remove that we can
/// make [`TimestampOracle`] shareable.
#[async_trait]
pub trait ShareableTimestampOracle<T> {
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
    async fn apply_write(&mut self, lower_bound: T);
}
