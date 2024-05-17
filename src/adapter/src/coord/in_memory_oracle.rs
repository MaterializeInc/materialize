// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle that relies on the [`crate::catalog::Catalog`] for persistence/durability
//! and reserves ranges of timestamps.

use std::fmt::Debug;

use derivative::Derivative;
use mz_repr::TimestampManipulation;
use mz_timestamp_oracle::{GenericNowFn, WriteTimestamp};

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
#[derive(Derivative)]
#[derivative(Debug)]
pub struct InMemoryTimestampOracle<T, N>
where
    T: Debug,
    N: GenericNowFn<T>,
{
    read_ts: T,
    write_ts: T,
    #[derivative(Debug = "ignore")]
    #[allow(dead_code)]
    next: N,
}

impl<T: TimestampManipulation, N> InMemoryTimestampOracle<T, N>
where
    N: GenericNowFn<T>,
{
    /// Create a new timeline, starting at the indicated time. `next` generates
    /// new timestamps when invoked. The timestamps have no requirements, and
    /// can retreat from previous invocations.
    pub fn new(initially: T, next: N) -> Self
    where
        N: GenericNowFn<T>,
    {
        Self {
            read_ts: initially.clone(),
            write_ts: initially,
            next,
        }
    }

    /// Acquire a new timestamp for writing.
    ///
    /// This timestamp will be strictly greater than all prior values of
    /// `self.read_ts()` and `self.write_ts()`.
    #[allow(dead_code)]
    fn write_ts(&mut self) -> WriteTimestamp<T> {
        let mut next = self.next.now();
        if next.less_equal(&self.write_ts) {
            next = TimestampManipulation::step_forward(&self.write_ts);
        }
        assert!(self.read_ts.less_than(&next));
        assert!(self.write_ts.less_than(&next));
        self.write_ts = next.clone();
        assert!(self.read_ts.less_equal(&self.write_ts));
        let advance_to = TimestampManipulation::step_forward(&next);
        WriteTimestamp {
            timestamp: next,
            advance_to,
        }
    }

    /// Peek the current write timestamp.
    #[allow(dead_code)]
    fn peek_write_ts(&self) -> T {
        self.write_ts.clone()
    }

    /// Acquire a new timestamp for reading.
    ///
    /// This timestamp will be greater or equal to all prior values of
    /// `self.apply_write(write_ts)`, and strictly less than all subsequent
    /// values of `self.write_ts()`.
    pub(crate) fn read_ts(&self) -> T {
        self.read_ts.clone()
    }

    /// Mark a write at `write_ts` completed.
    ///
    /// All subsequent values of `self.read_ts()` will be greater or equal to
    /// `write_ts`.
    pub(crate) fn apply_write(&mut self, write_ts: T) {
        if self.read_ts.less_than(&write_ts) {
            self.read_ts = write_ts;

            if self.write_ts.less_than(&self.read_ts) {
                self.write_ts = self.read_ts.clone();
            }
        }
        assert!(self.read_ts.less_equal(&self.write_ts));
    }
}
