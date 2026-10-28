// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions and types.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mz_ore::cast::CastLossy;

/// Specifies a timeout for a Kafka operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Timeout {
    /// Time out after the specified duration elapses.
    After(Duration),
    /// Block forever.
    Never,
}

impl std::ops::SubAssign for Timeout {
    /// Subtracts `other` from `self`.
    ///
    /// `Never - After(_)` is `Never`. Panics if `other` is `Never` or if the
    /// subtraction underflows, as in rust-rdkafka.
    fn sub_assign(&mut self, other: Self) {
        match (self, other) {
            (Timeout::After(lhs), Timeout::After(rhs)) => *lhs -= rhs,
            (Timeout::Never, Timeout::After(_)) => (),
            _ => panic!("subtraction of Timeout::Never is ill-defined"),
        }
    }
}

impl From<Duration> for Timeout {
    fn from(d: Duration) -> Timeout {
        Timeout::After(d)
    }
}

impl From<Option<Duration>> for Timeout {
    fn from(v: Option<Duration>) -> Timeout {
        match v {
            None => Timeout::Never,
            Some(d) => Timeout::After(d),
        }
    }
}

/// Converts the given time to the number of milliseconds since the Unix epoch.
///
/// Times before the epoch map to 0.
pub fn millis_to_epoch(time: SystemTime) -> i64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Returns the current time in milliseconds since the Unix epoch.
pub fn current_time_millis() -> i64 {
    millis_to_epoch(SystemTime::now())
}

/// A value that can be attached to a produced message and handed back in its
/// delivery report.
///
/// NOTE: rust-rdkafka's trait converts to and from raw pointers. This one is
/// a marker trait: the producer keeps opaques by value next to their queued
/// records and reports them from its I/O tasks, which requires `Send` and
/// `'static`.
pub trait IntoOpaque: Send + Sync + Sized + 'static {}

impl IntoOpaque for () {}

impl IntoOpaque for usize {}

impl<T: Send + Sync + 'static> IntoOpaque for Box<T> {}

impl<T: Send + Sync + 'static> IntoOpaque for Arc<T> {}

/// An absolute point in time derived from a [`Timeout`], for operations that
/// make several waits against one overall timeout.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Deadline(Option<Instant>);

impl Deadline {
    /// Starts the deadline now. A timeout too large to represent is treated as
    /// [`Timeout::Never`].
    pub(crate) fn new(timeout: impl Into<Timeout>) -> Deadline {
        match timeout.into() {
            Timeout::After(d) => Deadline(Instant::now().checked_add(d)),
            Timeout::Never => Deadline(None),
        }
    }

    /// A deadline that never elapses.
    pub(crate) fn never() -> Deadline {
        Deadline(None)
    }

    /// The instant at which the deadline elapses, or `None` for no deadline.
    pub(crate) fn instant(&self) -> Option<Instant> {
        self.0
    }

    /// Time left until the deadline, zero once it has elapsed.
    pub(crate) fn remaining(&self) -> Timeout {
        match self.0 {
            Some(at) => Timeout::After(at.saturating_duration_since(Instant::now())),
            None => Timeout::Never,
        }
    }

    /// Time left until the deadline, or `None` for no deadline.
    pub(crate) fn remaining_duration(&self) -> Option<Duration> {
        self.0
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    /// Reports whether the deadline has passed.
    pub(crate) fn is_elapsed(&self) -> bool {
        self.0.is_some_and(|at| Instant::now() >= at)
    }

    /// Returns the earlier of the two deadlines.
    pub(crate) fn min(self, other: Deadline) -> Deadline {
        match (self.0, other.0) {
            (Some(a), Some(b)) => Deadline(Some(a.min(b))),
            (Some(a), None) | (None, Some(a)) => Deadline(Some(a)),
            (None, None) => Deadline(None),
        }
    }

    /// Remaining time in whole milliseconds clamped to `i32`, as used by Kafka
    /// request fields. `-1` for no deadline, librdkafka's "infinite".
    pub(crate) fn remaining_millis_i32(&self) -> i32 {
        match self.remaining_duration() {
            Some(d) => i32::try_from(d.as_millis()).unwrap_or(i32::MAX),
            None => -1,
        }
    }
}

/// Converts a millisecond count to a [`Duration`], treating negative values as
/// zero.
pub(crate) fn duration_from_millis(millis: i64) -> Duration {
    Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// Converts a fractional millisecond count (librdkafka float properties such
/// as `linger.ms`) to a [`Duration`].
pub(crate) fn duration_from_millis_f64(millis: f64) -> Duration {
    if millis.is_nan() || millis <= 0.0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(u64::cast_lossy((millis * 1_000_000.0).round()))
}

/// Converts a [`Duration`] to fractional milliseconds.
pub(crate) fn duration_to_millis_f64(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn timeout_conversions() {
        assert_eq!(
            Timeout::from(Duration::from_secs(1)),
            Timeout::After(Duration::from_secs(1))
        );
        assert_eq!(Timeout::from(None::<Duration>), Timeout::Never);
        assert_eq!(
            Timeout::from(Some(Duration::from_millis(5))),
            Timeout::After(Duration::from_millis(5))
        );
        assert!(Timeout::After(Duration::MAX) < Timeout::Never);

        let mut t = Timeout::After(Duration::from_secs(3));
        t -= Timeout::After(Duration::from_secs(1));
        assert_eq!(t, Timeout::After(Duration::from_secs(2)));
        let mut t = Timeout::Never;
        t -= Timeout::After(Duration::from_secs(1));
        assert_eq!(t, Timeout::Never);
    }

    #[mz_ore::test]
    #[should_panic(expected = "subtraction of Timeout::Never is ill-defined")]
    fn timeout_sub_never_panics() {
        let mut t = Timeout::After(Duration::from_secs(1));
        t -= Timeout::Never;
    }

    #[mz_ore::test]
    fn deadline() {
        let never = Deadline::new(Timeout::Never);
        assert_eq!(never.remaining(), Timeout::Never);
        assert_eq!(never.remaining_millis_i32(), -1);
        assert!(!never.is_elapsed());

        let zero = Deadline::new(Duration::ZERO);
        assert!(zero.is_elapsed());
        assert_eq!(zero.remaining(), Timeout::After(Duration::ZERO));
        assert_eq!(zero.remaining_millis_i32(), 0);

        let far = Deadline::new(Duration::from_secs(3600));
        assert!(!far.is_elapsed());
        assert!(far.remaining_duration().unwrap() > Duration::from_secs(3500));
        assert_eq!(far.min(zero), zero);
        assert_eq!(never.min(far), far);

        // Overflowing timeouts degrade to "never".
        assert_eq!(Deadline::new(Duration::MAX), Deadline::never());
    }

    #[mz_ore::test]
    fn millis_helpers() {
        assert_eq!(millis_to_epoch(UNIX_EPOCH), 0);
        assert_eq!(
            millis_to_epoch(UNIX_EPOCH + Duration::from_millis(1234)),
            1234
        );
        assert_eq!(millis_to_epoch(UNIX_EPOCH - Duration::from_secs(1)), 0);
        assert_eq!(duration_from_millis(-5), Duration::ZERO);
        assert_eq!(duration_from_millis(5), Duration::from_millis(5));
        assert_eq!(duration_from_millis_f64(2.5), Duration::from_micros(2500));
        assert_eq!(duration_from_millis_f64(-1.0), Duration::ZERO);
        assert_eq!(duration_to_millis_f64(Duration::from_micros(2500)), 2.5);
    }
}
