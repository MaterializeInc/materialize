// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::Timestamp;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct RefreshSchedule {
    // `REFRESH EVERY`s
    pub everies: Vec<RefreshEvery>,
    // `REFRESH AT`s
    pub ats: Vec<Timestamp>,
}

impl RefreshSchedule {
    /// Rounds up the timestamp to the time of the next refresh.
    /// Returns None if there is no next refresh.
    /// It saturates, i.e., if the next refresh would be larger than the maximum timestamp, then it
    /// returns the maximum timestamp.
    /// Note that this function is monotonic.
    pub fn round_up_timestamp(&self, timestamp: Timestamp) -> Option<Timestamp> {
        let everies = self.everies.iter();
        let next_everies = everies.map(|every| every.round_up_timestamp(timestamp));
        let next_ats = self.ats.iter().copied().filter(|at| *at >= timestamp);
        next_everies.chain(next_ats).min()
    }

    /// Rounds down `timestamp - 1` to the time of the previous refresh.
    /// Returns None if there is no previous refresh.
    /// It saturates, i.e., if the previous refresh would be smaller than the minimum timestamp,
    /// then it returns the minimum timestamp.
    /// Note that this fn is monotonic.
    pub fn round_down_timestamp_m1(&self, timestamp: Timestamp) -> Option<Timestamp> {
        let everies = self.everies.iter();
        let prev_everies = everies.map(|every| every.round_down_timestamp_m1(timestamp));
        // Note that we use `<` instead of `<=`. This is because we are rounding
        // `timestamp - 1`, and not simply `timestamp`.
        let prev_ats = self.ats.iter().copied().filter(|at| *at < timestamp);
        prev_everies.chain(prev_ats).max()
    }

    /// Returns the time of the last refresh. Returns None if there is no last refresh (e.g., for a
    /// periodic refresh).
    ///
    /// (If there is no last refresh, then we have a `REFRESH EVERY`, in which case the saturating
    /// roundup puts a refresh at the maximum possible timestamp. This means that it would make
    /// some sense to return the maximum possible timestamp instead of None. Indeed, some of our
    /// callers handle our None return value in exactly this way. However, some other callers do
    /// something else with None, and therefore we don't want to hardcode this behavior in this
    /// function.)
    pub fn last_refresh(&self) -> Option<Timestamp> {
        if self.everies.is_empty() {
            self.ats.iter().max().cloned()
        } else {
            None
        }
    }

    /// Returns whether the schedule is empty, i.e., no `EVERY` or `AT`.
    pub fn is_empty(&self) -> bool {
        self.everies.is_empty() && self.ats.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct RefreshEvery {
    pub interval: Duration,
    pub aligned_to: Timestamp,
}

impl RefreshEvery {
    /// Rounds up the timestamp to the time of the next refresh, according to the given periodic
    /// refresh schedule. It saturates, i.e., if the rounding would make it overflow, then it
    /// returns the maximum possible timestamp.
    ///
    /// # Panics
    /// - if the refresh interval converted to milliseconds cast to u64 overflows;
    /// - if the interval is 0.
    /// (These should be checked in HIR planning.)
    pub fn round_up_timestamp(&self, timestamp: Timestamp) -> Timestamp {
        let interval = u64::try_from(self.interval.as_millis()).unwrap();
        let aligned_to = u64::from(self.aligned_to);
        let timestamp = u64::from(timestamp);

        let result = if timestamp > aligned_to {
            let rounded = Self::round_up_to_multiple_of_interval(interval, timestamp - aligned_to);
            aligned_to.saturating_add(rounded)
        } else {
            // Note: `timestamp == aligned_to` has to be handled here, because in the other branch
            // `x - 1` in `round_up_to_multiple_of_interval` would underflow.
            //
            // Also, no need to check for overflows here, since all the numbers are either between
            // `timestamp` and `aligned_to`, or not greater than `aligned_to - timestamp`.
            aligned_to - Self::round_down_to_multiple_of_interval(interval, aligned_to - timestamp)
        };
        // TODO: Downgrade these to non-logging soft asserts when we have built more confidence in the code.
        assert!(result >= timestamp);
        assert!(result - timestamp < interval);
        Timestamp::new(result)
    }

    /// Rounds down `timestamp - 1` to the time of the previous refresh, according to the given
    /// periodic refresh schedule. It saturates, i.e., if the rounding would make it underflow, then
    /// it returns the minimum possible timestamp.
    ///
    /// # Panics
    /// - if the refresh interval converted to milliseconds cast to u64 overflows;
    /// - if the interval is 0.
    /// (These should be checked in HIR planning.)
    pub fn round_down_timestamp_m1(&self, timestamp: Timestamp) -> Timestamp {
        let interval = u64::try_from(self.interval.as_millis()).unwrap();
        let aligned_to = u64::from(self.aligned_to);
        let timestamp = u64::from(timestamp.saturating_sub(1));

        let result = if timestamp >= aligned_to {
            // Note: `timestamp == aligned_to` has to be handled here, because in the other branch
            // `x - 1` in `round_up_to_multiple_of_interval` would underflow.
            //
            // Also, No need to check for overflows here, since all the numbers are either between
            // `aligned_to` and `timestamp`, or not greater than `timestamp - aligned_to`.
            aligned_to + Self::round_down_to_multiple_of_interval(interval, timestamp - aligned_to)
        } else {
            let rounded = Self::round_up_to_multiple_of_interval(interval, aligned_to - timestamp);
            aligned_to.saturating_sub(rounded)
        };
        // TODO: Downgrade these to non-logging soft asserts when we have built more confidence in the code.
        assert!(result <= timestamp);
        assert!(timestamp - result < interval);
        Timestamp::new(result)
    }

    /// Rounds up `x` to the nearest multiple of `interval`.
    /// `x` must not be 0.
    ///
    /// It saturates, i.e., if the rounding would make it overflow, then it
    /// returns the maximum possible timestamp.
    fn round_up_to_multiple_of_interval(interval: u64, x: u64) -> u64 {
        assert_ne!(x, 0);
        (((x - 1) / interval) + 1).saturating_mul(interval)
    }

    /// Rounds down `x` to the nearest multiple of `interval`.
    fn round_down_to_multiple_of_interval(interval: u64, x: u64) -> u64 {
        x / interval * interval
    }
}

#[cfg(test)]
mod tests {
    use crate::Timestamp;
    use crate::adt::interval::Interval;
    use crate::refresh_schedule::{RefreshEvery, RefreshSchedule};
    use std::str::FromStr;

    #[mz_ore::test]
    fn test_round_up_down_timestamp() {
        let ts = |t: u64| Timestamp::new(t);
        let test = |schedule: RefreshSchedule| {
            move |expected_round_down_ts: Option<u64>,
                  expected_round_up_ts: Option<u64>,
                  input_ts| {
                assert_eq!(
                    expected_round_down_ts.map(ts),
                    schedule.round_down_timestamp_m1(ts(input_ts)),
                );
                assert_eq!(
                    expected_round_up_ts.map(ts),
                    schedule.round_up_timestamp(ts(input_ts))
                );
            }
        };
        {
            let schedule = RefreshSchedule {
                everies: vec![],
                ats: vec![ts(123), ts(456)],
            };
            let test = test(schedule);
            test(None, Some(123), 0);
            test(None, Some(123), 50);
            test(None, Some(123), 122);
            test(None, Some(123), 123);
            test(Some(123), Some(456), 124);
            test(Some(123), Some(456), 130);
            test(Some(123), Some(456), 455);
            test(Some(123), Some(456), 456);
            test(Some(456), None, 457);
            test(Some(456), None, 12345678);
            test(Some(456), None, u64::MAX - 1000);
            test(Some(456), None, u64::MAX - 1);
            test(Some(456), None, u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![RefreshEvery {
                    interval: Interval::from_str("100 milliseconds")
                        .unwrap()
                        .duration()
                        .unwrap(),
                    aligned_to: ts(500),
                }],
                ats: vec![],
            };
            let test = test(schedule);
            test(Some(0), Some(0), 0);
            test(Some(0), Some(100), 1);
            test(Some(0), Some(100), 2);
            test(Some(0), Some(100), 99);
            test(Some(0), Some(100), 100);
            test(Some(100), Some(200), 101);
            test(Some(100), Some(200), 102);
            test(Some(100), Some(200), 199);
            test(Some(100), Some(200), 200);
            test(Some(200), Some(300), 201);
            test(Some(300), Some(400), 400);
            test(Some(400), Some(500), 401);
            test(Some(400), Some(500), 450);
            test(Some(400), Some(500), 499);
            test(Some(400), Some(500), 500);
            test(Some(500), Some(600), 501);
            test(Some(500), Some(600), 599);
            test(Some(500), Some(600), 600);
            test(Some(600), Some(700), 601);
            test(Some(5434532500), Some(5434532600), 5434532599);
            test(Some(5434532500), Some(5434532600), 5434532600);
            test(Some(5434532600), Some(5434532700), 5434532601);
            test(Some(18446744073709551600), Some(u64::MAX), u64::MAX - 1);
            test(Some(18446744073709551600), Some(u64::MAX), u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![RefreshEvery {
                    interval: Interval::from_str("100 milliseconds")
                        .unwrap()
                        .duration()
                        .unwrap(),
                    aligned_to: ts(542),
                }],
                ats: vec![],
            };
            let test = test(schedule);
            test(Some(0), Some(42), 0);
            test(Some(0), Some(42), 1);
            test(Some(0), Some(42), 41);
            test(Some(0), Some(42), 42);
            test(Some(42), Some(142), 43);
            test(Some(342), Some(442), 441);
            test(Some(342), Some(442), 442);
            test(Some(442), Some(542), 443);
            test(Some(442), Some(542), 541);
            test(Some(442), Some(542), 542);
            test(Some(542), Some(642), 543);
            test(Some(18446744073709551542), Some(u64::MAX), u64::MAX - 1);
            test(Some(18446744073709551542), Some(u64::MAX), u64::MAX);
        }
        {
            let schedule = RefreshSchedule {
                everies: vec![
                    RefreshEvery {
                        interval: Interval::from_str("100 milliseconds")
                            .unwrap()
                            .duration()
                            .unwrap(),
                        aligned_to: ts(400),
                    },
                    RefreshEvery {
                        interval: Interval::from_str("100 milliseconds")
                            .unwrap()
                            .duration()
                            .unwrap(),
                        aligned_to: ts(542),
                    },
                ],
                ats: vec![ts(2), ts(300), ts(400), ts(471), ts(541), ts(123456)],
            };
            let test = test(schedule);
            test(Some(0), Some(0), 0);
            test(Some(0), Some(2), 1);
            test(Some(0), Some(2), 2);
            test(Some(2), Some(42), 3);
            test(Some(2), Some(42), 41);
            test(Some(2), Some(42), 42);
            test(Some(42), Some(100), 43);
            test(Some(42), Some(100), 99);
            test(Some(42), Some(100), 100);
            test(Some(100), Some(142), 101);
            test(Some(100), Some(142), 141);
            test(Some(100), Some(142), 142);
            test(Some(142), Some(200), 143);
            test(Some(242), Some(300), 243);
            test(Some(242), Some(300), 299);
            test(Some(242), Some(300), 300);
            test(Some(300), Some(342), 301);
            test(Some(342), Some(400), 343);
            test(Some(342), Some(400), 399);
            test(Some(342), Some(400), 400);
            test(Some(400), Some(442), 401);
            test(Some(400), Some(442), 441);
            test(Some(400), Some(442), 442);
            test(Some(442), Some(471), 443);
            test(Some(442), Some(471), 470);
            test(Some(442), Some(471), 471);
            test(Some(471), Some(500), 472);
            test(Some(471), Some(500), 480);
            test(Some(471), Some(500), 500);
            test(Some(500), Some(541), 501);
            test(Some(500), Some(541), 540);
            test(Some(500), Some(541), 541);
            test(Some(541), Some(542), 542);
            test(Some(542), Some(600), 543);
            test(Some(65442), Some(65500), 65454);
            test(Some(87800), Some(87842), 87831);
            test(Some(123400), Some(123442), 123442);
            test(Some(123442), Some(123456), 123443);
            test(Some(123442), Some(123456), 123456);
            test(Some(123456), Some(123500), 123457);
            test(Some(18446744073709551600), Some(u64::MAX), u64::MAX - 1);
            test(Some(18446744073709551600), Some(u64::MAX), u64::MAX);
        }
    }
}
