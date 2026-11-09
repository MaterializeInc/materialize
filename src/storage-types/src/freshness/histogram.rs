// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exponential staleness buckets.

use std::time::Duration;

use mz_ore::cast::CastLossy;

/// Exponential staleness buckets with a power-of-two count per octave.
///
/// Bucket `k` covers staleness in `(2^((k-1)/b), 2^(k/b)]` ms for `b` buckets per octave.
/// Staleness of at most 1 ms is bucket 0.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BucketScheme {
    per_octave: u32,
}

impl BucketScheme {
    /// `None` unless `per_octave` is a power of two in `1..=256`.
    pub fn new(per_octave: u32) -> Option<Self> {
        (per_octave.is_power_of_two() && per_octave <= 256).then_some(BucketScheme { per_octave })
    }

    pub fn per_octave(&self) -> u32 {
        self.per_octave
    }

    /// The bucket of a staleness, rounded up to whole milliseconds.
    pub fn bucket(&self, staleness: Duration) -> i32 {
        let mut staleness_ms = u64::try_from(staleness.as_millis()).unwrap_or(u64::MAX);
        if staleness.subsec_nanos() % 1_000_000 != 0 {
            staleness_ms = staleness_ms.saturating_add(1);
        }
        self.bucket_ms(staleness_ms)
    }

    fn bucket_ms(&self, staleness_ms: u64) -> i32 {
        if staleness_ms <= 1 {
            return 0;
        }
        // The octave `e` with `2^(e-1) < staleness <= 2^e` is exact in integers, and bounds the
        // bucket to `((e-1) * b, e * b]`.
        let e = i32::try_from(u64::BITS - (staleness_ms - 1).leading_zeros()).expect("at most 64");
        let b = i32::try_from(self.per_octave).expect("at most 256");
        let (mut lo, mut hi) = ((e - 1) * b + 1, e * b);
        // Binary search for the first bucket whose largest staleness reaches `staleness_ms`.
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.max_staleness_ms(mid) >= staleness_ms {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo
    }

    /// The upper bound `2^(k/b)` ms of bucket `k`, inclusive.
    pub fn upper_bound_ms(&self, k: i32) -> f64 {
        (f64::from(k) / f64::from(self.per_octave)).exp2()
    }

    /// The largest integer staleness in ms in bucket `k`, 0 for buckets below 0.
    fn max_staleness_ms(&self, k: i32) -> u64 {
        if k < 0 {
            return 0;
        }
        let b = i32::try_from(self.per_octave).expect("at most 256");
        // NOTE: A float `exp2` can land a hair off at exact powers of two, which are the
        // bucket bounds that integers actually hit, so those take the integer path.
        if k % b == 0 {
            let e = u32::try_from(k / b).expect("non-negative");
            return 1u64.checked_shl(e).unwrap_or(u64::MAX);
        }
        // Saturates at `u64::MAX` for bounds beyond the integer range.
        u64::cast_lossy(self.upper_bound_ms(k).floor())
    }
}
