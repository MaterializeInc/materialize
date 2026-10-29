// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exponential latency buckets and the accounting of newly visible arrivals into them.

use std::collections::BTreeMap;

use mz_ore::cast::CastLossy;

use super::ResolvedTree;

/// Exponential latency buckets with a power-of-two count per octave.
///
/// Bucket `k` covers latencies in `(2^((k-1)/b), 2^(k/b)]` ms for `b` buckets per octave.
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

    /// The bucket of an integer latency in ms. Latency 1 ms is bucket 0.
    pub fn bucket(&self, latency_ms: u64) -> i32 {
        if latency_ms <= 1 {
            return 0;
        }
        // The octave `e` with `2^(e-1) < latency <= 2^e` is exact in integers, and bounds the
        // bucket to `((e-1) * b, e * b]`.
        let e = i32::try_from(u64::BITS - (latency_ms - 1).leading_zeros()).expect("at most 64");
        let b = i32::try_from(self.per_octave).expect("at most 256");
        let (mut lo, mut hi) = ((e - 1) * b + 1, e * b);
        // Binary search for the first bucket whose largest latency reaches `latency_ms`.
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.max_latency_ms(mid) >= latency_ms {
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

    /// The largest integer latency in bucket `k`, 0 for buckets below 0.
    fn max_latency_ms(&self, k: i32) -> u64 {
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

/// Adds the virtual arrivals in `(from, to]` that became visible at `advance_ms` to `counts`,
/// keyed by bucket. Arrivals sit at multiples of `interval_ms`.
///
/// `from` and `to` must be window boundaries of `tree`, as returned by `last_reflected`. The
/// work is bounded by the number of touched buckets and independent of the span length.
pub fn add_span(
    tree: &ResolvedTree,
    scheme: &BucketScheme,
    interval_ms: u64,
    from: u64,
    to: u64,
    advance_ms: u64,
    counts: &mut BTreeMap<i32, u64>,
) {
    add_span_with(
        &|z| tree.next_boundary(z),
        scheme,
        interval_ms,
        from,
        to,
        advance_ms,
        counts,
    )
}

/// `add_span` with the next-boundary function `beta` as a parameter, so tests can count calls.
pub(super) fn add_span_with(
    beta: &dyn Fn(u64) -> u64,
    scheme: &BucketScheme,
    interval_ms: u64,
    from: u64,
    to: u64,
    advance_ms: u64,
    counts: &mut BTreeMap<i32, u64>,
) {
    if from >= to || interval_ms == 0 {
        return;
    }
    // NOTE: A frontier can be reported before the wall clock reaches the span end, for example
    // under clock skew between the writer and the controller. Latency is then measured from
    // the span end, which keeps every latency at least the length of its window.
    add_window_latencies(
        beta,
        scheme,
        interval_ms,
        from,
        to,
        advance_ms.max(to),
        counts,
    );
}

/// Adds the virtual arrivals in `(from, to]` to `counts`, each with the latency `x - s` for
/// the start `s` of its window, and a latency of at most 1 ms, also a negative one, in the
/// bucket of 1 ms.
///
/// `from` and `to` must be window boundaries under `beta`. The work is bounded by the number of
/// touched buckets.
pub(super) fn add_window_latencies(
    beta: &dyn Fn(u64) -> u64,
    scheme: &BucketScheme,
    interval_ms: u64,
    from: u64,
    to: u64,
    x: u64,
    counts: &mut BTreeMap<i32, u64>,
) {
    if from >= to || interval_ms == 0 {
        return;
    }
    // A window starting at boundary `s` in `[from, to)` has latency `x - s`.
    let shortest = x.saturating_sub(to - 1).max(1);
    let longest = x.saturating_sub(from).max(1);
    for k in scheme.bucket(shortest)..=scheme.bucket(longest) {
        // The windows with latency in this bucket start in `[x - hi, x - lo)`, so their
        // arrivals are `(beta(x - hi), beta(x - lo)]`. The lowest bucket also takes the
        // windows that start at or after `x`.
        let lo = scheme.max_latency_ms(k - 1);
        let hi = scheme.max_latency_ms(k);
        let start = beta(x.saturating_sub(hi)).max(from);
        let end = match k {
            0 => to,
            _ => beta(x.saturating_sub(lo)).min(to),
        };
        if start < end {
            let n = end / interval_ms - start / interval_ms;
            if n > 0 {
                *counts.entry(k).or_default() += n;
            }
        }
    }
}
