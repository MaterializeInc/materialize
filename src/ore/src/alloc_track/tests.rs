// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::alloc_track::{Sampler, filter, sample_scale};
use crate::cast::{CastFrom, CastLossy};

/// Weighted sample estimates converge on the true totals, for allocation
/// sizes far below, near, and far above the interval.
#[crate::test]
#[cfg_attr(miri, ignore)] // too slow
fn sampler_estimates_are_unbiased() {
    const INTERVAL: u64 = 1 << 19;
    for size in [64usize, 4096, 1 << 19, 8 << 20] {
        let mut sampler = Sampler::seeded(u64::cast_from(size));
        let total_bytes = 1u64 << 33;
        let n = total_bytes / u64::cast_from(size);
        let mut est_count = 0.0;
        for _ in 0..n {
            if sampler.step(size, INTERVAL) {
                est_count += sample_scale(size, INTERVAL);
            }
        }
        let err = (est_count - f64::cast_lossy(n)).abs() / f64::cast_lossy(n);
        assert!(err < 0.03, "size {size}: estimated {est_count}, actual {n}");
    }
}

#[crate::test]
fn sample_scale_limits() {
    assert!((sample_scale(1 << 30, 1 << 19) - 1.0).abs() < 1e-9);
    let small = sample_scale(8, 1 << 19);
    assert!((small - f64::cast_lossy(1u64 << 19) / 8.0).abs() / small < 1e-3);
}

#[crate::test]
fn filter_counts_samples_per_page() {
    // Real allocations sharing these pages would only see filter false
    // positives, which the registry tolerates.
    let base = (1usize << 48) - (1 << 28);
    let a = base + 16;
    let b = base + 2048;
    let c = base + 4096;
    assert!(!filter::maybe_contains(a));
    assert!(filter::insert(a));
    assert!(filter::insert(b));
    assert!(filter::maybe_contains(a));
    assert!(filter::maybe_contains(b), "same page as a");
    assert!(!filter::maybe_contains(c), "next page");
    filter::remove(a);
    assert!(filter::maybe_contains(b), "b is still live");
    filter::remove(b);
    assert!(!filter::maybe_contains(a));
    // Addresses beyond the filtered range always answer "maybe".
    assert!(filter::maybe_contains(1 << 50));
}
