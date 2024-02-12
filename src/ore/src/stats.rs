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

//! Statistics utilities.

/// A standard range of buckets for timing data, measured in seconds.
/// Individual histograms may only need a subset of this range, in which case,
/// see `histogram_seconds_buckets` below.
///
/// Note that any changes to this range may modify buckets for existing metrics.
const HISTOGRAM_SECOND_BUCKETS: [f64; 19] = [
    0.000_128, 0.000_256, 0.000_512, 0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256,
    0.512, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0,
];

/// Returns a `Vec` of time buckets that are both present in our standard
/// buckets above and within the provided inclusive range. (This makes it
/// more meaningful to compare latency percentiles across histograms if needed,
/// without requiring all metrics to use exactly the same buckets.)
pub fn histogram_seconds_buckets(from: f64, to: f64) -> Vec<f64> {
    let mut vec = Vec::with_capacity(HISTOGRAM_SECOND_BUCKETS.len());
    vec.extend(
        HISTOGRAM_SECOND_BUCKETS
            .iter()
            .copied()
            .filter(|&b| b >= from && b <= to),
    );
    vec
}

/// A standard range of buckets for timing data, measured in seconds.
/// Individual histograms may only need a subset of this range, in which case,
/// see `histogram_seconds_buckets` below.
///
/// Note that any changes to this range may modify buckets for existing metrics.
const HISTOGRAM_MILLISECOND_BUCKETS: [f64; 19] = [
    0.128, 0.256, 0.512, 1., 2., 4., 8., 16., 32., 64., 128., 256., 512., 1000., 2000., 4000.,
    8000., 16000., 32000.,
];

/// Returns a `Vec` of time buckets that are both present in our standard
/// buckets above and within the provided inclusive range. (This makes it
/// more meaningful to compare latency percentiles across histograms if needed,
/// without requiring all metrics to use exactly the same buckets.)
pub fn histogram_milliseconds_buckets(from_ms: f64, to_ms: f64) -> Vec<f64> {
    let mut vec = Vec::with_capacity(HISTOGRAM_MILLISECOND_BUCKETS.len());
    vec.extend(
        HISTOGRAM_MILLISECOND_BUCKETS
            .iter()
            .copied()
            .filter(|&b| b >= from_ms && b <= to_ms),
    );
    vec
}

/// Buckets that capture sizes of 64 bytes up to a gigabyte
pub const HISTOGRAM_BYTE_BUCKETS: [f64; 7] = [
    64.0,
    1024.0,
    16384.0,
    262144.0,
    4194304.0,
    67108864.0,
    1073741824.0,
];
