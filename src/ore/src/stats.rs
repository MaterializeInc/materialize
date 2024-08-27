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

/// Keeps track of the minimum and maximum value over a fixed-size sliding window of samples.
///
/// Inspired by the [`moving_min_max`] crate, see that crate's documentation for a description of
/// the high-level algorithm used here.
///
/// There are two major differences to [`moving_min_max`]:
///  * `SlidingMinMax` tracks both the minimum and the maximum value at the same time.
///  * `SlidingMinMax` assumes a fixed-size window. Pushing new samples automatically pops old ones
///    and there is no support for manually popping samples.
///
/// The memory required for a `SlidingMinMax` value is `size_of::<T> * 3 * window_size`, plus a
/// small constant overhead.
///
/// [`moving_min_max`]: https://crates.io/crates/moving_min_max
#[derive(Debug)]
pub struct SlidingMinMax<T> {
    /// The push stack and the pop stack, merged into one allocation to optimize memory usage.
    ///
    /// The push stack is the first `push_stack_len` items, the pop stack is the rest.
    /// The push stack grows to the right, the pop stack grows to the left.
    ///
    /// +--------------------------------+
    /// | push stack --> | <-- pop stack |
    /// +----------------^---------------^
    ///           push_stack_len
    ///
    ///  New samples are pushed to the push stack, together with the current minimum and maximum
    ///  values. If the pop stack is not empty, each push implicitly pops an element from the pop
    ///  stack, by increasing `push_stack_len`. Once the push stack has reached the window size
    ///  (i.e. the capacity of `stacks`), we "flip" the stacks by converting the push stack into a
    ///  full pop stack with an inverted order of samples and min/max values. After the flip,
    ///  `push_stack_len` is zero again, and new samples can be pushed to the push stack.
    stacks: Vec<(T, T, T)>,
    /// The length of the push stack.
    ///
    /// The top of the push stack is `stacks[push_stack_len - 1]`.
    /// The top of the pop stack is `stacks[push_stack_len]`.
    push_stack_len: usize,
}

impl<T> SlidingMinMax<T>
where
    T: Clone + PartialOrd,
{
    /// Creates a new `SlidingMinMax` for the given window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            stacks: Vec::with_capacity(window_size),
            push_stack_len: 0,
        }
    }

    /// Returns a reference to the item at the top of the push stack.
    fn top_of_push_stack(&self) -> Option<&(T, T, T)> {
        self.push_stack_len.checked_sub(1).map(|i| &self.stacks[i])
    }

    /// Returns a reference to the item at the top of the pop stack.
    fn top_of_pop_stack(&self) -> Option<&(T, T, T)> {
        self.stacks.get(self.push_stack_len)
    }

    /// Adds the given sample.
    pub fn add_sample(&mut self, sample: T) {
        if self.push_stack_len == self.stacks.capacity() {
            self.flip_stacks();
        }

        let (min, max) = match self.top_of_push_stack() {
            Some((_, min, max)) => {
                let min = po_min(min, &sample).clone();
                let max = po_max(max, &sample).clone();
                (min, max)
            }
            None => (sample.clone(), sample.clone()),
        };

        if self.stacks.len() <= self.push_stack_len {
            self.stacks.push((sample, min, max));
        } else {
            self.stacks[self.push_stack_len] = (sample, min, max);
        }
        self.push_stack_len += 1;
    }

    /// Drains the push stack into the pop stack.
    fn flip_stacks(&mut self) {
        let Some((sample, _, _)) = self.top_of_push_stack().cloned() else {
            return;
        };

        self.push_stack_len -= 1;
        self.stacks[self.push_stack_len] = (sample.clone(), sample.clone(), sample);

        while let Some((sample, _, _)) = self.top_of_push_stack() {
            let (_, min, max) = self.top_of_pop_stack().expect("pop stack not empty");
            let sample = sample.clone();
            let min = po_min(min, &sample).clone();
            let max = po_max(max, &sample).clone();

            self.push_stack_len -= 1;
            self.stacks[self.push_stack_len] = (sample, min, max);
        }
    }

    /// Returns the current minimum and maximum values.
    pub fn get(&self) -> Option<(&T, &T)> {
        match (self.top_of_push_stack(), self.top_of_pop_stack()) {
            (None, None) => None,
            (None, Some((_, min, max))) | (Some((_, min, max)), None) => Some((min, max)),
            (Some((_, min1, max1)), Some((_, min2, max2))) => {
                Some((po_min(min1, min2), po_max(max1, max2)))
            }
        }
    }
}

/// Like `std::cmp::min`, but works with `PartialOrd` values.
///
/// If `a` and `b` are not comparable, `b` is returned.
fn po_min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

/// Like `std::cmp::max`, but works with `PartialOrd` values.
///
/// If `a` and `b` are not comparable, `b` is returned.
fn po_max<T: PartialOrd>(a: T, b: T) -> T {
    if a > b {
        a
    } else {
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn minmax() {
        let mut minmax = SlidingMinMax::new(5);

        assert_eq!(minmax.get(), None);

        let mut push_and_check = |x, expected| {
            minmax.add_sample(x);
            let actual = minmax.get().map(|(min, max)| (*min, *max));
            assert_eq!(actual, Some(expected), "{minmax:?}");
        };

        push_and_check(5, (5, 5));
        push_and_check(1, (1, 5));
        push_and_check(10, (1, 10));
        push_and_check(2, (1, 10));
        push_and_check(9, (1, 10));
        push_and_check(3, (1, 10));
        push_and_check(8, (2, 10));
        push_and_check(5, (2, 9));
        push_and_check(5, (3, 9));
        push_and_check(5, (3, 8));
        push_and_check(5, (5, 8));
        push_and_check(5, (5, 5));
    }
}
