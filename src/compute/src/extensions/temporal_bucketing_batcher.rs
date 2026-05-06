// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A `Batcher` wrapper that coarsens seal calls onto power-of-two bucket
//! boundaries to reduce the number of small batches reaching the spine.
//!
//! The wrapper preserves all timestamps verbatim. Its only effect is to delay
//! `seal` calls so that they happen at bucket boundaries instead of at every
//! input frontier advance. The inner batcher continues to hold updates whose
//! time is at or beyond the most recent (coarsened) seal upper, exactly as it
//! would for any other batcher consumer.

use differential_dataflow::logging::Logger;
use differential_dataflow::trace::{Batcher, Builder};
use mz_timely_util::temporal::BucketTimestamp;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

/// A `Batcher` wrapper that aligns `seal` calls to power-of-two bucket
/// boundaries.
///
/// Given the most recent seal upper `last_lower`, an incoming `seal(upper)`
/// is coarsened to `last_lower + 2^k` for the largest `k` such that the
/// result is still `<= upper`. If no such `k` exists (i.e. `upper ==
/// last_lower`), the seal is forwarded unchanged and produces an empty batch.
///
/// Empty `upper` antichains (representing complete inputs) are forwarded
/// verbatim so that the inner batcher fully drains at shutdown.
pub struct TemporalBucketingBatcher<B: Batcher>
where
    B::Time: BucketTimestamp,
{
    inner: B,
    /// The upper of the most recent inner `seal`, or `[T::minimum()]` if no
    /// seal has happened yet. The next coarsened upper is computed relative
    /// to this antichain.
    last_lower: Antichain<B::Time>,
}

impl<B> Batcher for TemporalBucketingBatcher<B>
where
    B: Batcher,
    B::Time: BucketTimestamp,
{
    type Input = B::Input;
    type Output = B::Output;
    type Time = B::Time;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            inner: B::new(logger, operator_id),
            last_lower: Antichain::from_elem(B::Time::minimum()),
        }
    }

    fn push_container(&mut self, batch: &mut Self::Input) {
        self.inner.push_container(batch);
    }

    fn seal<Bu: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> Bu::Output {
        let coarse = bucket_align(&self.last_lower, &upper);
        let result = self.inner.seal::<Bu>(coarse.clone());
        self.last_lower = coarse;
        result
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.frontier()
    }
}

/// Compute the coarsened upper for a `seal` request.
///
/// The coarsened upper is the largest power-of-two advance from `last_lower`
/// that does not exceed `upper`. Falls back to `upper` itself when:
///
///   * `upper` is empty (input complete; drain everything),
///   * `last_lower` is empty (already drained),
///   * either antichain has more than one element (non-totally-ordered
///     timestamp; bucketing is undefined),
///   * `upper == last_lower` (no progress to coarsen),
///   * the smallest power-of-two advance from `last_lower` already equals or
///     exceeds `upper` (no slack to coarsen).
fn bucket_align<T: BucketTimestamp>(
    last_lower: &Antichain<T>,
    upper: &Antichain<T>,
) -> Antichain<T> {
    if upper.is_empty() {
        return upper.clone();
    }
    // Multi-element antichains (or empty `last_lower`) have no total-order
    // interpretation we can bucket against. Forward the request unchanged.
    let (lower_t, upper_t) = match (last_lower.elements(), upper.elements()) {
        ([l], [u]) => (l, u),
        _ => return upper.clone(),
    };
    if lower_t == upper_t {
        return upper.clone();
    }

    // Find the largest k such that lower_t + 2^k <= upper_t.
    let mut best: Option<T> = None;
    for k in 0..u32::try_from(T::DOMAIN).expect("domain fits in u32") {
        match lower_t.advance_by_power_of_two(k) {
            Some(candidate) if &candidate <= upper_t => best = Some(candidate),
            // Either overflow or candidate > upper_t: further k can only be
            // larger, so we can stop.
            _ => break,
        }
    }

    match best {
        Some(t) => Antichain::from_elem(t),
        None => upper.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use differential_dataflow::trace::implementations::chunker::ContainerChunker;
    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
    use differential_dataflow::trace::implementations::merge_batcher::container::VecMerger;
    use mz_repr::Timestamp as MzTs;

    /// A `Builder` that captures sealed updates into a flat `Vec` so tests can
    /// assert on what each `seal` released.
    struct CaptureBuilder<D, T, R> {
        captured: Vec<(D, T, R)>,
    }

    impl<D, T, R> Default for CaptureBuilder<D, T, R> {
        fn default() -> Self {
            Self {
                captured: Vec::new(),
            }
        }
    }

    impl<D: Clone + Ord + 'static, T: Timestamp + Clone, R: Clone + 'static> Builder
        for CaptureBuilder<D, T, R>
    {
        type Input = Vec<(D, T, R)>;
        type Time = T;
        type Output = Vec<(D, T, R)>;

        fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
            Self::default()
        }

        fn push(&mut self, chunk: &mut Self::Input) {
            self.captured.append(chunk);
        }

        fn done(
            self,
            _description: differential_dataflow::trace::Description<Self::Time>,
        ) -> Self::Output {
            self.captured
        }

        fn seal(
            chain: &mut Vec<Self::Input>,
            description: differential_dataflow::trace::Description<Self::Time>,
        ) -> Self::Output {
            let mut builder = Self::with_capacity(0, 0, 0);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }
            builder.done(description)
        }
    }

    type TestBatcher = MergeBatcher<
        Vec<(u64, MzTs, isize)>,
        ContainerChunker<Vec<(u64, MzTs, isize)>>,
        VecMerger<u64, MzTs, isize>,
    >;
    type Wrapper = TemporalBucketingBatcher<TestBatcher>;

    fn t(v: u64) -> MzTs {
        MzTs::from(v)
    }

    fn push(b: &mut Wrapper, updates: Vec<(u64, u64, isize)>) {
        let mut chunk: Vec<_> = updates
            .into_iter()
            .map(|(d, ts, r)| (d, t(ts), r))
            .collect();
        b.push_container(&mut chunk);
    }

    fn seal_to(b: &mut Wrapper, upper: u64) -> Vec<(u64, MzTs, isize)> {
        b.seal::<CaptureBuilder<u64, MzTs, isize>>(Antichain::from_elem(t(upper)))
    }

    #[mz_ore::test]
    fn align_no_progress_returns_lower() {
        let lower = Antichain::from_elem(t(7));
        let upper = Antichain::from_elem(t(7));
        let aligned = bucket_align(&lower, &upper);
        assert_eq!(aligned, upper);
    }

    #[mz_ore::test]
    fn align_one_step_returns_one_step() {
        // lower = 0, upper = 1: only candidate is 0 + 2^0 = 1. Coarse = 1.
        let lower = Antichain::from_elem(t(0));
        let upper = Antichain::from_elem(t(1));
        let aligned = bucket_align(&lower, &upper);
        assert_eq!(aligned, upper);
    }

    #[mz_ore::test]
    fn align_picks_largest_power_of_two() {
        // lower = 0, upper = 5: 2^0=1, 2^1=2, 2^2=4 (<=5). 2^3=8 (>5). Coarse = 4.
        let lower = Antichain::from_elem(t(0));
        let upper = Antichain::from_elem(t(5));
        let aligned = bucket_align(&lower, &upper);
        assert_eq!(aligned, Antichain::from_elem(t(4)));
    }

    #[mz_ore::test]
    fn align_relative_to_last_lower() {
        // lower = 4, upper = 13: deltas 1,2,4,8 from 4. 4+8=12 (<=13). Coarse = 12.
        let lower = Antichain::from_elem(t(4));
        let upper = Antichain::from_elem(t(13));
        let aligned = bucket_align(&lower, &upper);
        assert_eq!(aligned, Antichain::from_elem(t(12)));
    }

    #[mz_ore::test]
    fn align_empty_upper_passes_through() {
        let lower = Antichain::from_elem(t(7));
        let upper: Antichain<MzTs> = Antichain::new();
        let aligned = bucket_align(&lower, &upper);
        assert!(aligned.is_empty());
    }

    #[mz_ore::test]
    fn hold_and_release_far_future_data() {
        // Push an update at time 100. Repeatedly seal at small uppers; the
        // update should be held until the seal upper coarsens past 100.
        let mut b = Wrapper::new(None, 0);
        push(&mut b, vec![(42u64, 100u64, 1isize)]);

        // Seal at 1, 2, 4, ..., 64: all returned batches are empty, the
        // batcher's frontier reports the held update's time.
        let mut upper = 1u64;
        while upper <= 64 {
            let released = seal_to(&mut b, upper);
            assert!(
                released.is_empty(),
                "unexpected release at upper={upper}: {released:?}"
            );
            assert_eq!(
                b.frontier().to_owned(),
                Antichain::from_elem(t(100)),
                "frontier should reflect held update"
            );
            upper = upper.saturating_mul(2).max(upper + 1);
        }

        // Seal at 128: 100 < 128 so the update is released.
        let released = seal_to(&mut b, 128);
        assert_eq!(released, vec![(42u64, t(100), 1)]);
    }

    #[mz_ore::test]
    fn release_on_empty_upper() {
        let mut b = Wrapper::new(None, 0);
        push(&mut b, vec![(1u64, 50u64, 1), (2u64, 75u64, 1)]);
        // Seal at small upper: nothing released, both held.
        let released = seal_to(&mut b, 2);
        assert!(released.is_empty());
        // Drain at empty upper: everything released.
        let released = b.seal::<CaptureBuilder<u64, MzTs, isize>>(Antichain::new());
        assert_eq!(released.len(), 2);
    }

    #[mz_ore::test]
    fn preserves_distinct_timestamps() {
        // Two updates at distinct nearby times must remain distinct after
        // bucket-coarsened seal.
        let mut b = Wrapper::new(None, 0);
        push(&mut b, vec![(1u64, 10u64, 1), (1u64, 11u64, 1)]);
        // Seal at 64: coarse = 0 + 64 = 64. Both updates have time < 64, so
        // both released and remain distinct.
        let released = seal_to(&mut b, 64);
        let mut times: Vec<_> = released.iter().map(|(_, ts, _)| *ts).collect();
        times.sort();
        assert_eq!(times, vec![t(10), t(11)]);
    }
}
