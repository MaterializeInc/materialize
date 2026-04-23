// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Consolidation of `(data, diff)` and `(data, time, diff)` collections.

use crate::difference::Semigroup;

pub fn consolidate<D: Ord, R: Semigroup>(vec: &mut Vec<(D, R)>) {
    let len = consolidate_slice(&mut vec[..]);
    vec.truncate(len);
}

pub fn consolidate_slice<D: Ord, R: Semigroup>(slice: &mut [(D, R)]) -> usize {
    if slice.len() <= 1 {
        return if slice.len() == 1 && slice[0].1.is_zero() { 0 } else { slice.len() };
    }
    slice.sort_by(|a, b| a.0.cmp(&b.0));
    let mut w = 0;
    for r in 1..slice.len() {
        if slice[w].0 == slice[r].0 {
            let d = slice[r].1.clone();
            slice[w].1.plus_equals(&d);
        } else {
            if !slice[w].1.is_zero() { w += 1; }
            slice.swap(w, r);
        }
    }
    if !slice[w].1.is_zero() { w += 1; }
    w
}

pub fn consolidate_updates<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>) {
    let len = consolidate_updates_slice(&mut vec[..]);
    vec.truncate(len);
}

pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {
    if slice.len() <= 1 {
        return if slice.len() == 1 && slice[0].2.is_zero() { 0 } else { slice.len() };
    }
    slice.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let mut w = 0;
    for r in 1..slice.len() {
        if slice[w].0 == slice[r].0 && slice[w].1 == slice[r].1 {
            let d = slice[r].2.clone();
            slice[w].2.plus_equals(&d);
        } else {
            if !slice[w].2.is_zero() { w += 1; }
            slice.swap(w, r);
        }
    }
    if !slice[w].2.is_zero() { w += 1; }
    w
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn consolidate_sums_per_key(v in prop::collection::vec((any::<u16>(), -4i64..=4), 0..40)) {
            let mut expected: BTreeMap<u16, i64> = BTreeMap::new();
            for (k, d) in &v { *expected.entry(*k).or_insert(0) += d; }
            expected.retain(|_, v| *v != 0);

            let mut vv = v.clone();
            consolidate(&mut vv);

            // No zero diffs in output.
            for (_, d) in &vv { prop_assert_ne!(*d, 0); }
            // Sorted and unique keys.
            for window in vv.windows(2) {
                prop_assert!(window[0].0 < window[1].0);
            }
            // Same aggregated content.
            let got: BTreeMap<u16, i64> = vv.into_iter().collect();
            prop_assert_eq!(got, expected);
        }

        #[test]
        fn consolidate_is_idempotent(v in prop::collection::vec((any::<u16>(), -4i64..=4), 0..40)) {
            let mut once = v.clone();
            consolidate(&mut once);
            let mut twice = once.clone();
            consolidate(&mut twice);
            prop_assert_eq!(once, twice);
        }

        #[test]
        fn consolidate_updates_sums_per_data_time(
            v in prop::collection::vec(((any::<u16>(), any::<u16>()), -4i64..=4), 0..40)
        ) {
            let flat: Vec<(u16, u16, i64)> =
                v.iter().map(|((d, t), r)| (*d, *t, *r)).collect();

            let mut expected: BTreeMap<(u16, u16), i64> = BTreeMap::new();
            for (d, t, r) in &flat { *expected.entry((*d, *t)).or_insert(0) += r; }
            expected.retain(|_, v| *v != 0);

            let mut vv = flat.clone();
            consolidate_updates(&mut vv);

            for (_, _, r) in &vv { prop_assert_ne!(*r, 0); }
            for window in vv.windows(2) {
                prop_assert!((window[0].0, window[0].1) < (window[1].0, window[1].1));
            }
            let got: BTreeMap<(u16, u16), i64> =
                vv.into_iter().map(|(d, t, r)| ((d, t), r)).collect();
            prop_assert_eq!(got, expected);
        }
    }
}
