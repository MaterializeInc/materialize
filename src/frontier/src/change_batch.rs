// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! A batch of `(T, i64)` updates that compacts on access.

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Ord + Serialize", deserialize = "T: Ord + Deserialize<'de>"))]
pub struct ChangeBatch<T> {
    updates: SmallVec<[(T, i64); 2]>,
    clean: usize,
}

impl<T> ChangeBatch<T> {
    pub fn new() -> Self { ChangeBatch { updates: SmallVec::new(), clean: 0 } }
    pub fn new_from(key: T, val: i64) -> Self where T: Ord {
        let mut b = Self::new(); b.update(key, val); b
    }
    pub fn with_capacity(cap: usize) -> Self {
        ChangeBatch { updates: SmallVec::with_capacity(cap), clean: 0 }
    }
    pub fn update(&mut self, item: T, value: i64) where T: Ord { self.updates.push((item, value)); }
    pub fn extend<I: IntoIterator<Item = (T, i64)>>(&mut self, iter: I) where T: Ord { self.updates.extend(iter); }
    pub fn is_empty(&mut self) -> bool where T: Ord { self.compact(); self.updates.is_empty() }
    pub fn is_dirty(&self) -> bool { self.clean < self.updates.len() }
    pub fn len(&mut self) -> usize where T: Ord { self.compact(); self.updates.len() }
    pub fn iter(&mut self) -> std::slice::Iter<'_, (T, i64)> where T: Ord { self.compact(); self.updates.iter() }
    pub fn drain(&mut self) -> smallvec::Drain<'_, [(T, i64); 2]> where T: Ord { self.compact(); self.clean = 0; self.updates.drain(..) }
    pub fn into_inner(mut self) -> SmallVec<[(T, i64); 2]> where T: Ord { self.compact(); self.updates }
    pub fn clear(&mut self) { self.updates.clear(); self.clean = 0; }
    pub fn drain_into(&mut self, other: &mut ChangeBatch<T>) where T: Ord + Clone { self.compact(); other.updates.extend(self.updates.drain(..)); self.clean = 0; }
    pub fn unstable_internal_updates(&self) -> &SmallVec<[(T, i64); 2]> { &self.updates }
    pub fn unstable_internal_clean(&self) -> usize { self.clean }

    fn compact(&mut self) where T: Ord {
        if self.clean < self.updates.len() && self.updates.len() > 1 {
            self.updates.sort_by(|a, b| a.0.cmp(&b.0));
            let mut w = 0;
            for r in 1..self.updates.len() {
                if self.updates[w].0 == self.updates[r].0 {
                    self.updates[w].1 += self.updates[r].1;
                } else {
                    if self.updates[w].1 != 0 { w += 1; }
                    self.updates.swap(w, r);
                }
            }
            if self.updates[w].1 != 0 { w += 1; }
            self.updates.truncate(w);
        } else if self.updates.len() == 1 && self.updates[0].1 == 0 {
            self.updates.clear();
        }
        self.clean = self.updates.len();
    }
}

impl<T> Default for ChangeBatch<T> { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn compact_sums_per_key(updates in prop::collection::vec((any::<u16>(), -4i64..=4), 0..30)) {
            let mut batch = ChangeBatch::new();
            for (t, d) in &updates {
                batch.update(*t, *d);
            }
            let mut expected: BTreeMap<u16, i64> = BTreeMap::new();
            for (t, d) in &updates {
                *expected.entry(*t).or_insert(0) += d;
            }
            expected.retain(|_, v| *v != 0);

            let compacted: Vec<(u16, i64)> = batch.drain().collect();
            let got: BTreeMap<u16, i64> = compacted.into_iter().collect();
            prop_assert_eq!(got, expected);
        }

        #[test]
        fn compact_drops_zero_sum(t in any::<u16>(), a in -4i64..=4, b in -4i64..=4) {
            let mut batch = ChangeBatch::new();
            batch.update(t, a);
            batch.update(t, b);
            let n = batch.len();
            if a + b == 0 {
                prop_assert_eq!(n, 0);
            } else {
                prop_assert_eq!(n, 1);
            }
        }

        #[test]
        fn drain_into_merges(a in prop::collection::vec((any::<u16>(), -4i64..=4), 0..15),
                             b in prop::collection::vec((any::<u16>(), -4i64..=4), 0..15)) {
            let mut left = ChangeBatch::new();
            for (t, d) in &a { left.update(*t, *d); }
            let mut right = ChangeBatch::new();
            for (t, d) in &b { right.update(*t, *d); }
            left.drain_into(&mut right);
            prop_assert!(left.is_empty());

            let mut expected: BTreeMap<u16, i64> = BTreeMap::new();
            for (t, d) in a.iter().chain(b.iter()) {
                *expected.entry(*t).or_insert(0) += d;
            }
            expected.retain(|_, v| *v != 0);
            let got: BTreeMap<u16, i64> = right.drain().collect();
            prop_assert_eq!(got, expected);
        }
    }
}
