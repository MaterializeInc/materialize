// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Antichain data structures for totally-ordered time domains.
//!
//! Because the control plane only works with totally-ordered timestamps,
//! an antichain is always either empty or a single element. Internally
//! this is just `Option<T>`.

use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// ---------------------------------------------------------------------------
// Antichain
// ---------------------------------------------------------------------------

/// A frontier in a totally-ordered time domain.
///
/// Contains at most one element. An empty antichain represents a "closed"
/// frontier (no future timestamps are possible).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
pub struct Antichain<T> {
    element: Option<T>,
}

impl<T> Antichain<T> {
    /// Creates an empty antichain (closed frontier).
    pub fn new() -> Self {
        Antichain { element: None }
    }

    /// Creates an antichain containing a single element.
    pub fn from_elem(element: T) -> Self {
        Antichain {
            element: Some(element),
        }
    }

    /// Returns the contained elements as a slice (0 or 1 elements).
    pub fn elements(&self) -> &[T] {
        match &self.element {
            Some(t) => std::slice::from_ref(t),
            None => &[],
        }
    }

    /// Iterates over the contained elements (0 or 1).
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.elements().iter()
    }

    /// Borrows the antichain as an [`AntichainRef`].
    pub fn borrow(&self) -> AntichainRef<'_, T> {
        AntichainRef::new(self.elements())
    }

    /// Returns `true` if the frontier is empty (closed).
    pub fn is_empty(&self) -> bool {
        self.element.is_none()
    }

    /// Clears the antichain.
    pub fn clear(&mut self) {
        self.element = None;
    }

    /// No-op (provided for API compatibility with timely's Antichain).
    pub fn sort(&mut self)
    where
        T: Ord,
    {
    }

    /// No-op (provided for API compatibility).
    pub fn reserve(&mut self, _additional: usize) {}
}

impl<T: Ord> Antichain<T> {
    /// Inserts `element` into the antichain.
    ///
    /// For a totally-ordered type, this keeps the minimum of the existing
    /// element and the new one. Returns `true` if the element was actually
    /// added (i.e., it was smaller than the existing element, or the
    /// antichain was empty).
    pub fn insert(&mut self, element: T) -> bool {
        match &self.element {
            Some(existing) if *existing <= element => false,
            _ => {
                self.element = Some(element);
                true
            }
        }
    }

    /// Inserts by cloning `element`.
    pub fn insert_ref(&mut self, element: &T) -> bool
    where
        T: Clone,
    {
        match &self.element {
            Some(existing) if *existing <= *element => false,
            _ => {
                self.element = Some(element.clone());
                true
            }
        }
    }

    /// Returns `true` if the frontier element is strictly less than `time`.
    pub fn less_than(&self, time: &T) -> bool {
        self.element.as_ref().is_some_and(|e| *e < *time)
    }

    /// Returns `true` if the frontier element is less than or equal to `time`.
    pub fn less_equal(&self, time: &T) -> bool {
        self.element.as_ref().is_some_and(|e| *e <= *time)
    }

    /// Extends the antichain with multiple elements (keeps the minimum).
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for element in iter {
            self.insert(element);
        }
    }
}

impl<T: Ord + Clone> Antichain<T> {
    /// Lattice join. For a totally-ordered type an empty antichain is the top
    /// of the lattice, so join with an empty antichain yields empty.
    pub fn join(&self, other: &Self) -> Self {
        match (&self.element, &other.element) {
            (None, _) | (_, None) => Antichain::new(),
            (Some(a), Some(b)) => Antichain::from_elem(a.clone().max(b.clone())),
        }
    }

    /// In-place lattice join.
    pub fn join_assign(&mut self, other: &Self) {
        match (&self.element, &other.element) {
            (None, _) => {}
            (Some(_), None) => self.element = None,
            (Some(a), Some(b)) if b > a => self.element = Some(b.clone()),
            _ => {}
        }
    }

    /// Lattice meet. An empty antichain acts as the identity for meet
    /// (contributes no elements).
    pub fn meet(&self, other: &Self) -> Self {
        match (&self.element, &other.element) {
            (None, None) => Antichain::new(),
            (Some(a), None) => Antichain::from_elem(a.clone()),
            (None, Some(b)) => Antichain::from_elem(b.clone()),
            (Some(a), Some(b)) => Antichain::from_elem(std::cmp::min(a, b).clone()),
        }
    }

    /// In-place lattice meet.
    pub fn meet_assign(&mut self, other: &Self) {
        match (&self.element, &other.element) {
            (_, None) => {}
            (None, Some(b)) => self.element = Some(b.clone()),
            (Some(a), Some(b)) if b < a => self.element = Some(b.clone()),
            _ => {}
        }
    }
}

impl<T> Antichain<T> {
    /// Returns the single element, consuming the antichain.
    ///
    /// # Panics
    ///
    /// Panics if the antichain is empty.
    pub fn into_element(self) -> T {
        self.element.expect("expected exactly one element")
    }

    /// Returns `Some(&T)` if there is exactly one element, `None` if empty.
    pub fn as_option(&self) -> Option<&T> {
        self.element.as_ref()
    }

    /// Returns the inner `Option<T>`.
    pub fn into_option(self) -> Option<T> {
        self.element
    }
}

impl<T> Default for Antichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PartialEq> PartialEq for Antichain<T> {
    fn eq(&self, other: &Self) -> bool {
        self.element == other.element
    }
}

impl<T: Eq> Eq for Antichain<T> {}

impl<T: Hash> Hash for Antichain<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.element.hash(state);
    }
}

impl<T: Ord> PartialOrd for Antichain<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for Antichain<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.element.cmp(&other.element)
    }
}

impl<T> IntoIterator for Antichain<T> {
    type Item = T;
    type IntoIter = std::option::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.element.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Antichain<T> {
    type Item = &'a T;
    type IntoIter = std::option::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.element.iter()
    }
}

impl<T: Ord> FromIterator<T> for Antichain<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut antichain = Antichain::new();
        for element in iter {
            antichain.insert(element);
        }
        antichain
    }
}

impl<T: Ord> From<Vec<T>> for Antichain<T> {
    fn from(vec: Vec<T>) -> Self {
        vec.into_iter().collect()
    }
}

impl<T: fmt::Display> fmt::Display for Antichain<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.borrow().fmt(f)
    }
}

// ---------------------------------------------------------------------------
// AntichainRef
// ---------------------------------------------------------------------------

/// A borrowed reference to an antichain (a slice of 0 or 1 elements).
pub struct AntichainRef<'a, T> {
    frontier: &'a [T],
}

impl<'a, T> Clone for AntichainRef<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T> Copy for AntichainRef<'a, T> {}

impl<'a, T> AntichainRef<'a, T> {
    pub fn new(frontier: &'a [T]) -> Self {
        AntichainRef { frontier }
    }

    pub fn to_owned(&self) -> Antichain<T>
    where
        T: Clone,
    {
        Antichain {
            element: self.frontier.first().cloned(),
        }
    }
}

impl<'a, T: Ord> AntichainRef<'a, T> {
    pub fn less_than(&self, time: &T) -> bool {
        self.frontier.first().is_some_and(|e| *e < *time)
    }
    pub fn less_equal(&self, time: &T) -> bool {
        self.frontier.first().is_some_and(|e| *e <= *time)
    }
}

impl<'a, T> std::ops::Deref for AntichainRef<'a, T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.frontier
    }
}

impl<'a, T: fmt::Debug> fmt::Debug for AntichainRef<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.frontier.iter()).finish()
    }
}

impl<'a, T: fmt::Display> fmt::Display for AntichainRef<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        let mut iter = self.frontier.iter();
        if let Some(t) = iter.next() {
            t.fmt(f)?;
        }
        for t in iter {
            f.write_str(", ")?;
            t.fmt(f)?;
        }
        f.write_str("}")
    }
}

impl<'a, T: PartialEq> PartialEq for AntichainRef<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.frontier == other.frontier
    }
}
impl<'a, T: Eq> Eq for AntichainRef<'a, T> {}

// ---------------------------------------------------------------------------
// MutableAntichain
// ---------------------------------------------------------------------------

/// A multiset-based frontier tracker for totally-ordered time domains.
///
/// Tracks the minimum time with a positive reference count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Ord + Serialize", deserialize = "T: Ord + Deserialize<'de>"))]
pub struct MutableAntichain<T> {
    updates: Vec<(T, i64)>,
    frontier: Antichain<T>,
    clean: usize,
}

impl<T> MutableAntichain<T> {
    pub fn new() -> Self {
        MutableAntichain {
            updates: Vec::new(),
            frontier: Antichain::new(),
            clean: 0,
        }
    }

    pub fn frontier(&self) -> AntichainRef<'_, T> {
        self.frontier.borrow()
    }

    pub fn is_empty(&self) -> bool {
        self.frontier.is_empty()
    }

    pub fn clear(&mut self) {
        self.updates.clear();
        self.frontier.clear();
        self.clean = 0;
    }
}

impl<T: Clone + Ord> MutableAntichain<T> {
    pub fn from_elem(element: T) -> Self {
        let mut result = Self::new();
        result.update_iter(std::iter::once((element, 1)));
        result
    }

    /// Returns the reference count for `query_time`, or zero if absent.
    pub fn count_for<O: ?Sized>(&self, query_time: &O) -> i64
    where
        T: PartialEq<O>,
    {
        self.updates
            .iter()
            .find(|(t, _)| t.eq(query_time))
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }

    /// Rebuilds the internal representation and exposes `(time, count)` pairs.
    pub fn updates(&mut self) -> impl Iterator<Item = &(T, i64)> {
        if self.clean < self.updates.len() {
            self.rebuild();
        }
        self.updates.iter()
    }

    /// Applies updates and returns frontier changes.
    pub fn update_iter(
        &mut self,
        updates: impl IntoIterator<Item = (T, i64)>,
    ) -> SmallVec<[(T, i64); 2]> {
        let old_frontier = self.frontier.element.clone();

        for (time, delta) in updates {
            self.updates.push((time, delta));
        }
        self.rebuild();

        let new_frontier = &self.frontier.element;
        let mut changes = SmallVec::<[(T, i64); 2]>::new();
        if old_frontier != *new_frontier {
            if let Some(old) = old_frontier {
                changes.push((old, -1));
            }
            if let Some(new) = new_frontier.clone() {
                changes.push((new, 1));
            }
        }
        changes
    }

    fn rebuild(&mut self) {
        self.updates.sort_by(|a, b| a.0.cmp(&b.0));
        let mut write = 0;
        for read in 1..self.updates.len() {
            if self.updates[write].0 == self.updates[read].0 {
                self.updates[write].1 += self.updates[read].1;
            } else {
                if self.updates[write].1 != 0 {
                    write += 1;
                }
                self.updates.swap(write, read);
            }
        }
        if !self.updates.is_empty() {
            if self.updates[write].1 != 0 {
                write += 1;
            }
            self.updates.truncate(write);
        }
        self.clean = self.updates.len();

        // Frontier is the minimum time with positive count.
        self.frontier.clear();
        for (time, count) in &self.updates {
            if *count > 0 {
                self.frontier.insert_ref(time);
            }
        }
    }

    pub fn less_than(&self, time: &T) -> bool {
        self.frontier.less_than(time)
    }
    pub fn less_equal(&self, time: &T) -> bool {
        self.frontier.less_equal(time)
    }
}

impl<T: Clone + Ord> From<Antichain<T>> for MutableAntichain<T> {
    fn from(antichain: Antichain<T>) -> Self {
        let mut result = MutableAntichain::new();
        let updates: Vec<_> = antichain.into_iter().map(|t| (t, 1i64)).collect();
        result.update_iter(updates);
        result
    }
}

impl<'a, T: Clone + Ord> From<AntichainRef<'a, T>> for MutableAntichain<T> {
    fn from(antichain: AntichainRef<'a, T>) -> Self {
        let mut result = MutableAntichain::new();
        let updates: Vec<_> = antichain.iter().map(|t| (t.clone(), 1i64)).collect();
        result.update_iter(updates);
        result
    }
}

impl<T> Default for MutableAntichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::order::PartialOrder as FrontierPartialOrder;

    fn arb_antichain() -> impl Strategy<Value = Antichain<u32>> {
        prop::collection::vec(any::<u32>(), 0..8).prop_map(Antichain::from)
    }

    proptest! {
        #[test]
        fn at_most_one_element(v in prop::collection::vec(any::<u32>(), 0..32)) {
            let a: Antichain<u32> = v.into_iter().collect();
            prop_assert!(a.elements().len() <= 1);
        }

        #[test]
        fn contains_minimum(v in prop::collection::vec(any::<u32>(), 1..32)) {
            let a: Antichain<u32> = v.iter().copied().collect();
            let min = v.iter().copied().min().unwrap();
            prop_assert_eq!(a.as_option().copied(), Some(min));
        }

        #[test]
        fn insert_keeps_minimum(a in any::<u32>(), b in any::<u32>()) {
            let mut ac = Antichain::from_elem(a);
            let changed = ac.insert(b);
            if b < a {
                prop_assert!(changed);
                prop_assert_eq!(ac.into_element(), b);
            } else {
                prop_assert!(!changed);
                prop_assert_eq!(ac.into_element(), a);
            }
        }

        #[test]
        fn less_equal_element_matches_le(a in any::<u32>(), b in any::<u32>()) {
            let ac = Antichain::from_elem(a);
            prop_assert_eq!(ac.less_equal(&b), a <= b);
            prop_assert_eq!(ac.less_than(&b), a < b);
        }

        #[test]
        fn empty_is_neither_le_nor_lt_any_element(t in any::<u32>()) {
            let empty: Antichain<u32> = Antichain::new();
            prop_assert!(!empty.less_equal(&t));
            prop_assert!(!empty.less_than(&t));
        }

        // ---- Lattice laws ----
        #[test]
        fn join_commutative(a in arb_antichain(), b in arb_antichain()) {
            prop_assert_eq!(a.join(&b), b.join(&a));
        }

        #[test]
        fn join_associative(a in arb_antichain(), b in arb_antichain(), c in arb_antichain()) {
            prop_assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
        }

        #[test]
        fn join_idempotent(a in arb_antichain()) {
            prop_assert_eq!(a.join(&a), a.clone());
        }

        #[test]
        fn meet_commutative(a in arb_antichain(), b in arb_antichain()) {
            prop_assert_eq!(a.meet(&b), b.meet(&a));
        }

        #[test]
        fn meet_associative(a in arb_antichain(), b in arb_antichain(), c in arb_antichain()) {
            prop_assert_eq!(a.meet(&b).meet(&c), a.meet(&b.meet(&c)));
        }

        #[test]
        fn meet_idempotent(a in arb_antichain()) {
            prop_assert_eq!(a.meet(&a), a.clone());
        }

        #[test]
        fn absorption(a in arb_antichain(), b in arb_antichain()) {
            prop_assert_eq!(a.join(&a.meet(&b)), a.clone());
            prop_assert_eq!(a.meet(&a.join(&b)), a.clone());
        }

        #[test]
        fn empty_absorbs_join(a in arb_antichain()) {
            let empty: Antichain<u32> = Antichain::new();
            prop_assert_eq!(empty.join(&a), empty.clone());
            prop_assert_eq!(a.join(&empty), empty);
        }

        #[test]
        fn empty_is_identity_for_meet(a in arb_antichain()) {
            let empty: Antichain<u32> = Antichain::new();
            prop_assert_eq!(empty.meet(&a), a.clone());
            prop_assert_eq!(a.meet(&empty), a);
        }

        #[test]
        fn join_assign_matches_join(a in arb_antichain(), b in arb_antichain()) {
            let joined = a.join(&b);
            let mut a_mut = a.clone();
            a_mut.join_assign(&b);
            prop_assert_eq!(joined, a_mut);
        }

        #[test]
        fn meet_assign_matches_meet(a in arb_antichain(), b in arb_antichain()) {
            let met = a.meet(&b);
            let mut a_mut = a.clone();
            a_mut.meet_assign(&b);
            prop_assert_eq!(met, a_mut);
        }

        // ---- Partial order semantics ----
        #[test]
        fn partial_order_reflexive(a in arb_antichain()) {
            prop_assert!(FrontierPartialOrder::less_equal(&a, &a));
            prop_assert!(!FrontierPartialOrder::less_than(&a, &a));
        }

        #[test]
        fn partial_order_antisymmetric(a in arb_antichain(), b in arb_antichain()) {
            if FrontierPartialOrder::less_equal(&a, &b)
                && FrontierPartialOrder::less_equal(&b, &a)
            {
                prop_assert_eq!(a, b);
            }
        }

        #[test]
        fn partial_order_transitive(a in arb_antichain(), b in arb_antichain(), c in arb_antichain()) {
            if FrontierPartialOrder::less_equal(&a, &b)
                && FrontierPartialOrder::less_equal(&b, &c)
            {
                prop_assert!(FrontierPartialOrder::less_equal(&a, &c));
            }
        }

        #[test]
        fn join_is_lub(a in arb_antichain(), b in arb_antichain()) {
            let j = a.join(&b);
            prop_assert!(FrontierPartialOrder::less_equal(&a, &j));
            prop_assert!(FrontierPartialOrder::less_equal(&b, &j));
        }

        #[test]
        fn meet_is_glb(a in arb_antichain(), b in arb_antichain()) {
            let m = a.meet(&b);
            prop_assert!(FrontierPartialOrder::less_equal(&m, &a));
            prop_assert!(FrontierPartialOrder::less_equal(&m, &b));
        }

        // ---- Serialization roundtrip ----
        #[test]
        fn serde_roundtrip(a in arb_antichain()) {
            let bytes = bincode::serialize(&a).unwrap();
            let back: Antichain<u32> = bincode::deserialize(&bytes).unwrap();
            prop_assert_eq!(a, back);
        }

        // ---- Display ----
        #[test]
        fn display_format(a in arb_antichain()) {
            let s = format!("{}", a);
            match a.as_option() {
                None => prop_assert_eq!(s, "{}"),
                Some(t) => prop_assert_eq!(s, format!("{{{}}}", t)),
            }
        }

        // ---- AntichainRef ----
        #[test]
        fn antichain_ref_roundtrip(a in arb_antichain()) {
            let owned = a.borrow().to_owned();
            prop_assert_eq!(a, owned);
        }

        // ---- MutableAntichain ----
        #[test]
        fn mutable_frontier_tracks_minimum_positive(updates in prop::collection::vec((any::<u32>(), -2i64..=2), 0..20)) {
            let mut m: MutableAntichain<u32> = MutableAntichain::new();
            m.update_iter(updates.iter().cloned());

            // Compute the frontier semantically: minimum time with positive net count.
            let mut counts: BTreeMap<u32, i64> = BTreeMap::new();
            for (t, d) in &updates {
                *counts.entry(*t).or_insert(0) += d;
            }
            let expected: Option<u32> = counts
                .iter()
                .filter(|(_, c)| **c > 0)
                .map(|(t, _)| *t)
                .min();

            match expected {
                None => prop_assert!(m.frontier().is_empty()),
                Some(t) => prop_assert_eq!(m.frontier().first().copied(), Some(t)),
            }
        }

        #[test]
        fn mutable_refcount_cancels(t in any::<u32>()) {
            let mut m = MutableAntichain::new();
            m.update_iter([(t, 1)]);
            prop_assert!(!m.is_empty());
            m.update_iter([(t, -1)]);
            prop_assert!(m.is_empty());
        }

        #[test]
        fn mutable_count_for_matches(updates in prop::collection::vec((any::<u16>(), -2i64..=2), 0..20), query in any::<u16>()) {
            let mut m: MutableAntichain<u16> = MutableAntichain::new();
            m.update_iter(updates.iter().cloned());
            let expected: i64 = updates.iter().filter(|(t, _)| *t == query).map(|(_, d)| *d).sum();
            prop_assert_eq!(m.count_for(&query), expected);
        }

        #[test]
        fn from_antichain_gives_refcount_one(a in arb_antichain()) {
            let m: MutableAntichain<u32> = a.clone().into();
            match a.as_option() {
                None => prop_assert!(m.is_empty()),
                Some(t) => prop_assert_eq!(m.frontier().first().copied(), Some(*t)),
            }
        }
    }
}
