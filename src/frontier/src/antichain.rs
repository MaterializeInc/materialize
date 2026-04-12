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
#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl<T> Default for MutableAntichain<T> {
    fn default() -> Self {
        Self::new()
    }
}
