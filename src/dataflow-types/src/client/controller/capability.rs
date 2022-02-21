// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use timely::order::PartialOrder;
use timely::progress::{
    frontier::{AntichainRef, MutableAntichain},
    Antichain, Timestamp,
};

/// A maintainer of frontiered capabilities.
///
/// Capabilities produced will hold back the collective frontier until
/// they are released.
pub struct Capabilities<T, I = Box<dyn Iterator<Item = usize>>>
where
    I: Iterator,
    I::Item: Ord + PartialOrd + Eq + PartialEq,
{
    /// An identifier generator. Ideally does not repeat.
    next_id: I,
    /// The accumulation of all outstanding capabilities.
    ///
    /// The `frontier()` represents the lower bound of all outstanding capabilities.
    accumulated: MutableAntichain<T>,
    /// Each of the capabilities, and their associated frontier.
    capabilities: BTreeMap<I::Item, Antichain<T>>,
}

impl<T> Capabilities<T>
where
    T: Timestamp,
{
    /// Creates a new capability tracker from an initial frontier, returned as capability.
    pub fn new(frontier: Antichain<T>) -> (Self, usize) {
        let mut next_id: Box<dyn Iterator<Item = usize>> = Box::new(0..);
        let capability = next_id.next().unwrap();
        let mut capabilities = BTreeMap::default();
        capabilities.insert(capability, frontier.clone());
        (
            Self {
                next_id,
                accumulated: frontier.into(),
                capabilities,
            },
            capability,
        )
    }
}

impl<I, T> Capabilities<T, I>
where
    I: Iterator,
    I::Item: Ord + PartialOrd + Eq + PartialEq + Clone,
    T: Timestamp,
{
    /// Reports the maintained lower bound on capabilities.
    pub fn frontier(&self) -> AntichainRef<T> {
        self.accumulated.frontier()
    }

    /// Reads the value of a supplied capability.
    pub fn read(&self, capability: &I::Item) -> Option<&Antichain<T>> {
        self.capabilities.get(capability)
    }
    /// Acquires a capability for the current frontier.
    ///
    /// This may return `None` if we cannot provide a meaningful capability for any reasons.
    /// These reasons currently include: the capability id generator has been exhausted, and
    /// the frontier is currently empty.
    pub fn acquire(&mut self) -> Option<I::Item> {
        let frontier = self.accumulated.frontier().to_owned();
        if frontier.is_empty() {
            return None;
        }
        let mut changes = self
            .accumulated
            .update_iter(frontier.iter().map(|time| (time.clone(), 1)));
        assert!(changes.next().is_none());
        let this_id = self.next_id.next()?;
        self.capabilities.insert(this_id.clone(), frontier);
        Some(this_id)
    }
    /// Downgrades a capability to an advanced frontier.
    ///
    /// Returns `None` if `new_frontier` does not advance the capability, or it does not exist.
    /// Otherwise, it returns an iterator describing any consequences to the accumultade frontier,
    /// which should be stashed or communicated onward.
    pub fn downgrade(
        &mut self,
        capability: &I::Item,
        mut new_frontier: Antichain<T>,
    ) -> Option<impl Iterator<Item = (T, i64)> + '_> {
        if let Some(old_frontier) = self.capabilities.get_mut(capability) {
            if <_ as PartialOrder>::less_equal(old_frontier, &new_frontier) {
                std::mem::swap(old_frontier, &mut new_frontier);
                Some(
                    self.accumulated.update_iter(
                        new_frontier
                            .into_iter()
                            .map(|time| (time, -1))
                            .chain(old_frontier.iter().map(|time| (time.clone(), 1))),
                    ),
                )
            } else {
                None
            }
        } else {
            None
        }
    }
}
