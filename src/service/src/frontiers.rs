// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Reconciliation of timely frontiers.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use timely::progress::frontier::MutableAntichain;
use timely::progress::{ChangeBatch, Timestamp};

use mz_repr::GlobalId;

/// Reconciles a set of frontiers that are each associated with a [`GlobalId`].
#[derive(Debug)]
pub struct FrontierReconcile<T> {
    frontiers: HashMap<GlobalId, MutableAntichain<T>>,
}

impl<T> Default for FrontierReconcile<T> {
    fn default() -> FrontierReconcile<T> {
        FrontierReconcile {
            frontiers: HashMap::default(),
        }
    }
}

impl<T> FrontierReconcile<T>
where
    T: Timestamp + Copy,
{
    /// Starts tracking the frontier for an ID.
    ///
    /// If the ID is already tracked, returns a change batch that describes
    /// how to update the minimum frontier to the currently tracked frontier.
    pub fn start_tracking(&mut self, id: GlobalId) -> ChangeBatch<T> {
        match self.frontiers.entry(id) {
            Entry::Occupied(entry) => {
                let mut change_batch = ChangeBatch::new_from(T::minimum(), -1);
                change_batch.extend(entry.get().frontier().iter().map(|t| (*t, 1)));
                change_batch
            }
            Entry::Vacant(entry) => {
                entry.insert(MutableAntichain::new_bottom(T::minimum()));
                ChangeBatch::new()
            }
        }
    }

    /// Stops tracking the frontier for an ID.
    ///
    /// Returns the tracked frontier for the ID, if the ID was tracked.
    pub fn stop_tracking(&mut self, id: GlobalId) -> Option<MutableAntichain<T>> {
        self.frontiers.remove(&id)
    }

    /// Reports whether the ID is currently tracked.
    pub fn is_tracked(&self, id: GlobalId) -> bool {
        self.frontiers.contains_key(&id)
    }

    /// Absorbs new frontiers and mutates the provided frontiers to reflect the
    /// tracked state.
    pub fn absorb(&mut self, frontiers: &mut Vec<(GlobalId, ChangeBatch<T>)>) {
        frontiers.retain_mut(|(id, changes)| {
            if let Some(frontier) = self.frontiers.get_mut(id) {
                let iter = frontier.update_iter(changes.drain());
                changes.extend(iter);
                true
            } else {
                false
            }
        });
    }

    /// Removes all tracked frontiers.
    pub fn clear(&mut self) {
        self.frontiers.clear();
    }
}
