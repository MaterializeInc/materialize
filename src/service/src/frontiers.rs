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
    frontiers: HashMap<GlobalId, FrontierState<T>>,
    epoch: u64,
}

#[derive(Debug)]
struct FrontierState<T> {
    frontier: MutableAntichain<T>,
    epoch: u64,
}

impl<T> Default for FrontierReconcile<T> {
    fn default() -> FrontierReconcile<T> {
        FrontierReconcile {
            frontiers: HashMap::default(),
            epoch: 0,
        }
    }
}

impl<T> FrontierReconcile<T>
where
    T: Timestamp + Copy,
{
    /// Bumps the internal epoch to account for a new controller.
    ///
    /// After calling this method, `absorb` will no longer return any changes.
    /// [`FrontierReconcile::start_tracking`] must be called again for any
    /// frontiers that remain of interest.
    pub fn bump_epoch(&mut self) {
        self.epoch += 1;
    }

    /// Starts tracking the frontier for an ID.
    ///
    /// If the ID is already tracked, returns a change batch that describes
    /// how to update the minimum frontier to the currently tracked frontier.
    pub fn start_tracking(&mut self, id: GlobalId) -> ChangeBatch<T> {
        match self.frontiers.entry(id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().epoch = self.epoch;
                let frontier = entry.get().frontier.frontier();
                let mut change_batch = ChangeBatch::new_from(T::minimum(), -1);
                change_batch.extend(frontier.iter().map(|t| (*t, 1)));
                change_batch
            }
            Entry::Vacant(entry) => {
                entry.insert(FrontierState {
                    frontier: MutableAntichain::new_bottom(T::minimum()),
                    epoch: self.epoch,
                });
                ChangeBatch::new()
            }
        }
    }

    /// Stops tracking the frontier for an ID.
    ///
    /// Returns the tracked frontier for the ID, if the ID was tracked.
    pub fn stop_tracking(&mut self, id: GlobalId) -> Option<MutableAntichain<T>> {
        self.frontiers.remove(&id).map(|fs| fs.frontier)
    }

    /// Reports whether the ID is currently tracked.
    pub fn is_tracked(&self, id: GlobalId) -> bool {
        self.frontiers.contains_key(&id)
    }

    /// Absorbs new frontiers and mutates the provided frontiers to reflect the
    /// tracked state.
    pub fn absorb(&mut self, frontiers: &mut Vec<(GlobalId, ChangeBatch<T>)>) {
        frontiers.retain_mut(|(id, changes)| {
            if let Some(fs) = self.frontiers.get_mut(id) {
                let iter = fs.frontier.update_iter(changes.drain());
                if fs.epoch == self.epoch {
                    changes.extend(iter);
                    true
                } else {
                    false
                }
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
