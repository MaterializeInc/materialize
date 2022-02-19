// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use timely::progress::{frontier::AntichainRef, Antichain, ChangeBatch, Timestamp};

use mz_expr::GlobalId;

pub struct SinceUpperMap<T> {
    since_uppers: BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
}

impl<T> Default for SinceUpperMap<T> {
    fn default() -> Self {
        Self {
            since_uppers: Default::default(),
        }
    }
}

impl<T> SinceUpperMap<T>
where
    T: Timestamp + Lattice,
{
    pub fn insert(&mut self, id: GlobalId, since_upper: (Antichain<T>, Antichain<T>)) {
        self.since_uppers.insert(id, since_upper);
    }
    pub fn get(&self, id: &GlobalId) -> Option<(AntichainRef<T>, AntichainRef<T>)> {
        self.since_uppers
            .get(id)
            .map(|(since, upper)| (since.borrow(), upper.borrow()))
    }
    pub fn advance_since_for(&mut self, id: GlobalId, frontier: &Antichain<T>) {
        if let Some((since, _upper)) = self.since_uppers.get_mut(&id) {
            since.join_assign(frontier);
        } else {
            // If we allow compaction before the item is created, pre-restrict the valid range.
            self.since_uppers
                .insert(id, (frontier.clone(), Antichain::from_elem(T::minimum())));
        }
    }
    pub fn update_upper_for(&mut self, id: GlobalId, changes: &ChangeBatch<T>) {
        if let Some((_since, upper)) = self.since_uppers.get_mut(&id) {
            // Apply `changes` to `upper`.
            let mut changes = changes.clone();
            for time in upper.elements().iter() {
                changes.update(time.clone(), 1);
            }
            upper.clear();
            for (time, count) in changes.drain() {
                assert_eq!(count, 1);
                upper.insert(time);
            }
        } else {
            // No panic, as we could have recently dropped this.
            // If we can tell these are updates to an id that could still be constructed,
            // something is weird and we should error.
        }
    }
}
