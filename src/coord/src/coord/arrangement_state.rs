// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Frontier state for each arrangement.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::Timestamp;

use expr::GlobalId;

use crate::coord::antichain::AntichainToken;

/// A map from global identifiers to arrangement frontier state.
pub struct ArrangementFrontiers<T: Timestamp> {
    index: HashMap<GlobalId, Frontiers<T>>,
}

impl<T: Timestamp> Default for ArrangementFrontiers<T> {
    fn default() -> Self {
        Self {
            index: HashMap::new(),
        }
    }
}

impl<T: Timestamp> ArrangementFrontiers<T> {
    pub fn get(&self, id: &GlobalId) -> Option<&Frontiers<T>> {
        self.index.get(id)
    }
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut Frontiers<T>> {
        self.index.get_mut(id)
    }
    pub fn contains_key(&self, id: GlobalId) -> bool {
        self.index.contains_key(&id)
    }
    pub fn intersection(&self, ids: impl IntoIterator<Item = GlobalId>) -> Vec<GlobalId> {
        ids.into_iter()
            .filter(|id| self.contains_key(*id))
            .collect()
    }
    pub fn insert(&mut self, id: GlobalId, state: Frontiers<T>) -> Option<Frontiers<T>> {
        self.index.insert(id, state)
    }
    pub fn remove(&mut self, id: &GlobalId) -> Option<Frontiers<T>> {
        self.index.remove(id)
    }

    /// The upper frontier of a maintained index, if it exists.
    pub fn upper_of(&self, name: &GlobalId) -> Option<AntichainRef<T>> {
        if let Some(index_state) = self.get(name) {
            Some(index_state.upper.frontier())
        } else {
            None
        }
    }

    /// The since frontier of a maintained index, if it exists.
    pub fn since_of(&self, name: &GlobalId) -> Option<Antichain<T>> {
        if let Some(index_state) = self.get(name) {
            // TODO: &..to_owned needlessly allocs.
            Some(index_state.since.borrow().frontier().to_owned())
        } else {
            None
        }
    }

    /// Reports the greatest frontier less than all identified `upper` frontiers.
    pub fn greatest_open_upper<I>(&self, identifiers: I) -> Antichain<T>
    where
        I: IntoIterator<Item = GlobalId>,
        T: Lattice,
    {
        // Form lower bound on available times
        let mut min_upper = Antichain::new();
        for id in identifiers {
            // To track the meet of `upper` we just extend with the upper frontier.
            // This was almost `meet_assign` but our uppers are `MutableAntichain`s.
            min_upper.extend(self.upper_of(&id).unwrap().iter().cloned());
        }
        min_upper
    }

    /// Reports the minimal frontier greater than all identified `since` frontiers.
    pub fn least_valid_since<I>(&self, identifiers: I) -> Antichain<T>
    where
        I: IntoIterator<Item = GlobalId>,
        T: Lattice,
    {
        let mut max_since = Antichain::from_elem(T::minimum());
        for id in identifiers {
            // TODO: We could avoid repeated allocation by swapping two buffers.
            max_since.join_assign(&self.since_of(&id).expect("Since missing at coordinator"));
        }
        max_since
    }
}

pub struct Frontiers<T: Timestamp> {
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    pub upper: MutableAntichain<T>,
    /// The most recent frontier for durable data.
    /// All data at times in advance of this frontier have not yet been
    /// durably persisted and may not be replayable across restarts.
    pub durability: MutableAntichain<T>,
    /// The compaction frontier.
    /// All peeks in advance of this frontier will be correct,
    /// but peeks not in advance of this frontier may not be.
    pub since: Rc<RefCell<MutableAntichain<T>>>,
    /// The function to run on since changes.
    /// Passes the new since frontier.
    since_action: Rc<RefCell<dyn FnMut(Antichain<T>)>>,
    /// Compaction delay.
    ///
    /// This timestamp drives the advancement of the since frontier as a
    /// function of the upper frontier, trailing it by exactly this much.
    pub compaction_window_ms: Option<T>,
}

impl<T: Timestamp + Copy> Frontiers<T> {
    /// Creates an empty index state from a function to run when the since changes.
    /// Returns the initial since handle.
    pub fn new<I, F>(
        initial: I,
        compaction_window_ms: Option<T>,
        since_action: F,
    ) -> (Self, AntichainToken<T>)
    where
        I: IntoIterator<Item = T>,
        F: FnMut(Antichain<T>) + 'static,
    {
        let mut upper = MutableAntichain::new();
        // Upper must always start at minimum ("0"), even if we initialize since to
        // something in advance of it.
        upper.update_iter(Some((T::minimum(), 1)));
        let durability = upper.clone();
        let frontier = Self {
            upper,
            durability,
            since: Rc::new(RefCell::new(MutableAntichain::new())),
            compaction_window_ms,
            since_action: Rc::new(RefCell::new(since_action)),
        };
        let handle = frontier.since_handle(initial);
        (frontier, handle)
    }

    /// Returns a wrapped MutableAntichain that propogates changes to `since`.
    pub fn since_handle<I>(&self, values: I) -> AntichainToken<T>
    where
        I: IntoIterator<Item = T>,
    {
        let since = Rc::clone(&self.since);
        let since_action = Rc::clone(&self.since_action);
        AntichainToken::new(values, move |changes| {
            let changed = since.borrow_mut().update_iter(changes).next().is_some();
            if changed {
                (since_action.borrow_mut())(since.borrow().frontier().to_owned());
            }
        })
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_window_ms(&mut self, window_ms: Option<T>) {
        self.compaction_window_ms = window_ms;
    }
}

/// Track sink state for which timestamps the sink has written out.
pub struct SinkWrites<T: Timestamp> {
    /// The write frontier for the sink, ie all subsequent writes will be at
    /// timestamps that are at or in advance of this frontier.
    pub frontier: MutableAntichain<T>,
    /// Set of handles to sources that the sink transitively depends on. We hold
    /// back all of these sources' since frontiers to trail the sink's write frontier
    /// and allow the since frontier to advance as the sink's write frontier advances.
    pub source_handles: Vec<AntichainToken<T>>,
}

impl<T: Timestamp> SinkWrites<T> {
    pub fn new(source_handles: Vec<AntichainToken<T>>) -> Self {
        let mut frontier = MutableAntichain::new();
        frontier.update_iter(Some((T::minimum(), 1)));

        SinkWrites {
            frontier,
            source_handles,
        }
    }

    /// Allow all sources that the sink transitively depends on to advance their compaction/since
    /// frontiers up to the sink's write frontier, if they so choose.
    ///
    /// This function needs to be called periodically, otherwise sink's source dependencies will
    /// never compact their timestamp bindings in memory or in the catalog. However, this doesn't
    /// have to happen in lockstep with write frontier updates.
    pub fn advance_source_handles(&mut self) {
        let frontier: Vec<_> = self.frontier.frontier().to_owned().elements().to_vec();
        for handle in self.source_handles.iter_mut() {
            handle.maybe_advance(frontier.iter().cloned());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use timely::progress::frontier::Antichain;

    use super::Frontiers;

    #[test]
    fn test_frontiers_action() {
        let expect = Rc::new(RefCell::new(Antichain::from_elem(0)));
        let inner = Rc::clone(&expect);
        let (f, initial) = Frontiers::new(Some(0), None, move |since| {
            assert_eq!(*inner.borrow(), since);
        });
        // Adding 5 should not change the since.
        let h1 = f.since_handle(vec![5u64]);
        assert_eq!(f.since.borrow().frontier().to_owned(), *expect.borrow());

        // When we drop the initial, it should advance to 5.
        expect.replace(Antichain::from_elem(5));
        drop(initial);
        assert_eq!(f.since.borrow().frontier().to_owned(), *expect.borrow());

        // When the last since_handle is dropped, it should be empty.
        expect.replace(Antichain::new());
        drop(h1);
        assert_eq!(f.since.borrow().frontier().to_owned(), *expect.borrow());
    }
}
