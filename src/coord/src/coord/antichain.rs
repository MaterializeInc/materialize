// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Antichain utilities.

#![allow(missing_debug_implementations)]

use std::rc::Rc;
use tracing::trace;

use timely::order::PartialOrder;
use timely::progress::change_batch::ChangeBatch;
use timely::progress::frontier::MutableAntichain;
use timely::progress::Antichain;

/// A token that holds a mutable antichain and invokes an action should it change.
pub struct AntichainToken<T: PartialOrder + Ord + Clone> {
    antichain: MutableAntichain<T>,
    // This is behind an `Rc` so that `Clone` can use `&self` rather than `&mut self`.
    changes: std::rc::Rc<std::cell::RefCell<ChangeBatch<T>>>,
    action: std::rc::Rc<std::cell::RefCell<dyn FnMut(std::vec::Drain<(T, i64)>)>>,
}

impl<T: PartialOrder + Ord + Clone + std::fmt::Debug> AntichainToken<T> {
    /// Creates a new token.
    pub fn new<I, F>(values: I, action: F) -> Self
    where
        I: IntoIterator<Item = T>,
        F: FnMut(std::vec::Drain<(T, i64)>) + 'static,
    {
        let mut antichain = MutableAntichain::new();
        let changes = std::rc::Rc::new(std::cell::RefCell::new(ChangeBatch::new()));
        let action = std::rc::Rc::new(std::cell::RefCell::new(action));
        changes
            .borrow_mut()
            .extend(values.into_iter().map(|time| (time, 1)));
        (action.borrow_mut())(antichain.update_iter(changes.borrow_mut().drain()));

        Self {
            antichain,
            changes,
            action,
        }
    }

    /// Advance the frontier of the mutable antichain to `frontier`, if `frontier` is
    /// ahead of the current frontier.
    ///
    /// This function intentionally does not assert that `frontier` will necessarily
    /// be in advance of the current frontier because some uses in the Coordinator don't
    /// work well with that. Specifically, in the Coordinator its possible for a `since`
    /// frontier to be ahead of the `upper` frontier for an index, and changes to the
    /// `upper` frontier cause the Coordinator to want to "advance" the `since` to
    /// `new_upper - logical_compaction_window` which might not be in advance of the
    /// existing `since` frontier.
    /// TODO: can we remove `maybe_advance` and replace with a stricter `advace` that
    /// requires callers to present incresing frontiers?
    pub fn maybe_advance<F>(&mut self, frontier: F)
    where
        F: IntoIterator<Item = T>,
    {
        // TODO(rkhaitan): do this without the extra allocations.
        let mut new_frontier = Antichain::new();
        new_frontier.extend(frontier.into_iter());

        if !<_ as PartialOrder>::less_equal(&self.antichain.frontier(), &new_frontier.borrow()) {
            trace!(
                "Ignoring request to 'advance' to {:?} current antichain {:?}",
                new_frontier,
                self.antichain
            );
            return;
        }
        self.changes.borrow_mut().extend(
            self.antichain
                .frontier()
                .into_iter()
                .cloned()
                .map(|time| (time, -1)),
        );
        self.changes.borrow_mut().extend(
            new_frontier
                .elements()
                .into_iter()
                .cloned()
                .map(|time| (time, 1)),
        );
        (self.action.borrow_mut())(
            self.antichain
                .update_iter(self.changes.borrow_mut().drain()),
        );
    }
}

impl<T: PartialOrder + Ord + Clone> std::ops::Deref for AntichainToken<T> {
    type Target = MutableAntichain<T>;
    fn deref(&self) -> &Self::Target {
        &self.antichain
    }
}

impl<T: PartialOrder + Ord + Clone> Clone for AntichainToken<T> {
    fn clone(&self) -> Self {
        // Report a second copy made of the antichain.
        self.changes.borrow_mut().extend(
            self.antichain
                .frontier()
                .into_iter()
                .cloned()
                .map(|time| (time, 1)),
        );
        (self.action.borrow_mut())(self.changes.borrow_mut().drain());
        Self {
            antichain: self.antichain.clone(),
            changes: Rc::clone(&self.changes),
            action: Rc::clone(&self.action),
        }
    }
}

impl<T: PartialOrder + Ord + Clone> Drop for AntichainToken<T> {
    fn drop(&mut self) {
        // Report deletions of the antichain elements.
        self.changes.borrow_mut().extend(
            self.antichain
                .frontier()
                .into_iter()
                .cloned()
                .map(|time| (time, -1)),
        );
        (self.action.borrow_mut())(self.changes.borrow_mut().drain());
    }
}
