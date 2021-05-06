// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Antichain utilities.

#![allow(missing_debug_implementations)]

use timely::order::PartialOrder;
use timely::progress::change_batch::ChangeBatch;
use timely::progress::frontier::MutableAntichain;

/// A token that holds a mutable antichain and invokes an action should it change.
pub struct AntichainToken<T: PartialOrder + Ord + Clone> {
    antichain: MutableAntichain<T>,
    // This is behind an `Rc` so that `Clone` can use `&self` rather than `&mut self`.
    changes: std::rc::Rc<std::cell::RefCell<ChangeBatch<T>>>,
    action: std::rc::Rc<std::cell::RefCell<dyn FnMut(std::vec::Drain<(T, i64)>)>>,
}

impl<T: PartialOrder + Ord + Clone> AntichainToken<T> {
    /// Creates a new token.
    pub fn new<I, F>(values: I, action: F) -> Self
    where
        I: IntoIterator<Item = T>,
        F: FnMut(std::vec::Drain<(T, i64)>) + 'static,
    {
        let mut token = Self {
            antichain: MutableAntichain::new(),
            changes: std::rc::Rc::new(std::cell::RefCell::new(ChangeBatch::new())),
            action: std::rc::Rc::new(std::cell::RefCell::new(action)),
        };
        token.advance(values);
        token
    }

    /// Apply direct updates to the maintained antichain.
    pub fn update<I>(&mut self, updates: I)
    where
        I: IntoIterator<Item = (T, i64)>,
    {
        self.changes.borrow_mut().extend(updates.into_iter());
        (self.action.borrow_mut())(
            self.antichain
                .update_iter(self.changes.borrow_mut().drain()),
        );
    }

    /// Advance the frontier of the mutable antichain to `frontier`.
    pub fn advance<F>(&mut self, frontier: F)
    where
        F: IntoIterator<Item = T>,
    {
        // TODO(mjibson): Add self.antichain.less_equal(frontier) check here. Need to
        // correctly handle the initial case where self.antichain is empty.
        self.changes.borrow_mut().extend(
            self.antichain
                .frontier()
                .into_iter()
                .cloned()
                .map(|time| (time, -1)),
        );
        self.changes
            .borrow_mut()
            .extend(frontier.into_iter().map(|time| (time, 1)));
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
            changes: self.changes.clone(),
            action: self.action.clone(),
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
