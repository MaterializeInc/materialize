// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for describing captured timely dataflow streams.
//!
//! This is roughly based on [timely::dataflow::operators::event].

use crate::activator::RcActivator;
use std::borrow::Borrow;
use timely::dataflow::operators::capture::event::EventIteratorCore;
use timely::dataflow::operators::capture::{EventCore, EventPusherCore};

/// An event link wrapper that activates targets on push.
#[derive(Clone, Debug)]
pub struct ActivatedEventLink<E> {
    inner: E,
    activators: RcActivator,
}

impl<E> ActivatedEventLink<E> {
    /// Create a new activated event link wrapper.
    ///
    /// * inner: A wrapped event pusher/iterator.
    pub fn new(inner: E, activators: RcActivator) -> Self {
        Self { inner, activators }
    }

    pub fn activator(&self) -> &RcActivator {
        self.activators.borrow()
    }
}

impl<T, D, E: EventPusherCore<T, D>> EventPusherCore<T, D> for ActivatedEventLink<E> {
    fn push(&mut self, event: EventCore<T, D>) {
        self.inner.push(event);
        self.activators.activate();
    }
}

impl<T, D, E: EventIteratorCore<T, D>> EventIteratorCore<T, D> for ActivatedEventLink<E> {
    fn next(&mut self) -> Option<&EventCore<T, D>> {
        self.inner.next()
    }
}
