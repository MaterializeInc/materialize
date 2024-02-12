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
//! This is roughly based on [timely::dataflow::operators::capture::event].

use timely::dataflow::operators::capture::{EventCore, EventPusherCore};

use crate::activator::RcActivator;

/// An event pusher wrapper that activates targets on push.
#[derive(Clone, Debug)]
pub struct ActivatedEventPusher<E> {
    /// Inner event pusher.
    pub inner: E,
    /// Activator used to prompt receipt of event.
    pub activator: RcActivator,
}

impl<E> ActivatedEventPusher<E> {
    /// Create a new activated event link wrapper.
    ///
    /// * inner: A wrapped event pusher/iterator.
    pub fn new(inner: E, activator: RcActivator) -> Self {
        Self { inner, activator }
    }
}

impl<T, D, E: EventPusherCore<T, D>> EventPusherCore<T, D> for ActivatedEventPusher<E> {
    fn push(&mut self, event: EventCore<T, D>) {
        self.inner.push(event);
        self.activator.activate();
    }
}
