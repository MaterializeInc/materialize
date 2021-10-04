// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to activate dataflows based on external triggers.

use std::cell::RefCell;
use std::rc::Rc;
use timely::scheduling::Activator;

/// An shared handle to multiple activators with support for triggering and acknowledging
/// activations.
#[derive(Debug, Clone)]
pub struct RcActivator {
    inner: Rc<RefCell<ActivatorInner>>,
}

impl RcActivator {
    /// Construct a new [RcActivator] with the given name.
    pub fn new(name: String) -> Self {
        let inner = ActivatorInner::new(name);
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }

    /// Register an additional [Activator] with this [RcActivator]
    pub fn register(&mut self, activator: Activator) {
        self.inner.borrow_mut().register(activator)
    }

    /// Activate all contained activators.
    ///
    /// The implementation is free to ignore activations and only release them once a sufficient
    /// volume has been accumulated.
    pub fn activate(&mut self) {
        self.inner.borrow_mut().activate()
    }

    /// Acknowledge the activation, which enables new activations to be scheduled.
    pub fn ack(&mut self) {
        self.inner.borrow_mut().ack()
    }
}

#[derive(Debug)]
struct ActivatorInner {
    activated: usize,
    activators: Vec<Activator>,
    name: String,
}

impl ActivatorInner {
    const THRESHOLD: usize = 32;

    fn new(name: String) -> Self {
        Self {
            name,
            activated: 0,
            activators: Vec::new(),
        }
    }

    fn register(&mut self, activator: Activator) {
        self.activators.push(activator)
    }

    fn activate(&mut self) {
        if self.activators.is_empty() {
            return;
        }
        self.activated += 1;
        if self.activated == ActivatorInner::THRESHOLD {
            for activator in &self.activators {
                activator.activate();
            }
        }
    }

    fn ack(&mut self) {
        self.activated = 0;
    }
}
