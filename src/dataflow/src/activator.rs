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
use std::sync::{Arc, Mutex};
use timely::dataflow::Scope;
use timely::scheduling::{Activator, SyncActivator};

/// Generic activator behavior
pub trait ActivatorTrait {
    /// Trigger an activation of operators behind this activator.
    fn activate(&self);

    /// Acknowledge the receipt of activations from whithin an operator.
    fn ack(&self);

    /// Register a new operator with its path with this activator.
    fn register<S: Scope>(&self, scope: &mut S, path: &[usize]);
}

/// An shared handle to multiple activators with support for triggering and acknowledging
/// activations.
///
/// Activations are only triggered once the `activate` function has been called at least `threshold`
/// times, and then not again until `ack` is called. This way, the [RcActivator] ensures two
/// properties:
/// * It does not enqueue more than one activation per activator, if there is only one activator
///   registered with this [RcActivator]. Once multiple activators are registered, any ack will
///   enable more activations.
/// * The threshold to activation avoids activations purely caused by previous activations. Each
///   scheduling of a logging dataflow potentially creates additional log data, which needs to be
///   processed. The threshold should ensure that multiple activations under no load cause the
///   dataflow to be scheduled. For Materialize's log dataflows, this number seems to be larger than
///   32, below we might risk that we do not cause monotonically decreasing work. A value of 64 or
///   larger is recommended, as there is no harm in bigger values. The log dataflow will still pick
///   up all its inputs once every introspection interval, and this activator only creates
///   additional activations.
#[derive(Debug, Clone)]
pub struct RcActivator {
    inner: Rc<RefCell<ActivatorInner>>,
}

impl RcActivator {
    /// Construct a new [RcActivator] with the given name and threshold.
    ///
    /// The threshold determines now many activations to ignore until scheduling the activation.
    pub fn new(name: String, threshold: usize) -> Self {
        let inner = ActivatorInner::new(name, threshold);
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }

    /// Register an additional [Activator] with this [RcActivator].
    pub fn register(&self, activator: Activator) {
        self.inner.borrow_mut().register(activator)
    }

    /// Activate all contained activators.
    ///
    /// The implementation is free to ignore activations and only release them once a sufficient
    /// volume has been accumulated.
    pub fn activate(&self) {
        self.inner.borrow_mut().activate()
    }

    /// Acknowledge the activation, which enables new activations to be scheduled.
    pub fn ack(&self) {
        self.inner.borrow_mut().ack()
    }
}

impl ActivatorTrait for RcActivator {
    fn activate(&self) {
        self.activate()
    }

    fn ack(&self) {
        self.ack()
    }

    fn register<S: Scope>(&self, scope: &mut S, path: &[usize]) {
        self.register(scope.activator_for(path))
    }
}

#[derive(Debug)]
struct ActivatorInner {
    activated: usize,
    activators: Vec<Activator>,
    _name: String,
    threshold: usize,
}

impl ActivatorInner {
    fn new(name: String, threshold: usize) -> Self {
        Self {
            _name: name,
            threshold,
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
        if self.activated == self.threshold {
            for activator in &self.activators {
                activator.activate();
            }
        }
    }

    fn ack(&mut self) {
        self.activated = 0;
    }
}

/// An shared handle to multiple activators with support for triggering and acknowledging
/// activations.
///
/// See the documentation of [RcActivator] for details. This implementation differs in that it
/// provides a thread-safe activation mechanism.
#[derive(Debug, Clone)]
pub struct ArcActivator {
    inner: Arc<Mutex<ArcActivatorInner>>,
}

impl ArcActivator {
    /// Construct a new [ArcActivator] with the given name and threshold.
    ///
    /// The threshold determines now many activations to ignore until scheduling the activation.
    pub fn new(name: String, threshold: usize) -> Self {
        let inner = ArcActivatorInner::new(name, threshold);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Register an additional [Activator] with this [ArcActivator].
    pub fn register(&self, activator: SyncActivator) {
        self.inner.lock().unwrap().register(activator)
    }

    /// Activate all contained activators.
    ///
    /// The implementation is free to ignore activations and only release them once a sufficient
    /// volume has been accumulated.
    pub fn activate(&self) {
        self.inner.lock().unwrap().activate()
    }

    /// Acknowledge the activation, which enables new activations to be scheduled.
    pub fn ack(&self) {
        self.inner.lock().unwrap().ack()
    }
}

impl ActivatorTrait for ArcActivator {
    fn activate(&self) {
        self.activate()
    }

    fn ack(&self) {
        self.ack()
    }

    fn register<S: Scope>(&self, scope: &mut S, path: &[usize]) {
        self.register(scope.sync_activator_for(path))
    }
}

#[derive(Debug)]
struct ArcActivatorInner {
    activated: usize,
    activators: Vec<SyncActivator>,
    _name: String,
    threshold: usize,
}

impl ArcActivatorInner {
    fn new(name: String, threshold: usize) -> Self {
        Self {
            _name: name,
            threshold,
            activated: 0,
            activators: Vec::new(),
        }
    }

    fn register(&mut self, activator: SyncActivator) {
        self.activators.push(activator)
    }

    fn activate(&mut self) {
        if self.activators.is_empty() {
            return;
        }
        self.activated += 1;
        if self.activated == self.threshold {
            for activator in &self.activators {
                activator.activate().unwrap();
            }
        }
    }

    fn ack(&mut self) {
        self.activated = 0;
    }
}
