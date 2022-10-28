// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scope combinators that alter the nature of the operators scheduled within

use std::any::Any;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

use timely::dataflow::scopes::{Child, Scope};
use timely::dataflow::ScopeParent;
use timely::progress::operate::SharedProgress;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, ChangeBatch, Operate, SubgraphBuilder, Timestamp};
use timely::scheduling::Schedule;
use timely::worker::AsWorker;

/// Scope extension trait
///
/// For now the only extension it provides is to define fused regions that can be individually
/// disabled using a token.
pub trait ScopeExt: Scope {
    /// Creates a dataflow region with the same timestamp and returns a token that can be used to
    /// control the scheduling of operators within that region.
    ///
    /// When the last clone of the token is dropped, the operators of this region will be dropped
    /// and the inputs will be drained.
    ///
    /// It is up to the caller to ensure that this behavior results in meaningful results.
    fn region_fused<R, F>(&mut self, func: F) -> (R, Rc<dyn Any>)
    where
        F: FnOnce(&mut Child<'_, Self, Self::Timestamp>) -> R;
}

impl<'a, G, T> ScopeExt for Child<'a, G, T>
where
    G: ScopeParent,
    T: Timestamp + Refines<G::Timestamp>,
{
    fn region_fused<R, F>(&mut self, func: F) -> (R, Rc<dyn Any>)
    where
        F: FnOnce(&mut Child<'_, Self, Self::Timestamp>) -> R,
    {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.subgraph.borrow().path.clone();

        let subscope = RefCell::new(SubgraphBuilder::new_from(
            index,
            path,
            self.logging(),
            self.progress_logging.clone(),
            "FusedRegion",
        ));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        let (fused, token) = FusedOperator::new(subscope);
        self.add_operator_with_index(Box::new(fused), index);

        (result, token)
    }
}

struct FusedOperator<O: Operate<T>, T: Timestamp> {
    operator: Option<O>,
    inputs: usize,
    outputs: usize,
    local: bool,
    notify: bool,
    name: String,
    path: Vec<usize>,
    progress: Rc<RefCell<SharedProgress<T>>>,
    reported_progress: Rc<RefCell<SharedProgress<T>>>,
    /// This records what capabilities are held by the operator at any given moment. When the
    /// operator is dropped we can emit what is left in here with negative diffs to effectively
    /// release all the capabilities.
    capabilities: Vec<ChangeBatch<T>>,
    weak_token: Weak<()>,
}

impl<T: Timestamp, O: Operate<T>> FusedOperator<O, T> {
    fn new(operator: O) -> (Self, Rc<()>) {
        let token = Rc::new(());
        let weak_token = Rc::downgrade(&token);

        let reported_progress = Rc::new(RefCell::new(SharedProgress::new(
            operator.inputs(),
            operator.outputs(),
        )));

        let fused = Self {
            inputs: operator.inputs(),
            outputs: operator.outputs(),
            local: operator.local(),
            notify: operator.notify_me(),
            name: operator.name().to_owned(),
            path: operator.path().to_owned(),
            progress: Rc::new(RefCell::new(SharedProgress::new(0, 0))),
            reported_progress,
            capabilities: vec![ChangeBatch::new(); operator.outputs()],
            operator: Some(operator),
            weak_token,
        };

        (fused, token)
    }

    /// Reports the progress produced by the operator and notes down the capabilities that is
    /// holding onto internally
    fn report_progress(&mut self) {
        let mut from = self.progress.borrow_mut();
        let mut to = self.reported_progress.borrow_mut();
        for (from, to) in from.frontiers.iter_mut().zip(&mut to.frontiers) {
            from.drain_into(to)
        }
        for (from, to) in from.consumeds.iter_mut().zip(&mut to.consumeds) {
            from.drain_into(to)
        }
        for ((from, to), capabilities) in from
            .internals
            .iter_mut()
            .zip(&mut to.internals)
            .zip(&mut self.capabilities)
        {
            capabilities.extend(from.iter().cloned());
            from.drain_into(to)
        }
        for (from, to) in from.produceds.iter_mut().zip(&mut to.produceds) {
            from.drain_into(to)
        }
    }
}

impl<O, T: Timestamp> Operate<T> for FusedOperator<O, T>
where
    O: Operate<T>,
{
    fn inputs(&self) -> usize {
        self.inputs
    }
    fn outputs(&self) -> usize {
        self.outputs
    }
    fn get_internal_summary(
        &mut self,
    ) -> (
        Vec<Vec<Antichain<T::Summary>>>,
        Rc<RefCell<SharedProgress<T>>>,
    ) {
        let (summary, progress) = self
            .operator
            .as_mut()
            .expect("token dropped before initialization")
            .get_internal_summary();
        self.progress = Rc::clone(&progress);
        self.report_progress();
        (summary, Rc::clone(&self.reported_progress))
    }

    fn local(&self) -> bool {
        self.local
    }
    fn set_external_summary(&mut self) {
        if let Some(operator) = self.operator.as_mut() {
            operator.set_external_summary();
            self.report_progress();
        }
    }
    fn notify_me(&self) -> bool {
        self.notify
    }
}

impl<O: Operate<T>, T: Timestamp> Schedule for FusedOperator<O, T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn path(&self) -> &[usize] {
        &self.path
    }
    fn schedule(&mut self) -> bool {
        if self.weak_token.upgrade().is_none() {
            if let Some(operator) = self.operator.take() {
                // Drop the operator to run any Drop destructors
                drop(operator);
                // Report progress one last time in case the Drop code manipulated progress
                self.report_progress();
                // Finally drop all remaining capabilities
                let mut progress = self.reported_progress.borrow_mut();
                for (progress, capabilities) in
                    progress.internals.iter_mut().zip(&mut self.capabilities)
                {
                    progress.extend(capabilities.drain().map(|(t, diff)| (t, -diff)));
                }
            }
        }
        if let Some(operator) = self.operator.as_mut() {
            let reschedule = operator.schedule();
            self.report_progress();
            reschedule
        } else {
            false
        }
    }
}
