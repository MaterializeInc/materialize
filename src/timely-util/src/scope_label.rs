// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Scopes with profiling labels set at schedule time.

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::Scope;
use timely::dataflow::scopes::Child;
use timely::dataflow::scopes::ScopeParent;
use timely::progress::operate::SharedProgress;
use timely::progress::timestamp::Refines;
use timely::progress::{Operate, SubgraphBuilder, Timestamp};
use timely::scheduling::Schedule;
use timely::scheduling::Scheduler;
use timely::worker::AsWorker;

/// A wrapper around a timely [`Scope`] that that sets its name as a profiling label before
/// scheduling its child operators.
#[derive(Clone)]
pub struct LabelledScope<G> {
    /// Label value to set when an operator is scheduled.
    label: String,
    /// The inner scope.
    inner: G,
}

impl<'a, G, T> LabelledScope<Child<'a, G, T>>
where
    G: ScopeParent,
    T: Timestamp + Refines<G::Timestamp>,
{
    /// A reference of the child’s parent scope.
    pub fn parent(&self) -> &G {
        &self.inner.parent
    }
}

impl<G: Scheduler> Scheduler for LabelledScope<G> {
    fn activations(&self) -> Rc<RefCell<timely::scheduling::Activations>> {
        self.inner.activations()
    }

    fn activator_for(&self, path: Rc<[usize]>) -> timely::scheduling::Activator {
        self.inner.activator_for(path)
    }

    fn sync_activator_for(&self, path: Vec<usize>) -> timely::scheduling::SyncActivator {
        self.inner.sync_activator_for(path)
    }
}

impl<G: AsWorker> AsWorker for LabelledScope<G> {
    fn config(&self) -> &timely::WorkerConfig {
        self.inner.config()
    }

    fn index(&self) -> usize {
        self.inner.index()
    }

    fn peers(&self) -> usize {
        self.inner.peers()
    }

    fn allocate<T: timely::communication::Exchangeable>(
        &mut self,
        identifier: usize,
        address: Rc<[usize]>,
    ) -> (
        Vec<Box<dyn timely::communication::Push<T>>>,
        Box<dyn timely::communication::Pull<T>>,
    ) {
        self.inner.allocate(identifier, address)
    }

    fn pipeline<T: 'static>(
        &mut self,
        identifier: usize,
        address: Rc<[usize]>,
    ) -> (
        timely::communication::allocator::thread::ThreadPusher<T>,
        timely::communication::allocator::thread::ThreadPuller<T>,
    ) {
        self.inner.pipeline(identifier, address)
    }

    fn broadcast<T: timely::communication::Exchangeable + Clone>(
        &mut self,
        identifier: usize,
        address: Rc<[usize]>,
    ) -> (
        Box<dyn timely::communication::Push<T>>,
        Box<dyn timely::communication::Pull<T>>,
    ) {
        self.inner.broadcast(identifier, address)
    }

    fn new_identifier(&mut self) -> usize {
        self.inner.new_identifier()
    }

    fn peek_identifier(&self) -> usize {
        self.inner.peek_identifier()
    }

    fn log_register(&self) -> Option<std::cell::RefMut<'_, timely::logging_core::Registry>> {
        self.inner.log_register()
    }

    fn logger_for<CB: timely::ContainerBuilder>(
        &self,
        name: &str,
    ) -> Option<timely::logging_core::Logger<CB>> {
        self.inner.logger_for(name)
    }

    fn logging(&self) -> Option<timely::logging::TimelyLogger> {
        self.inner.logging()
    }
}

impl<G: ScopeParent> ScopeParent for LabelledScope<G> {
    type Timestamp = G::Timestamp;
}

impl<'a, G, T> Scope for LabelledScope<Child<'a, G, T>>
where
    G: ScopeParent,
    T: Timestamp + Refines<G::Timestamp>,
{
    fn name(&self) -> String {
        self.inner.name()
    }

    fn addr(&self) -> Rc<[usize]> {
        self.inner.addr()
    }

    fn addr_for_child(&self, index: usize) -> Rc<[usize]> {
        self.inner.addr_for_child(index)
    }

    fn add_edge(&self, source: timely::progress::Source, target: timely::progress::Target) {
        self.inner.add_edge(source, target)
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.inner.allocate_operator_index()
    }

    fn add_operator_with_indices(
        &mut self,
        operator: Box<dyn Operate<Self::Timestamp>>,
        local: usize,
        global: usize,
    ) {
        let operator = LabelledOperator::new(&self.label, BoxedOperator(operator));
        self.inner
            .add_operator_with_indices(Box::new(operator), local, global)
    }

    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp + Refines<<Self as ScopeParent>::Timestamp>,
        F: FnOnce(&mut Child<Self, T2>) -> R,
    {
        let index = self.inner.subgraph.borrow_mut().allocate_child_id();
        let identifier = self.new_identifier();
        let path = self.addr_for_child(index);

        let type_name = std::any::type_name::<T2>();
        let progress_logging = self.logger_for(&format!("timely/progress/{type_name}"));
        let summary_logging = self.logger_for(&format!("timely/summary/{type_name}"));

        let subscope = RefCell::new(SubgraphBuilder::new_from(
            path,
            identifier,
            self.logging(),
            summary_logging,
            name,
        ));
        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.inner.logging.clone(),
                progress_logging,
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);
        let subscope = LabelledOperator::new(&self.label, subscope);

        self.inner
            .add_operator_with_indices(Box::new(subscope), index, identifier);

        result
    }
}

/// A wrapper around a type implementing `Operate` that sets a profiling label every time the
/// operator is scheduled.
pub struct LabelledOperator<O> {
    /// Label value to set when the operator is scheduled.
    label: String,
    /// The inner operator.
    inner: O,
}

impl<O> LabelledOperator<O> {
    fn new(label: &str, operator: O) -> Self {
        LabelledOperator {
            label: label.to_owned(),
            inner: operator,
        }
    }
}

impl<T: Timestamp, O: Operate<T>> Operate<T> for LabelledOperator<O> {
    fn inputs(&self) -> usize {
        self.inner.inputs()
    }

    fn outputs(&self) -> usize {
        self.inner.outputs()
    }

    fn get_internal_summary(
        &mut self,
    ) -> (
        timely::progress::operate::Connectivity<T::Summary>,
        Rc<RefCell<SharedProgress<T>>>,
    ) {
        self.inner.get_internal_summary()
    }

    fn local(&self) -> bool {
        self.inner.local()
    }

    fn set_external_summary(&mut self) {
        self.inner.set_external_summary()
    }

    fn notify_me(&self) -> bool {
        self.inner.notify_me()
    }
}

impl<O: Schedule> Schedule for LabelledOperator<O> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn path(&self) -> &[usize] {
        self.inner.path()
    }

    #[inline(always)]
    fn schedule(&mut self) -> bool {
        custom_labels::with_label("timely-scope", &self.label, || self.inner.schedule())
    }
}

struct BoxedOperator<T>(Box<dyn Operate<T>>);

impl<T: Timestamp> Operate<T> for BoxedOperator<T> {
    fn inputs(&self) -> usize {
        self.0.inputs()
    }

    fn outputs(&self) -> usize {
        self.0.outputs()
    }

    fn get_internal_summary(
        &mut self,
    ) -> (
        timely::progress::operate::Connectivity<<T as Timestamp>::Summary>,
        Rc<RefCell<SharedProgress<T>>>,
    ) {
        self.0.get_internal_summary()
    }

    fn local(&self) -> bool {
        self.0.local()
    }

    fn set_external_summary(&mut self) {
        self.0.set_external_summary()
    }

    fn notify_me(&self) -> bool {
        self.0.notify_me()
    }
}

impl<T> Schedule for BoxedOperator<T> {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn path(&self) -> &[usize] {
        self.0.path()
    }

    #[inline(always)]
    fn schedule(&mut self) -> bool {
        self.0.schedule()
    }
}

/// Extension trait for timely [`Scope`] that allows one to convert a scope into one that sets its
/// name as a profiling label before scheduling its child operators.
pub trait ScopeExt: Sized {
    fn with_label(&mut self) -> LabelledScope<Self>;
}

impl<S> ScopeExt for S
where
    S: Scope + ScopeParent,
{
    fn with_label(&mut self) -> LabelledScope<Self> {
        LabelledScope {
            label: self.name(),
            inner: self.clone(),
        }
    }
}
