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

//! Build a nested subscope whose operators execute under a profiling label.
//!
//! Wrapping a subgraph in a [`LabelledOperator`] sets a `custom-labels`
//! thread-local for the duration of the subgraph's `schedule()` call. Because a
//! subgraph's `schedule()` transitively calls each child's `schedule()`, every
//! operator nested inside the subgraph sees the label through the thread-local,
//! so profilers can attribute CPU samples back to the originating scope without
//! wrapping each leaf operator individually.

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::Scope;
use timely::progress::operate::{Connectivity, FrontierInterest, SharedProgress};
use timely::progress::{Operate, Timestamp};
use timely::scheduling::Schedule;

/// Builds a nested subscope whose operators execute under `label` in the
/// `timely-scope` profiling slot.
///
/// The subgraph built from `func` is wrapped in a [`LabelledOperator`] before
/// being installed in the parent scope, so every `schedule()` call that runs
/// inside it observes the label via the `custom-labels` thread-local.
pub fn scoped_labelled<T, R, F>(scope: &mut Scope<T>, label: &str, func: F) -> R
where
    T: Timestamp,
    F: FnOnce(&mut Scope<T>) -> R,
{
    let (mut child, mut slot) = scope.new_subscope::<T>(label);
    let result = func(&mut child);
    let subgraph = child.build(slot.scope_mut());
    let labelled = LabelledOperator::new(label, subgraph);
    slot.install(Box::new(labelled));
    result
}

/// A wrapper around a type implementing [`Operate`] that sets a profiling label
/// every time the operator is scheduled.
pub struct LabelledOperator<O> {
    /// Label value to set when the operator is scheduled.
    label: String,
    /// The inner operator.
    inner: O,
}

impl<O> LabelledOperator<O> {
    /// Wraps `operator` so that its `schedule()` runs inside a `timely-scope`
    /// label guard set to `label`.
    pub fn new(label: &str, operator: O) -> Self {
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

    fn initialize(
        self: Box<Self>,
    ) -> (
        Connectivity<T::Summary>,
        Rc<RefCell<SharedProgress<T>>>,
        Box<dyn Schedule>,
    ) {
        let label = self.label;
        let (connectivity, shared, schedule) = Box::new(self.inner).initialize();
        (
            connectivity,
            shared,
            Box::new(LabelledSchedule {
                label,
                inner: schedule,
            }),
        )
    }

    fn local(&self) -> bool {
        self.inner.local()
    }

    fn notify_me(&self) -> &[FrontierInterest] {
        self.inner.notify_me()
    }
}

/// A wrapper around a `Schedule` that sets a profiling label before scheduling.
struct LabelledSchedule {
    label: String,
    inner: Box<dyn Schedule>,
}

impl Schedule for LabelledSchedule {
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
