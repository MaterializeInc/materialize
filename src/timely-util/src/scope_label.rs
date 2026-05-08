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
//!
//! [`ScopeExt::with_label`] wraps the body of a dataflow in a same-timestamp subgraph and
//! installs that subgraph as a `LabelledOperator`. Whenever the resulting operator is
//! scheduled, its child operators are scheduled inside a `custom_labels` scope that sets
//! `timely-scope=<scope name>`. CPU profilers that read the `custom_labels` thread-local state
//! (e.g., PolarSignals) attribute samples taken during the subgraph's execution to that label.
//!
//! The previous in-place `Scope` wrapper (pre-timely-v4) is no longer expressible against
//! the v4 API, since `Scope` is now a concrete struct. The current implementation introduces
//! one extra subgraph layer per call site, with the associated progress-tracking overhead.

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::Scope;
use timely::progress::operate::{Connectivity, FrontierInterest, SharedProgress};
use timely::progress::{Operate, Timestamp};
use timely::scheduling::Schedule;

/// Wraps an [`Operate`] so that each `schedule()` call on the initialized object runs within
/// a `custom_labels` scope tagged `timely-scope=<label>`.
struct LabelledOperator<O> {
    label: String,
    inner: O,
}

impl<T: Timestamp, O: Operate<T>> Operate<T> for LabelledOperator<O> {
    fn local(&self) -> bool {
        self.inner.local()
    }

    fn inputs(&self) -> usize {
        self.inner.inputs()
    }

    fn outputs(&self) -> usize {
        self.inner.outputs()
    }

    fn notify_me(&self) -> &[FrontierInterest] {
        self.inner.notify_me()
    }

    fn initialize(
        self: Box<Self>,
    ) -> (
        Connectivity<T::Summary>,
        Rc<RefCell<SharedProgress<T>>>,
        Box<dyn Schedule>,
    ) {
        let LabelledOperator { label, inner } = *self;
        let (connectivity, progress, schedule) = Box::new(inner).initialize();
        let labelled = LabelledSchedule {
            label,
            inner: schedule,
        };
        (connectivity, progress, Box::new(labelled))
    }
}

/// Wrapper around an initialized [`Schedule`] that sets a profiling label on each call.
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

/// Extension trait for timely [`Scope`] that runs `func` inside a labelled subgraph,
/// causing CPU profile samples taken during operator scheduling within `func` to be
/// attributed to the parent scope's name via the `timely-scope` label.
pub trait ScopeExt<T: Timestamp>: Sized {
    /// Runs `func` inside a labelled same-timestamp subgraph using the current scope's name.
    fn with_label<R, F>(self, func: F) -> R
    where
        F: for<'a> FnOnce(Scope<'a, T>) -> R;
}

impl<T: Timestamp> ScopeExt<T> for Scope<'_, T> {
    fn with_label<R, F>(self, func: F) -> R
    where
        F: for<'a> FnOnce(Scope<'a, T>) -> R,
    {
        let label = self.name();
        // `scoped_raw` lets us intercept the to-be-installed subgraph and wrap it before
        // handing it to `OperatorSlot::install`. This is the only v4 hook that gives access
        // to the operator object behind a child scope.
        let (result, subgraph, slot) = self.scoped_raw::<T, R, _>(&label, func);
        slot.install(Box::new(LabelledOperator {
            label,
            inner: subgraph,
        }));
        result
    }
}
