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
use timely::progress::{Operate, Subgraph, SubgraphBuilder, Timestamp};
use timely::scheduling::Schedule;

/// A wrapper around a timely [`Subgraph`] that sets a profiling label every
/// time the subgraph is scheduled.
pub struct LabelledSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    /// Label value to set when the subgraph is scheduled.
    label: String,
    /// The inner subgraph.
    inner: Subgraph<TOuter, TInner>,
}

impl<TOuter, TInner> LabelledSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    /// Creates a new labelled subgraph that will set `inner.name()` when scheduled.
    pub fn new(inner: Subgraph<TOuter, TInner>) -> Self {
        Self {
            label: inner.name().to_owned(),
            inner,
        }
    }
}

impl<TOuter, TInner> Schedule for LabelledSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn path(&self) -> &[usize] {
        self.inner.path()
    }

    fn schedule(&mut self) -> bool {
        custom_labels::with_label("timely-scope", &self.label, || self.inner.schedule())
    }
}

impl<TOuter, TInner> Operate<TOuter> for LabelledSubgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
{
    fn local(&self) -> bool {
        self.inner.local()
    }

    fn inputs(&self) -> usize {
        self.inner.inputs()
    }

    fn outputs(&self) -> usize {
        self.inner.outputs()
    }

    fn get_internal_summary(
        &mut self,
    ) -> (
        timely::progress::operate::Connectivity<TOuter::Summary>,
        Rc<RefCell<SharedProgress<TOuter>>>,
    ) {
        self.inner.get_internal_summary()
    }

    fn set_external_summary(&mut self) {
        self.inner.set_external_summary();
    }

    fn notify_me(&self) -> bool {
        self.inner.notify_me()
    }
}

/// Extension trait for timely [`Scope`] that adds a variant of `region` which
/// builds a subgraph wrapped in [`LabelledSubgraph`], so the region name is set
/// as a profiling label every time the subgraph is scheduled.
pub trait ScopeExt {
    /// Creates a dataflow subgraph whose schedule step runs with a profiling
    /// label set to `name`.
    fn region_labelled<R, F>(&mut self, name: &str, func: F) -> R
    where
        Self: ScopeParent,
        F: FnOnce(&mut Child<'_, Self, <Self as ScopeParent>::Timestamp>) -> R;
}

impl<S> ScopeExt for S
where
    S: Scope + ScopeParent,
{
    fn region_labelled<R, F>(&mut self, name: &str, func: F) -> R
    where
        Self: ScopeParent,
        F: FnOnce(&mut Child<'_, Self, <Self as ScopeParent>::Timestamp>) -> R,
    {
        let index = self.allocate_operator_index();
        let identifier = self.new_identifier();
        let path = self.addr_for_child(index);

        let type_name = std::any::type_name::<<Self as ScopeParent>::Timestamp>();
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
                logging: self.logging(),
                progress_logging,
            };
            func(&mut builder)
        };

        let subgraph = subscope.into_inner().build(self);
        let labelled = LabelledSubgraph::new(subgraph);
        self.add_operator_with_indices(Box::new(labelled), index, identifier);

        result
    }
}
