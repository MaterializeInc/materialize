// Copyright (c) 2015 Frank McSherry
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
//
// Portions of this file are derived from `operators/arrange/writer.rs` in
// differential-dataflow. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/arrange/writer.rs
//
// The crates.io release 0.26.0, which the workspace depends on, ships these sources unchanged.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.
//
// Not upstream: `crate::` paths name `differential_dataflow::`, and items
// upstream's `operators/arrange/mod.rs` defines come from `super`, which is
// `mz_timely_util::arrange_agent`. Otherwise the file is upstream's, run through rustfmt.

//! Write endpoint for a sequence of batches.
//!
//! A `TraceWriter` accepts a sequence of batches and distributes them
//! to both a shared trace and to a sequence of private queues.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use timely::progress::Antichain;

use differential_dataflow::trace::{Span, SpanOf, Trace};

use super::TraceAgentQueueWriter;
use super::TraceReplayInstruction;
use super::agent::trace_box::TraceBox;

/// Write endpoint for a sequence of batches.
///
/// A `TraceWriter` accepts a sequence of batches and distributes them
/// to both a shared trace and to a sequence of private queues.
pub struct TraceWriter<Tr: Trace> {
    /// Current upper limit.
    upper: Antichain<Tr::Time>,
    /// Shared trace, possibly absent (due to weakness).
    trace: Weak<RefCell<TraceBox<Tr>>>,
    /// A sequence of private queues into which batches are written.
    queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
}

impl<Tr: Trace> TraceWriter<Tr> {
    /// Creates a new `TraceWriter`.
    pub fn new(
        upper: Vec<Tr::Time>,
        trace: Weak<RefCell<TraceBox<Tr>>>,
        queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
    ) -> Self {
        let mut temp = Antichain::new();
        temp.extend(upper);
        Self {
            upper: temp,
            trace,
            queues,
        }
    }

    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.exert();
        }
    }

    /// Advances the trace by `span`.
    ///
    /// The `hint` argument is either `None` when the span carries no updates,
    /// or is `Some(time)` for a time less or equal to all updates in its batch
    /// and which is suitable for use as a capability.
    pub fn insert(&mut self, span: SpanOf<Tr>, hint: timely::progress::Stamp<Tr::Time>) {
        // Something is wrong if not a sequence.
        if !(&self.upper == span.lower()) {
            println!("{:?} vs {:?}", self.upper, span.lower());
        }
        assert!(&self.upper == span.lower());
        assert!(span.lower() != span.upper());

        self.upper.clone_from(span.upper());

        // push information to each listener that still exists.
        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            if let Some(pair) = queue.upgrade() {
                pair.1
                    .borrow_mut()
                    .push_back(TraceReplayInstruction::Span(span.clone(), hint.clone()));
                pair.1
                    .borrow_mut()
                    .push_back(TraceReplayInstruction::Frontier(span.upper().clone()));
                pair.0.activate();
            }
        }
        borrow.retain(|w| w.upgrade().is_some());

        // push data to the trace, if it still exists.
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.insert(span);
        }
    }

    /// Inserts an update-free span up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            self.insert(
                Span::empty(self.upper.clone(), upper),
                timely::progress::Stamp::new(),
            );
        }
    }
}

impl<Tr: Trace> Drop for TraceWriter<Tr> {
    fn drop(&mut self) {
        self.seal(Antichain::new())
    }
}
