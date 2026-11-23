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
// Portions of this file are derived from `operators/arrange/mod.rs` in
// differential-dataflow. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/arrange/mod.rs
//
// The crates.io release 0.26.0, which the workspace depends on, ships these sources unchanged.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! A Materialize-owned copy of differential's trace agent and writer, and of the arranging
//! operators that construct one.
//!
//! [`TraceAgent`] and [`TraceWriter`] are copies of differential's types of the same name.
//! [`arrange_core_with`] and [`reduce_with_tactic_with`] are copies of differential's
//! `arrange_core` and `reduce_with_tactic` that take a factory instead of building a
//! `TraceAgent`, called once with an [`ArrangeContext`] when the operator is built.
//! [`TraceAgent::factory`] builds the agent and writer exactly as upstream does. The [`Writer`]
//! and [`Agent`] traits mirror the signatures proposed in
//! TimelyDataflow/differential-dataflow#959.
//!
//! NOTE: The submodules are copies, not dependencies. Bumping differential-dataflow does not
//! update them, so review upstream's `operators/arrange/{agent,writer,arrangement}.rs`,
//! `operators/reduce.rs` and `operators/cursor/reduce.rs` on every bump and re-derive the copies
//! from the revision their headers name. The copies are run through rustfmt, so diff them
//! against a rustfmt-formatted upstream. Every intentional deviation carries a `Not upstream:`
//! comment.
//!
//! TODO: Drop these copies once differential's arranging operators accept an agent factory.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::{DifferentialEventBuilder, Logger};
use differential_dataflow::operators::arrange::{Arranged, TraceReplayInstruction};
use differential_dataflow::trace::{ExertionLogic, Span, Trace, TraceReader};
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, Stamp, Timestamp};
use timely::scheduling::Activator;

mod agent;
mod arrangement;
mod reduce;
mod writer;

pub use agent::TraceAgent;
pub use agent::trace_box;
pub use arrangement::arrange_core_with;
pub use reduce::{reduce_trace_with, reduce_with_tactic_with};
pub use writer::TraceWriter;

// Short names for strongly and weakly owned activators and shared queues.
type BatchQueue<Tr> = VecDeque<TraceReplayInstruction<Tr>>;
type TraceAgentQueueReader<Tr> = Rc<(Activator, RefCell<BatchQueue<Tr>>)>;
type TraceAgentQueueWriter<Tr> = Weak<(Activator, RefCell<BatchQueue<Tr>>)>;

/// The write endpoint an arranging operator feeds.
///
/// The operator inserts every span it produces, in order, seals the upper to the frontier it
/// has finished, whether or not it inserted a span up to there, and calls [`Writer::exert`] on
/// every activation, including activations with no new data. What happens on drop belongs to
/// the implementation, and the operators do not depend on it.
pub trait Writer {
    /// The timestamp of the spans written.
    type Time: Timestamp + Lattice;
    /// The batch type the spans carry.
    type Batch;
    /// Appends `span`, whose lower must equal the previous upper. `hint` holds times at or below
    /// every update in the span, each suitable as a capability. It is empty only for a span
    /// without updates.
    fn insert(&mut self, span: Span<Self::Time, Self::Batch>, hint: Stamp<Self::Time>);
    /// Advances the upper to `upper` with an update-free span, if it moves.
    fn seal(&mut self, upper: Antichain<Self::Time>);
    /// Exerts merge effort on the trace, even without new spans.
    fn exert(&mut self);
}

/// A shared handle to a trace that an arranging operator maintains through [`Agent::Writer`].
///
/// Reduce reads its own output through its clone of the agent. A span inserted through the
/// writer must be visible to that clone's `spans_through` before the operator's next read.
pub trait Agent: TraceReader + Clone {
    /// The write endpoint the arranging operator inserts spans through.
    type Writer: Writer<Time = Self::Time, Batch = Self::Batch> + 'static;
}

/// What an arranging operator offers the factory that builds its agent and writer.
pub struct ArrangeContext {
    /// The arranging operator.
    pub info: OperatorInfo,
    /// Schedules the arranging operator.
    pub activator: Activator,
    /// The worker's `differential/arrange` logger, if any.
    pub logger: Option<Logger>,
    /// The worker's `differential/default_exert_logic`, if set.
    pub exert_logic: Option<ExertionLogic>,
}

impl ArrangeContext {
    /// Collects the context for the operator `info` in `scope`.
    fn new<T: Timestamp>(scope: &Scope<'_, T>, info: OperatorInfo) -> Self {
        let logger = scope
            .worker()
            .logger_for::<DifferentialEventBuilder>("differential/arrange")
            .map(Into::into);
        let activator = scope.activator_for(Rc::clone(&info.address));
        let exert_logic = scope
            .worker()
            .config()
            .get::<ExertionLogic>("differential/default_exert_logic")
            .cloned();
        ArrangeContext {
            info,
            activator,
            logger,
            exert_logic,
        }
    }
}

impl<Tr: Trace + 'static> TraceAgent<Tr> {
    /// A factory that builds an empty `Tr` and its agent and writer, as differential's arranging
    /// operators do.
    pub fn factory() -> impl FnOnce(ArrangeContext) -> (Self, TraceWriter<Tr>) {
        |context: ArrangeContext| {
            let ArrangeContext {
                info,
                activator,
                logger,
                exert_logic,
            } = context;
            let mut trace = Tr::new(info.clone(), logger.clone(), Some(activator));
            if let Some(exert_logic) = exert_logic {
                trace.set_exert_logic(exert_logic);
            }
            TraceAgent::new(trace, info, logger)
        }
    }
}

impl<Tr: Trace + 'static> Agent for TraceAgent<Tr> {
    type Writer = TraceWriter<Tr>;
}

impl<Tr: Trace> Writer for TraceWriter<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn insert(&mut self, span: Span<Tr::Time, Tr::Batch>, hint: Stamp<Tr::Time>) {
        TraceWriter::insert(self, span, hint)
    }

    fn seal(&mut self, upper: Antichain<Tr::Time>) {
        TraceWriter::seal(self, upper)
    }

    fn exert(&mut self) {
        TraceWriter::exert(self)
    }
}
