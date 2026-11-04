// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arranging operators that maintain their trace behind a caller-chosen agent and writer.
//!
//! Differential's `arrange_core` and `reduce_with_tactic` always build a `TraceAgent`. The copies
//! here take a factory instead, called once with an [`ArrangeContext`] when the operator is built.
//! The traits mirror the signatures proposed in TimelyDataflow/differential-dataflow#959.
//!
//! TODO: Drop these copies once differential's arranging operators accept an agent factory.

use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::{DifferentialEventBuilder, Logger};
use differential_dataflow::trace::{BatchReader, ExertionLogic, TraceReader};
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Activator;

mod arrangement;
mod reduce;

pub use arrangement::arrange_core_with;
pub use reduce::{reduce_trace_with, reduce_with_tactic_with};

/// The write endpoint an arranging operator feeds.
///
/// The operator inserts every batch it produces, in order, seals the upper when its input
/// frontier advances without data, and calls [`Writer::exert`] on every activation, including
/// activations with no new data. What happens on drop belongs to the implementation, and the
/// operators do not depend on it.
pub trait Writer {
    /// The timestamp of the batches written.
    type Time: Timestamp + Lattice;
    /// The batch type written.
    type Batch: BatchReader<Time = Self::Time>;
    /// Appends `batch`, whose lower must equal the previous upper. `hint` is a time at or below
    /// every update in the batch, suitable as a capability, or `None` for an empty batch.
    fn insert(&mut self, batch: Self::Batch, hint: Option<Self::Time>);
    /// Advances the upper to `upper` with an empty batch, if it moves.
    fn seal(&mut self, upper: Antichain<Self::Time>);
    /// Exerts merge effort on the trace, even without new batches.
    fn exert(&mut self);
}

/// A shared handle to a trace that an arranging operator maintains through [`Agent::Writer`].
///
/// Reduce reads its own output through its clone of the agent. A batch inserted through the
/// writer must be visible to that clone's `batches_through` before the operator's next read.
pub trait Agent: TraceReader + Clone {
    /// The write endpoint the arranging operator inserts batches through.
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
