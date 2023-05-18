// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the "Resumption Frontier Operator".
//! See [`resumption_operator`] for more info.
//!
//! TODO(guswynn): link to design doc when its merged

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;
use mz_storage_client::controller::ResumptionFrontierCalculator;
use mz_timely_util::builder_async::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::Scope;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::timestamp::Timestamp as _;

use crate::source::source_reader_pipeline::RawSourceCreationConfig;

/// Generates a timely `Stream` with no inputs that periodically
/// downgrades its output `Capability` _to the "resumption frontier"
/// of the source_. It does not produce meaningful data.
///
/// The returned feedback `Handle` is to allow the downstream operator to
/// communicate a frontier back to this operator, so we can shutdown when that
/// frontier becomes the empty antichain.
///
/// This is useful when a source is finite or finishes for other reasons.
pub fn resumption_operator<G, R>(
    scope: &G,
    config: RawSourceCreationConfig,
    calc: R,
) -> (timely::dataflow::Stream<G, ()>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp> + Clone,
    R: ResumptionFrontierCalculator<Timestamp> + 'static,
{
    let RawSourceCreationConfig {
        id,
        worker_count,
        worker_id,
        storage_metadata: _,
        persist_clients,
        ..
    } = config;

    let chosen_worker = usize::cast_from(id.hashed() % u64::cast_from(worker_count));
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("resumption({})", id);
    let mut resume_op = OperatorBuilder::new(operator_name, scope.clone());
    // We just downgrade the capability to communicate the frontier, and
    // don't produce any real data.
    let (_resume_output, resume_stream) = resume_op.new_output();

    let mut upper = Antichain::from_elem(Timestamp::minimum());

    let button = resume_op.build(move |mut capabilities| async move {
        if !active_worker {
            return;
        }
        let mut cap_set = CapabilitySet::from_elem(capabilities.pop().expect("missing capability"));
        // We only have one output
        assert!(capabilities.is_empty());

        // TODO: determine what interval we want here.
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Clear the first instantaneous tick
        // See <https://docs.rs/tokio/latest/tokio/time/struct.Interval.html#method.tick>
        interval.tick().await;

        let mut calc_state = calc.initialize_state(&persist_clients).await;

        while !upper.is_empty() {
            interval.tick().await;

            // Get a new lower bound for the resumption frontier
            let new_upper = calc.calculate_resumption_frontier(&mut calc_state).await;

            if PartialOrder::less_than(&upper, &new_upper) {
                tracing::debug!(
                    resumption_frontier = ?new_upper,
                    "resumption({id}) {worker_id}/{worker_count}: calculated \
                    new resumption frontier",
                );

                cap_set.downgrade(&*new_upper);
                upper = new_upper;
            }
        }
    });

    (resume_stream, Rc::new(button.press_on_drop()))
}
