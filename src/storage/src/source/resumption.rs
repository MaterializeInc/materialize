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
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::Scope;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::timestamp::Timestamp as _;

use crate::controller::ResumptionFrontierCalculator;
use crate::source::source_reader_pipeline::RawSourceCreationConfig;
use mz_repr::Timestamp;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

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
        id: source_id,
        worker_count,
        worker_id,
        storage_metadata: _,
        persist_clients,
        ..
    } = config;

    let chosen_worker = (source_id.hashed() % worker_count as u64) as usize;
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("resumption({})", source_id);
    let mut resume_op = OperatorBuilder::new(operator_name, scope.clone());
    // We just downgrade the capability to communicate the frontier, and
    // don't produce any real data.
    let (_resume_output, resume_stream) = resume_op.new_output();

    let mut upper = Antichain::from_elem(Timestamp::minimum());

    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);
    resume_op.build_async(
        scope.clone(),
        move |mut capabilities, _frontiers, scheduler| async move {
            let mut cap_set = if active_worker {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                CapabilitySet::new()
            };
            // Explicitly release the unneeded capabilities!
            capabilities.clear();

            // TODO: determine what interval we want here.
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            let mut calc_state = {
                // The lock MUST be dropped before we enter the main loop.
                let mut persist_clients = persist_clients.lock().await;
                calc.initialize_state(&mut persist_clients).await
            };

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    return;
                }

                if !active_worker {
                    continue;
                }

                // Wait for the set period
                interval.tick().await;

                // Refresh the data
                let new_upper = calc.calculate_resumption_frontier(&mut calc_state).await;

                if PartialOrder::less_equal(&new_upper, &upper) {
                    continue;
                }

                tracing::trace!(
                    %source_id,
                    ?new_upper,
                    "read new resumption frontier from persist",
                );

                cap_set.downgrade(new_upper.elements());
                upper = new_upper;
                // The resumption frontier is lower bounded by the involved shards (data shard,
                // remap shard, etc.), so if it goes to empty we know that the source has finished
                // writing and can shut down.
                if upper.elements().is_empty() {
                    return;
                }
            }
        },
    );

    (resume_stream, token)
}
