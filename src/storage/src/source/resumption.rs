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

use crate::source::source_reader_pipeline::RawSourceCreationConfig;
use mz_repr::Timestamp;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

/// Generates a timely `Stream` with no inputs that periodically
/// downgrades its output `Capability` _to the "resumption frontier"
/// of the source_. It does not produce meaningful data.
pub fn resumption_operator<G>(
    config: RawSourceCreationConfig<G>,
) -> (timely::dataflow::Stream<G, ()>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let RawSourceCreationConfig {
        id,
        scope,
        worker_count,
        worker_id,
        storage_metadata,
        persist_clients,
        envelope,
        ..
    } = config;

    let chosen_worker = (id.hashed() % worker_count as u64) as usize;
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("resumption({})", id);
    let mut resume_op = OperatorBuilder::new(operator_name, scope.clone());
    // we just downgrade the capability
    let (_resume_output, resume_stream) = resume_op.new_output();

    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    let mut upper = Antichain::from_elem(Timestamp::minimum());

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

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    return;
                }
                if !active_worker {
                    continue;
                }

                interval.tick().await;
                let new_upper = storage_metadata
                    .get_resume_upper(&persist_clients, &envelope)
                    .await;

                if PartialOrder::less_equal(&new_upper, &upper) {
                    continue;
                }

                tracing::trace!(
                    %id,
                    ?new_upper,
                    "read new resumption frontier from persist",
                );

                cap_set.downgrade(new_upper.elements());
                upper = new_upper;
            }
        },
    );

    (resume_stream, token)
}
