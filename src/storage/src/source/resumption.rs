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

use mz_expr::PartitionId;
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
///
/// This source returns two tokens, dropping either of which signal
/// shutdown. The first is called the "downgrade" token, and is used
/// when a source needs to downgrade the operator, but full shutdown
/// is not occurring.
pub fn resumption_operator<G>(
    config: RawSourceCreationConfig<G>,
) -> (timely::dataflow::Stream<G, ()>, Rc<dyn Any>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let RawSourceCreationConfig {
        id: source_id,
        scope,
        worker_count,
        worker_id,
        storage_metadata,
        persist_clients,
        envelope,
        ..
    } = config;

    // This is the same as the calculation for single-instance workers, so that
    // the "downgrade token" can dropped on the same worker as the active worker
    let active_worker =
        crate::source::responsible_for(&source_id, worker_id, worker_count, &PartitionId::None);

    let operator_name = format!("resumption({})", source_id);
    let mut resume_op = OperatorBuilder::new(operator_name, scope.clone());
    // we just downgrade the capability
    let (_resume_output, resume_stream) = resume_op.new_output();

    let downgrade_token = Rc::new(());
    let downgrade_token_weak = Rc::downgrade(&downgrade_token);

    let shutdown_token = Rc::new(());
    let shutdown_token_weak = Rc::downgrade(&shutdown_token);

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

            let (mut remap_write, mut data_write) =
                storage_metadata.get_write_handles(&persist_clients).await;

            while scheduler.notified().await {
                if shutdown_token_weak.upgrade().is_none()
                    || downgrade_token_weak.upgrade().is_none()
                {
                    return;
                }
                if !active_worker {
                    continue;
                }

                // Wait for the set period
                interval.tick().await;

                // Refresh the data
                // TODO: add an `upper` field on `PersistClient` so we don't need to create
                // different typed `WriteHandle`s for each shard.
                remap_write.fetch_recent_upper().await;
                data_write.fetch_recent_upper().await;
                let new_upper = storage_metadata.get_resume_upper_from_handles(
                    &remap_write,
                    &data_write,
                    &envelope,
                );

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
            }
        },
    );

    (resume_stream, downgrade_token, shutdown_token)
}
