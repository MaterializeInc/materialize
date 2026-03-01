// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist sink wrapper for logging collections.
//!
//! Renders a persist sink that writes logging collection data to a persist shard.
//! Reuses the MV persist sink for self-correction on replica restarts.

use std::any::Any;
use std::rc::Rc;

use columnar::Index;
use differential_dataflow::VecCollection;
use mz_compute_client::logging::LogVariant;
use mz_expr::{MirScalarExpr, permutation_for_arrangement};
use mz_repr::{Diff, GlobalId, Row, RowRef, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::columnar::Column;
use mz_timely_util::operator::CollectionExt;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Scope;
use timely::dataflow::StreamCore;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::progress::frontier::Antichain;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;
use crate::sink::materialized_view;

/// Render a persist sink for a logging collection.
///
/// Takes a flat-row stream (already converted from key-value pairs) and writes it to the
/// specified persist shard using the MV persist sink for self-correction.
///
/// Returns a token that keeps the sink alive and must be held as long as the sink should run.
pub(super) fn render<S>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: VecCollection<S, Row, Diff>,
    compute_state: &mut ComputeState,
) -> Rc<dyn Any>
where
    S: Scope<Timestamp = Timestamp>,
{
    // Empty error collection for logging (logging doesn't produce errors).
    let err_collection: VecCollection<S, DataflowError, Diff> =
        VecCollection::empty(&ok_collection.scope());

    // Start immediately - logging doesn't need dataflow suspension.
    // Drop the suspension token to fire the start signal immediately.
    let (start_signal, suspension_token) = StartSignal::new();
    drop(suspension_token);

    // Use the replica-level read-only signal. Logging persist sinks are not tracked
    // as compute collections by the controller, so they cannot use the per-collection
    // AllowWrites mechanism. Instead, they share this signal which flips to writable
    // on the first AllowWrites command from the controller.
    let read_only_rx = compute_state.read_only_rx.clone();

    // Start from the beginning of time.
    let as_of = Antichain::from_elem(Timestamp::MIN);

    materialized_view::persist_sink(
        sink_id,
        target,
        ok_collection,
        err_collection,
        as_of,
        compute_state,
        start_signal,
        read_only_rx,
    )
}

/// Flatten an arranged key-value stream and render a persist sink for it.
///
/// Combines the "Persist flatten" unary operator (which converts key-value pairs back to
/// flat rows using the arrangement permutation) with the persist sink rendering.
///
/// Returns a token that keeps the sink alive and must be held as long as the sink should run.
pub(super) fn render_arranged<S>(
    stream: &StreamCore<S, Column<((Row, Row), Timestamp, Diff)>>,
    variant: LogVariant,
    sink_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
) -> Rc<dyn Any>
where
    S: Scope<Timestamp = Timestamp>,
{
    let (permutation, arity) = arrangement_permutation(variant);
    let flat_stream = stream
        .unary::<CapacityContainerBuilder<Vec<(Row, Timestamp, Diff)>>, _, _, _>(
            Pipeline,
            &format!("Persist flatten {variant:?}"),
            |_cap, _info| {
                move |input, output| {
                    input.for_each_time(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for ((key, value), ts, diff) in
                            data.flat_map(|c| c.borrow().into_index_iter())
                        {
                            let flat_row = merge_kv_to_flat_row(key, value, &permutation, arity);
                            session.give((flat_row, ts, diff));
                        }
                    });
                }
            },
        );
    let ok_collection = differential_dataflow::Collection::new(flat_stream);
    render(sink_id, target, ok_collection, compute_state)
}

/// Compute the arrangement permutation for a log variant.
///
/// Returns `(permutation, arity)` where `permutation[original_col]` gives the position
/// in the key++value concatenation for the datum at `original_col`.
pub(super) fn arrangement_permutation(variant: LogVariant) -> (Vec<usize>, usize) {
    let desc = variant.desc();
    let arity = desc.arity();
    let key_indices = variant.index_by();
    let (permutation, _) = permutation_for_arrangement(
        &key_indices
            .iter()
            .cloned()
            .map(MirScalarExpr::column)
            .collect::<Vec<_>>(),
        arity,
    );
    (permutation, arity)
}

/// Given key and value RowRefs and the arrangement permutation, reconstruct the flat row
/// in original column order.
///
/// The `permutation` maps each original column index to its position in the concatenated
/// key++value representation. This function reverses that mapping.
pub(super) fn merge_kv_to_flat_row(
    key: &RowRef,
    value: &RowRef,
    permutation: &[usize],
    arity: usize,
) -> Row {
    // Concatenate key and value datums.
    let kv_datums: Vec<_> = key.iter().chain(value.iter()).collect();

    // Re-order into original column order using the permutation.
    let mut flat_row = Row::with_capacity(arity);
    let mut packer = flat_row.packer();
    for original_col in 0..arity {
        packer.push(kv_datums[permutation[original_col]]);
    }
    flat_row
}
