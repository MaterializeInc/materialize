// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An async timely operator that reclocks source data arriving via a tokio channel.
//!
//! This is an alternative to [`mz_timely_util::reclock::reclock`] for sources that run as async
//! tasks outside of timely and communicate via channels. The core reclocking algorithm is identical
//! — each source update at `FromTime` is mapped to the unique `IntoTime` determined by the remap
//! collection — but data arrives from an [`mpsc::UnboundedReceiver`] instead of timely's capture
//! mechanism.
//!
//! The operator is designed for **single-worker** use: only one timely worker should instantiate it,
//! receiving all source data on one channel.

use std::cmp::Reverse;
use std::collections::binary_heap::BinaryHeap;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{VecCollection, consolidation};
use mz_ore::Overflowing;
use mz_ore::collections::CollectionExt;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::mpsc;

/// A batch of source data sent from an async source task to the dataflow.
#[derive(Debug)]
pub struct SourceBatch<D, FromTime, R> {
    /// The data updates, each at a specific `FromTime`.
    pub updates: Vec<(D, FromTime, R)>,
    /// The source frontier after this batch. Must be monotonically increasing across batches.
    /// This replaces `Event::Progress` from timely's capture mechanism.
    pub frontier: Antichain<FromTime>,
}

/// Constructs an async timely operator that reclocks data arriving from a tokio channel using
/// remap bindings from a timely collection.
///
/// The remap collection provides `(FromTime, IntoTime, Diff)` bindings that define the mapping
/// between source time and system time. Source data arrives via `source_rx` as [`SourceBatch`]
/// values containing updates and frontier information.
///
/// Returns the reclocked stream at `IntoTime` and a shutdown token.
///
/// The output is a raw timely stream of `(D, IntoTime, R)` tuples rather than a differential
/// collection, because `D` may not satisfy the `Ord` bound required by differential's
/// `Collection` type. The caller is responsible for any further transformation.
pub fn channel_reclock<G, D, FromTime, R>(
    remap_collection: VecCollection<G, FromTime, Overflowing<i64>>,
    as_of: Antichain<G::Timestamp>,
    source_rx: mpsc::UnboundedReceiver<SourceBatch<D, FromTime, R>>,
) -> (
    timely::dataflow::StreamVec<G, (D, G::Timestamp, R)>,
    PressOnDropButton,
)
where
    G: Scope,
    G::Timestamp: Timestamp + Lattice + TotalOrder,
    D: Clone + Send + Sync + 'static,
    FromTime: Timestamp,
    R: Clone + Send + Sync + Semigroup + 'static,
{
    let mut builder =
        AsyncOperatorBuilder::new("ChannelReclock".to_string(), remap_collection.scope());
    let (output_handle, output_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(D, G::Timestamp, R)>>>();
    let mut remap_input = builder.new_input_for(remap_collection.inner, Pipeline, &output_handle);

    let button = builder.build(move |capabilities| {
        let as_of = as_of;
        let mut source_rx = source_rx;

        async move {
            let mut cap_set: CapabilitySet<G::Timestamp> =
                CapabilitySet::from_elem(capabilities.into_element());
            cap_set.downgrade(&as_of.borrow());

            // -- Remap binding state --
            // Bindings received but not yet finalized (IntoTime >= remap_upper).
            let mut pending_remap: BinaryHeap<Reverse<(G::Timestamp, FromTime, i64)>> =
                BinaryHeap::new();
            // Accumulation buffer for consolidating remap input updates.
            let mut remap_accum: ChangeBatch<(G::Timestamp, FromTime)> = ChangeBatch::new();
            // Finalized bindings sorted by IntoTime. Correctly accumulated for times in
            // [remap_since, remap_upper).
            let mut remap_trace: Vec<(FromTime, G::Timestamp, i64)> = Vec::new();
            let mut remap_upper: Antichain<G::Timestamp> =
                Antichain::from_elem(G::Timestamp::minimum());
            let mut remap_since: Antichain<G::Timestamp> = as_of.clone();

            // -- Source data state --
            // Updates waiting for matching remap bindings.
            let mut deferred: Vec<(D, FromTime, R)> = Vec::new();
            // The frontier of the source channel.
            let mut source_frontier: MutableAntichain<FromTime> =
                MutableAntichain::from_elem(FromTime::minimum());
            // The last reported source frontier (for computing diffs).
            let mut last_source_frontier: Antichain<FromTime> =
                Antichain::from_elem(FromTime::minimum());

            // Scratch buffer for building up the current binding antichain.
            let mut binding_buffer: Vec<(FromTime, i64)> = Vec::new();

            // Whether the source channel is still open.
            let mut source_open = true;

            while !cap_set.is_empty() {
                // Wait for either remap bindings or source data.
                // Biased toward remap so bindings are available before we try to reclock data.
                if source_open {
                    tokio::select! {
                        biased;
                        _ = remap_input.ready() => {}
                        batch = source_rx.recv() => {
                            match batch {
                                Some(batch) => {
                                    deferred.extend(batch.updates);
                                    // Update source frontier via diff against last frontier.
                                    let changes: Vec<(FromTime, i64)> = last_source_frontier
                                        .iter()
                                        .map(|t| (t.clone(), -1))
                                        .chain(batch.frontier.iter().map(|t| (t.clone(), 1)))
                                        .collect();
                                    source_frontier.update_iter(changes);
                                    last_source_frontier = batch.frontier;
                                }
                                None => {
                                    // Channel closed — advance source frontier to empty.
                                    // This signals "no more data from this worker" which is
                                    // correct for both non-active workers (whose channels close
                                    // immediately) and the active worker (whose task completed).
                                    // The reclocked frontier will advance to remap_upper,
                                    // allowing the persist sink to make progress.
                                    let changes: Vec<(FromTime, i64)> = last_source_frontier
                                        .iter()
                                        .map(|t| (t.clone(), -1))
                                        .collect();
                                    source_frontier.update_iter(changes);
                                    last_source_frontier = Antichain::new();
                                    source_open = false;
                                }
                            }
                        }
                    }
                } else {
                    // Source closed, only wait for remap input.
                    remap_input.ready().await;
                }

                // -- STEP 1: Drain remap input into pending_remap --
                while let Some(event) = remap_input.next_sync() {
                    match event {
                        AsyncEvent::Data(_, data) => {
                            for (from, into, diff) in data {
                                let mut into: G::Timestamp = into;
                                into.advance_by(as_of.borrow());
                                let diff: Overflowing<i64> = diff;
                                remap_accum.update((into, from), diff.into_inner());
                            }
                        }
                        AsyncEvent::Progress(frontier) => {
                            remap_upper = frontier;
                        }
                    }
                }
                // Only drain accumulated bindings once the remap frontier has passed as_of.
                if !timely::PartialOrder::less_equal(&remap_upper.borrow(), &as_of.borrow()) {
                    for ((into, from), diff) in remap_accum.drain() {
                        pending_remap.push(Reverse((into, from, diff)));
                    }
                }

                // -- STEP 2: Extract finalized bindings into remap_trace --
                while let Some(update) = pending_remap.peek_mut() {
                    if !remap_upper.less_equal(&update.0.0) {
                        let Reverse((into, from, diff)) =
                            std::collections::binary_heap::PeekMut::pop(update);
                        remap_trace.push((from, into, diff));
                    } else {
                        break;
                    }
                }

                // -- STEP 3+4: Reclock updates --
                // We can only accumulate bindings correctly for times beyond remap_since.
                if remap_since.iter().all(|t| !remap_upper.less_equal(t)) {
                    let Some(cap) = cap_set.first().cloned() else {
                        break;
                    };

                    let mut cur_binding: MutableAntichain<FromTime> = MutableAntichain::new();
                    let mut reclocked_source_frontier: Antichain<G::Timestamp> =
                        remap_upper.clone();
                    let mut frontier_reclocked = false;

                    // Build the list of interesting IntoTimes from the remap trace.
                    //
                    // In the original reclock operator, `min_time` is always included because
                    // the remap trace's first binding is AT `as_of` (the minimum time). In the
                    // channel-based path, the first binding may be at a wall-clock timestamp
                    // much later than `as_of`. Including `min_time` when no trace entries exist
                    // at that time causes data to be reclocked to `min_time` (empty binding
                    // frontier → extract everything). We only include `min_time` when the trace
                    // is empty, matching the original's semantics for the bootstrap case.
                    let mut min_time = G::Timestamp::minimum();
                    min_time.advance_by(remap_since.borrow());

                    let min_time_iter: Box<dyn Iterator<Item = G::Timestamp>> =
                        if remap_trace.is_empty() {
                            Box::new(std::iter::once(min_time))
                        } else {
                            Box::new(std::iter::empty())
                        };

                    // Deduplicate consecutive times.
                    let mut prev_cur_time: Option<G::Timestamp> = None;
                    let interesting_times: Vec<G::Timestamp> = min_time_iter
                        .chain(remap_trace.iter().map(|(_, t, _)| t.clone()))
                        .filter(|v| {
                            let dominated = prev_cur_time.as_ref() == Some(v);
                            prev_cur_time = Some(v.clone());
                            !dominated
                        })
                        .collect();

                    // Sort deferred updates by FromTime for efficient extraction.
                    deferred.sort_unstable_by(|(_, t1, _), (_, t2, _)| t1.cmp(t2));

                    let mut remap_iter = remap_trace.iter().peekable();

                    for cur_time in &interesting_times {
                        if deferred.is_empty() && frontier_reclocked {
                            break;
                        }

                        // 4.0: Load bindings for cur_time into cur_binding.
                        while let Some((t_from, _, diff)) =
                            remap_iter.next_if(|(_, t, _)| t == cur_time)
                        {
                            binding_buffer.push((t_from.clone(), *diff));
                        }
                        cur_binding.update_iter(binding_buffer.drain(..));
                        let binding_frontier = cur_binding.frontier();

                        // 4.1+4.2: Extract matching updates from deferred.
                        // An update at `from_time` maps to `cur_time` if
                        // `!binding_frontier.less_equal(&from_time)`.
                        let mut i = 0;
                        while i < deferred.len() {
                            if !binding_frontier.less_equal(&deferred[i].1) {
                                let (data, _, diff) = deferred.swap_remove(i);
                                output_handle
                                    .give(&cap.delayed(cur_time), (data, cur_time.clone(), diff));
                                // Don't increment i — swap_remove moved the last element here.
                            } else {
                                i += 1;
                            }
                        }

                        // 4.3: Reclock source frontier.
                        // If any FromTime in source frontier is not beyond the binding frontier,
                        // then this is the first IntoTime at which data could still arrive.
                        if !frontier_reclocked
                            && source_frontier
                                .frontier()
                                .iter()
                                .any(|t| !binding_frontier.less_equal(t))
                        {
                            reclocked_source_frontier.insert(cur_time.clone());
                            frontier_reclocked = true;
                        }
                    }

                    // -- STEP 5: Downgrade capability and compact remap trace --
                    cap_set.downgrade(&reclocked_source_frontier.borrow());
                    remap_since = reclocked_source_frontier;
                    for (_, into_time, _) in remap_trace.iter_mut() {
                        into_time.advance_by(remap_since.borrow());
                    }
                    consolidation::consolidate_updates(&mut remap_trace);
                    remap_trace.sort_unstable_by(|(_, t1, _), (_, t2, _)| t1.cmp(t2));

                    // Shrink if using less than a quarter of capacity.
                    if remap_trace.len() < remap_trace.capacity() / 4 {
                        remap_trace.shrink_to(remap_trace.len() * 2);
                    }
                    if deferred.len() < deferred.capacity() / 4 {
                        deferred.shrink_to(deferred.len() * 2);
                    }
                }
            }
        }
    });

    (output_stream, button.press_on_drop())
}
