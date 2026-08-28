// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflow fragment that tracks each dataflow's replica-wide heap usage.
//!
//! Byte counts ride in the diff field, so totalling a dataflow's operators is a consolidation.
//! The fragment attributes bytes to dataflows worker-locally and then exchanges by dataflow index,
//! which is the only exchange it performs. The operator-to-dataflow mapping is replicated on every
//! worker, so exchanging it would consolidate it to the worker count, and because `join_core`
//! multiplies input diffs that factor would scale every byte total with it.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::AsCollection;
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, Timestamp};
use mz_timely_util::columnation::ColumnationChunker;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::{Concat, Operator};
use timely::progress::Antichain;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::Update;
use crate::typedefs::spines::{ColKeyBuilder, ColValBatcher, ColValBuilder};
use crate::typedefs::{KeyBatcher, KeySpine, KeyValSpine};

#[cfg(test)]
mod tests;

/// Heap usage in bytes per dataflow, keyed by dataflow index.
///
/// A worker holds entries only for the dataflows it owns, and an entry exists only while that
/// dataflow's total is non-zero.
pub(crate) type DataflowHeapSizes = Rc<RefCell<BTreeMap<usize, Diff>>>;

/// Inputs to the watchdog dataflow fragment.
pub(super) struct Streams<'scope> {
    /// Per-operator arrangement heap size deltas, bytes in the diff field.
    pub(super) arrangement_heap_size: StreamVec<'scope, Timestamp, Update<(usize, ())>>,
    /// Per-operator batcher heap size deltas, bytes in the diff field.
    pub(super) batcher_heap_size: StreamVec<'scope, Timestamp, Update<(usize, ())>>,
    /// Maps an operator id to the index of the dataflow that contains it.
    pub(super) operator_to_dataflow: StreamVec<'scope, Timestamp, Update<(usize, usize)>>,
    /// Receives the running byte total of every dataflow this worker owns.
    pub(super) dataflow_heap_sizes: DataflowHeapSizes,
}

/// Renders the watchdog fragment.
///
/// The fragment is a sink. It maintains `dataflow_heap_sizes`, which the worker's main loop reads
/// to decide which collections have outgrown their configured limit.
///
/// NOTE: correctness rests on operator and dataflow indices being identical across a replica's
/// workers, which holds because they build their dataflows in the same order. A dataflow's total
/// is visible only on the worker owning `hash(dataflow_index)`, so a reader has to tolerate most
/// workers knowing nothing about most dataflows.
pub(super) fn construct(streams: Streams<'_>) {
    let Streams {
        arrangement_heap_size,
        batcher_heap_size,
        operator_to_dataflow,
        dataflow_heap_sizes,
    } = streams;

    // Short aliases so the arrange turbofishes fit rustfmt's line width.
    type Chunker<D> = ColumnationChunker<D>;
    // Keyed by operator id, no value: the byte totals.
    type SizeBa = KeyBatcher<usize, Timestamp, Diff>;
    type SizeBu = ColKeyBuilder<usize, Timestamp, Diff>;
    type SizeSp = KeySpine<usize, Timestamp, Diff>;
    // Keyed by operator id, valued by dataflow index: the address mapping.
    type MapBa = ColValBatcher<usize, usize, Timestamp, Diff>;
    type MapBu = ColValBuilder<usize, usize, Timestamp, Diff>;
    type MapSp = KeyValSpine<usize, usize, Timestamp, Diff>;

    // `Pipeline`, not the default exchange by key: a worker's address log covers exactly the
    // operators it logs heap sizes for, so the join is complete without moving anything, and the
    // mapping keeps the multiplicity of one that `join_core`'s diff product requires.
    let operator_to_heap_size = arrangement_heap_size
        .concat(batcher_heap_size)
        .as_collection()
        .mz_arrange_core::<_, Chunker<_>, SizeBa, SizeBu, SizeSp>(
            Pipeline,
            "Arrange watchdog operator_to_heap_size",
        );

    let operator_to_dataflow = operator_to_dataflow
        .as_collection()
        .mz_arrange_core::<_, Chunker<_>, MapBa, MapBu, MapSp>(
            Pipeline,
            "Arrange watchdog operator_to_dataflow",
        );

    let dataflow_to_heap_size = operator_to_heap_size
        .join_core(operator_to_dataflow, |_op, (), dataflow| {
            Some((*dataflow, ()))
        });

    // The one exchange. After it the owning worker holds the exact replica-wide byte total for
    // each of its dataflows.
    let exchange = ExchangeCore::new(|((dataflow, ()), _time, _diff): &Update<(usize, ())>| {
        u64::cast_from(*dataflow)
    });

    let mut pending: BTreeMap<Timestamp, Vec<(usize, Diff)>> = BTreeMap::new();
    let mut frontier = Antichain::new();

    dataflow_to_heap_size
        .inner
        .unary_frontier::<CapacityContainerBuilder<Vec<()>>, _, _, _>(
            exchange,
            "WatchdogHeapSizes",
            move |_cap, _info| {
                move |(input, chain), _output| {
                    input.for_each_time(|time, data| {
                        let entry = pending.entry(*time.time()).or_default();
                        for container in data {
                            for ((dataflow, ()), _time, diff) in container.iter() {
                                entry.push((*dataflow, *diff));
                            }
                        }
                    });

                    // Apply a time only once it is closed. Folding deltas in as they arrive would
                    // expose a total that counts an addition whose matching retraction is still in
                    // flight, and a reader acting on that would kill a dataflow that never grew.
                    frontier.clear();
                    frontier.extend(chain.frontier().iter().cloned());
                    let closed: Vec<_> = pending
                        .keys()
                        .copied()
                        .take_while(|time| !frontier.less_equal(time))
                        .collect();
                    if closed.is_empty() {
                        return;
                    }

                    let mut sizes = dataflow_heap_sizes.borrow_mut();
                    for time in closed {
                        for (dataflow, diff) in pending.remove(&time).expect("time is present") {
                            let total = sizes.entry(dataflow).or_insert(Diff::ZERO);
                            *total += diff;
                            // Drop back to absent rather than leaving a zero behind, so a
                            // torn-down dataflow's index cannot be reported later.
                            if total.is_zero() {
                                sizes.remove(&dataflow);
                            }
                        }
                    }
                }
            },
        );
}
