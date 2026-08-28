// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A dataflow fragment that reports dataflows exceeding their heap size limit.
//!
//! The fragment consumes the heap sizes the logging fragments already collect, attributes them to
//! the dataflow hosting the reporting operator, and compares the per-dataflow total against the
//! limit the controller installed for that dataflow.
//!
//! Accounting is approximate by construction. Only instrumented allocations are visible, and they
//! are reported once per logging interval, so a dataflow can exceed its limit for up to an
//! interval before the fragment notices. The design accepts that: the limit exists to stop
//! runaway queries, not to bound memory exactly.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use mz_ore::cast::CastFrom;
use mz_repr::{Diff, Timestamp};
use mz_timely_util::columnation::ColumnationChunker;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Concatenate;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, StreamVec};

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::Update;
use crate::typedefs::spines::{ColKeyBatcher, ColKeyBuilder, ColValBatcher, ColValBuilder};
use crate::typedefs::{KeySpine, KeyValSpine};

/// The logging streams the watchdog derives its verdict from.
pub(super) struct Inputs<'scope> {
    /// Per-operator arrangement heap size deltas, in bytes carried in the diff field.
    pub arrangement_heap_size: StreamVec<'scope, Timestamp, Update<(usize, ())>>,
    /// Per-operator merge batcher heap size deltas, in bytes carried in the diff field.
    pub batcher_heap_size: StreamVec<'scope, Timestamp, Update<(usize, ())>>,
    /// Maps each arrangement operator to the dataflow hosting it, for as long as it exists.
    pub arrangement_dataflows: StreamVec<'scope, Timestamp, Update<(usize, usize)>>,
}

/// Renders the watchdog fragment.
///
/// `limits` maps a Timely dataflow index to the heap size limit in bytes the controller set for
/// it, and is maintained by the compute state as dataflows come and go. A dataflow whose total
/// heap size reaches its limit is recorded in `exceeded`, together with the size observed, which
/// the worker's main loop drains and turns into controller responses.
pub(super) fn construct<'scope>(
    scope: Scope<'scope, Timestamp>,
    inputs: Inputs<'scope>,
    limits: Rc<RefCell<BTreeMap<usize, u64>>>,
    exceeded: Rc<RefCell<BTreeMap<usize, u64>>>,
) {
    let Inputs {
        arrangement_heap_size,
        batcher_heap_size,
        arrangement_dataflows,
    } = inputs;

    // Both size streams are keyed by the global ID of the arrangement operator: Differential logs
    // a merge batcher under the ID of the operator that owns it.
    let operator_heap_size = scope.concatenate([arrangement_heap_size, batcher_heap_size]);

    // Arrange worker-locally. Every worker sees the same operator-to-dataflow mapping, so
    // exchanging it would inflate the mapping's multiplicity by the worker count, and the join's
    // diff multiplication would inflate the byte counts along with it.
    let operator_heap_size = operator_heap_size.mz_arrange_core::<
        _,
        ColumnationChunker<_>,
        ColKeyBatcher<_, _, _>,
        ColKeyBuilder<_, _, _>,
        KeySpine<usize, Timestamp, Diff>,
    >(Pipeline, "Arrange watchdog operator heap size");
    let arrangement_dataflows = arrangement_dataflows.mz_arrange_core::<
        _,
        ColumnationChunker<_>,
        ColValBatcher<_, _, _, _>,
        ColValBuilder<_, _, _, _>,
        KeyValSpine<usize, usize, Timestamp, Diff>,
    >(Pipeline, "Arrange watchdog operator dataflows");

    let dataflow_heap_size = operator_heap_size
        .join_core(arrangement_dataflows, |_operator, &(), &dataflow| {
            Some((dataflow, ()))
        });

    // Summing across workers happens here, in the exchange: every worker's subtotal for a given
    // dataflow lands on the same worker, which is the only one that can see the replica-wide
    // total and hence the only one that reports.
    let pact = Exchange::new(|((dataflow, ()), _time, _diff): &Update<(usize, ())>| {
        u64::cast_from(*dataflow)
    });

    let mut builder = OperatorBuilder::new("Watchdog: heap size limits".to_string(), scope);
    let mut input = builder.new_input(dataflow_heap_size.inner, pact);

    builder.build(move |_capabilities| {
        // Heap size deltas that the input frontier has not yet closed out. Folding them into the
        // totals early would let a transient over-count fail a query that never exceeded its
        // limit, because a retraction and the addition it cancels can arrive out of order across
        // workers.
        let mut pending: BTreeMap<Timestamp, Vec<(usize, Diff)>> = BTreeMap::new();
        let mut totals: BTreeMap<usize, Diff> = BTreeMap::new();
        // Dataflows already reported. The controller acts on the first report by failing the
        // query, so repeating it for every subsequent size change is noise.
        let mut reported: BTreeSet<usize> = BTreeSet::new();

        move |frontiers| {
            input.for_each_time(|_cap, data| {
                for ((dataflow, ()), time, diff) in
                    data.flat_map(|data: &mut Vec<_>| data.drain(..))
                {
                    pending.entry(time).or_default().push((dataflow, diff));
                }
            });

            let limits = limits.borrow();

            let frontier = &frontiers[0];
            let mut touched = BTreeSet::new();
            while let Some(entry) = pending.first_entry() {
                if frontier.less_equal(entry.key()) {
                    break;
                }
                for (dataflow, diff) in entry.remove() {
                    // A dataflow's limit is installed before it is rendered, so anything absent
                    // here will never gain a limit and is not worth accounting for.
                    if !limits.contains_key(&dataflow) {
                        continue;
                    }
                    *totals.entry(dataflow).or_default() += diff;
                    touched.insert(dataflow);
                }
            }

            // A dataflow's limit is dropped along with the dataflow, which is the signal that its
            // accounting can go too. The size updates do balance out to zero on their own, but
            // only once the drop has worked its way through the logging streams.
            totals.retain(|dataflow, _| limits.contains_key(dataflow));
            reported.retain(|dataflow| limits.contains_key(dataflow));

            if touched.is_empty() {
                return;
            }

            let mut exceeded = exceeded.borrow_mut();
            for dataflow in touched {
                let Some(&total) = totals.get(&dataflow) else {
                    continue;
                };
                let Ok(heap_size) = u64::try_from(total.into_inner()) else {
                    continue;
                };
                let limit = limits[&dataflow];
                // Strictly greater: a dataflow that lands exactly on its limit is within it.
                if heap_size > limit && reported.insert(dataflow) {
                    exceeded.insert(dataflow, heap_size);
                }
            }
        }
    });
}
