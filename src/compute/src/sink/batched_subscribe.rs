// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The batched subscribe sink.
//!
//! The sink reports its input as a chain of descriptions `[lower, upper)`, the first starting at
//! the dataflow's as-of, each later one at the previous upper. It renders three pieces:
//!
//! * Every worker arranges the ok and error inputs and holds the batches the arrangements emit.
//!   It drops the arrangements' traces, so no trace outlives the held batches.
//! * A mint operator on one worker cuts descriptions at its input frontier, clamped to `up_to`,
//!   and broadcasts them.
//! * A write operator on every worker answers each description in the order it was minted. Once
//!   its own input frontiers have reached the description's upper, it scans the held batches the
//!   description overlaps, reports the worker's share, and releases the batches the description
//!   covered.
//!
//! Every worker reports exactly one response per description, including an empty one, because the
//! partitioned merge pairs the k-th response of every worker.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, TraceReader};
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use mz_compute_client::protocol::response::{
    BatchedSubscribeBatch, BatchedSubscribeResponse, Share,
};
use mz_compute_types::sinks::{BatchedSubscribeSinkConnection, ComputeSinkDesc};
use mz_expr::{MapFilterProject, SafeMfpPlan};
use mz_ore::cast::CastFrom;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::{Diff, GlobalId, Row, Timestamp, UpdateCollection};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::probe::{Handle, ProbeNotify};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::vec::Broadcast;
use timely::progress::{Antichain, frontier::AntichainRef};

use crate::compute_state::description_iterator::DescriptionRange;
use crate::compute_state::description_scan::{DescriptionScan, DescriptionScanOutcome};
use crate::compute_state::peek_budget::InlineBudget;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

/// A batch of the sink's ok arrangement.
type OkBatch = <RowRowSpine<Timestamp, Diff> as TraceReader>::Batch;
/// A batch of the sink's error arrangement.
type ErrBatch = <ErrSpine<Timestamp, Diff> as TraceReader>::Batch;

/// The buffer the sink reports through, drained by `ComputeState::process_batched_subscribes`.
type ResponseBuffer = Rc<RefCell<Vec<(GlobalId, BatchedSubscribeResponse)>>>;

/// The bytes a scan buffers before it hands them out. Hand-outs are consolidated, which bounds the
/// buffer for updates that cancel within a description.
const SCAN_CUT_BYTES: usize = 1 << 20;

impl<'scope> SinkRender<'scope> for BatchedSubscribeSinkConnection {
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
        output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        let sinked_collection = sinked_collection
            .inner
            .probe_notify_with(vec![output_probe.clone()])
            .as_collection();

        let Arranged {
            stream: ok_batches,
            trace,
        } = sinked_collection
            .map(|row| (row, Row::default()))
            .mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("BatchedSubscribe oks");
        drop(trace);
        let Arranged {
            stream: err_batches,
            trace,
        } = KeyCollection::from(err_collection.clone())
            .mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("BatchedSubscribe errs");
        drop(trace);

        let descriptions = mint(
            sink_id,
            as_of.clone(),
            sink.up_to.clone(),
            ok_batches.clone(),
            err_collection,
        );

        let protocol = Rc::new(RefCell::new(Some(Protocol {
            sink_id,
            buffer: Some(Rc::clone(&compute_state.batched_subscribe_response_buffer)),
            upper: as_of,
            poison: None,
        })));
        let protocol_weak = Rc::downgrade(&protocol);
        // The plan applies to rows of `from_desc`.
        let map_filter_project = MapFilterProject::new(sink.from_desc.arity())
            .into_plan()
            .expect("an identity plan is valid")
            .into_nontemporal()
            .expect("an identity plan is not temporal");
        write(
            sink_id,
            descriptions.broadcast(),
            ok_batches,
            err_batches,
            Writer::new(
                sink_id,
                map_filter_project,
                InlineBudget::new(&compute_state.worker_config),
                protocol,
            ),
        );

        // Dropping the protocol reports the drop and keeps the write operator from reporting
        // anything while the dataflow shuts down.
        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(protocol) = protocol_weak.upgrade() {
                drop(protocol.borrow_mut().take());
            }
        })))
    }
}

/// A description the mint operator cuts, broadcast to every worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Description {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    /// Whether this is the first description, which also emits the updates before `lower`,
    /// advanced to `lower`.
    first: bool,
    /// Whether the description completes the subscribe at `up_to`. It holds no updates.
    completion: bool,
}

impl Description {
    fn range(&self) -> DescriptionRange {
        if self.first {
            DescriptionRange::First {
                as_of: self.lower.clone(),
                upper: self.upper.clone(),
            }
        } else {
            DescriptionRange::Later {
                lower: self.lower.clone(),
                upper: self.upper.clone(),
            }
        }
    }
}

/// Cuts the chain of descriptions of a subscribe rendered at an as-of.
#[derive(Debug)]
struct Minter {
    /// The lower of the next description.
    lower: Antichain<Timestamp>,
    /// Whether the next description is the first.
    first: bool,
    up_to: Antichain<Timestamp>,
    /// The error updates no minted description covers yet.
    errors: Vec<(DataflowErrorSer, Timestamp, Diff)>,
    /// Whether the chain has ended, by completion, by an empty input frontier, or by an error.
    done: bool,
}

impl Minter {
    fn new(as_of: Antichain<Timestamp>, up_to: Antichain<Timestamp>) -> Self {
        Self {
            lower: as_of,
            first: true,
            up_to,
            errors: Vec::new(),
            done: false,
        }
    }

    /// Returns the next description, given that the input is complete before `frontier`, or
    /// `None` if the frontier allows none.
    ///
    /// `self.errors` must hold every error update before `frontier`.
    fn mint(&mut self, frontier: AntichainRef<Timestamp>) -> Option<Description> {
        if self.done {
            return None;
        }
        // Reached after the description with upper `up_to`, or right away when `up_to` is not
        // beyond the as-of. The first case continues the chain at `up_to`. The second has
        // nothing to report, and `[as_of, ∅)` is the gapless way to end a chain starting at the
        // as-of.
        if !self.up_to.is_empty() && PartialOrder::less_equal(&self.up_to, &self.lower) {
            self.done = true;
            return Some(Description {
                lower: std::mem::replace(&mut self.lower, Antichain::new()),
                upper: Antichain::new(),
                first: self.first,
                completion: true,
            });
        }

        let upper = if PartialOrder::less_equal(&self.up_to.borrow(), &frontier) {
            self.up_to.clone()
        } else {
            frontier.to_owned()
        };
        if !PartialOrder::less_than(&self.lower, &upper) {
            return None;
        }

        let poisoned = self.take_poison(&upper);
        self.done = poisoned || upper.is_empty();
        Some(Description {
            lower: std::mem::replace(&mut self.lower, upper.clone()),
            upper,
            first: std::mem::replace(&mut self.first, false),
            completion: false,
        })
    }

    /// Removes the error updates before `upper` and returns whether any error accumulates to
    /// nonzero across them, which poisons the description ending at `upper`.
    ///
    /// Every error update before the description's lower was removed when the previous
    /// description was minted, so the updates removed here are exactly those the description
    /// covers. The first description covers all updates before its upper.
    fn take_poison(&mut self, upper: &Antichain<Timestamp>) -> bool {
        let mut covered = Vec::new();
        self.errors.retain(|(error, time, diff)| {
            let before = !upper.less_equal(time);
            if before {
                covered.push((error.clone(), *diff));
            }
            !before
        });
        consolidate(&mut covered);
        !covered.is_empty()
    }
}

/// Renders the mint operator, which runs on one worker and emits the descriptions it cuts.
///
/// Its input frontier is the meet of the ok arrangement's and the error input's frontiers.
fn mint<'scope>(
    sink_id: GlobalId,
    as_of: Antichain<Timestamp>,
    up_to: Antichain<Timestamp>,
    ok_batches: Stream<'scope, Timestamp, Vec<OkBatch>>,
    errs: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
) -> Stream<'scope, Timestamp, Vec<Description>> {
    let scope = ok_batches.scope();
    let mint_worker = usize::cast_from(sink_id.hashed()) % scope.peers();
    let is_mint_worker = scope.index() == mint_worker;

    let mut op = OperatorBuilder::new(format!("BatchedSubscribeMint({sink_id})"), scope);
    let (output, descriptions) = op.new_output();
    let mut output = OutputBuilder::<_, CapacityContainerBuilder<Vec<Description>>>::from(output);
    // Only the frontier of the ok batches matters here.
    let mut ok_input = op.new_input(ok_batches, Pipeline);
    // NOTE: The error collection is teed, since every worker also arranges it, and this copy
    // takes a non-key exchange. The mint worker sees every error update, so it knows which
    // description an error poisons and mints nothing after it. Error updates are rare, so the
    // copy and the routing to one worker cost little.
    let mint_target = u64::cast_from(mint_worker);
    let mut err_input = op.new_input(errs.inner, Exchange::new(move |_| mint_target));

    op.build(move |capabilities| {
        let mut capability = capabilities.into_iter().next().filter(|_| is_mint_worker);
        let mut minter = Minter::new(as_of, up_to);
        move |frontiers| {
            ok_input.for_each(|_, _| {});
            err_input.for_each(|_, data| {
                if !minter.done {
                    minter.errors.append(data);
                }
            });
            let Some(cap) = capability.as_mut() else {
                return;
            };

            let mut frontier = Antichain::new();
            for input in frontiers {
                frontier.extend(input.frontier().iter().copied());
            }
            let mut output = output.activate();
            while let Some(description) = minter.mint(frontier.borrow()) {
                output.session(&*cap).give(description.clone());
                if let Some(upper) = description.upper.as_option() {
                    cap.downgrade(upper);
                }
            }
            if minter.done {
                capability = None;
                minter.errors = Vec::new();
            }
        }
    });

    descriptions
}

/// Reports one worker's responses, and a drop if the sink token drops before the subscribe
/// completes.
struct Protocol {
    sink_id: GlobalId,
    /// `None` once the subscribe has completed, so that dropping reports nothing.
    buffer: Option<ResponseBuffer>,
    /// The upper of the last description reported, initially the as-of.
    upper: Antichain<Timestamp>,
    /// The error that poisoned the subscribe, reported for every later description.
    poison: Option<String>,
}

impl Protocol {
    /// Reports `description` with `shares`, or with the poison and no shares once poisoned.
    fn report(&mut self, description: &Description, shares: Vec<Share>) {
        // The mint operator mints nothing after the completing description.
        let Some(buffer) = self.buffer.as_ref() else {
            soft_panic_or_log!(
                "batched subscribe {} received a description after it completed",
                self.sink_id,
            );
            return;
        };
        let (shares, error) = match &self.poison {
            Some(error) => (Vec::new(), Some(error.clone())),
            None => (shares, None),
        };
        buffer.borrow_mut().push((
            self.sink_id,
            BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
                lower: description.lower.clone(),
                upper: description.upper.clone(),
                shares,
                error,
            }),
        ));
        self.upper = description.upper.clone();
        if self.upper.is_empty() {
            self.buffer = None;
        }
    }

    /// Whether the worker reports nothing that needs held batches anymore.
    fn finished(&self) -> bool {
        self.buffer.is_none() || self.poison.is_some()
    }
}

impl Drop for Protocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                BatchedSubscribeResponse::DroppedAt {
                    frontier: self.upper.clone(),
                    orphans: Vec::new(),
                },
            ));
        }
    }
}

/// The state of one worker's write operator.
struct Writer {
    sink_id: GlobalId,
    /// The descriptions received and not yet reported, oldest first.
    pending: VecDeque<Description>,
    /// The held batches, which the descriptions not yet reported need.
    held_oks: Vec<OkBatch>,
    held_errs: Vec<ErrBatch>,
    /// The scan of the oldest pending description, and the updates it has handed out.
    scan: Option<(
        DescriptionScan<OkBatch, ErrBatch>,
        Vec<(Row, Timestamp, Diff)>,
    )>,
    map_filter_project: SafeMfpPlan,
    budget: InlineBudget,
    protocol: Rc<RefCell<Option<Protocol>>>,
}

impl Writer {
    fn new(
        sink_id: GlobalId,
        map_filter_project: SafeMfpPlan,
        budget: InlineBudget,
        protocol: Rc<RefCell<Option<Protocol>>>,
    ) -> Self {
        Self {
            sink_id,
            pending: VecDeque::new(),
            held_oks: Vec::new(),
            held_errs: Vec::new(),
            scan: None,
            map_filter_project,
            budget,
            protocol,
        }
    }

    /// Whether the worker has stopped holding batches, because the subscribe completed, was
    /// poisoned, or was dropped.
    fn finished(&self) -> bool {
        self.protocol
            .borrow()
            .as_ref()
            .map_or(true, Protocol::finished)
    }

    /// Reports the pending descriptions the input frontiers allow, in order. Returns whether the
    /// fuel of this activation ran out with a scan unfinished.
    fn work(
        &mut self,
        ok_frontier: AntichainRef<Timestamp>,
        err_frontier: AntichainRef<Timestamp>,
    ) -> bool {
        let protocol_handle = Rc::clone(&self.protocol);
        let mut protocol = protocol_handle.borrow_mut();
        let Some(protocol) = protocol.as_mut() else {
            self.pending.clear();
            self.release_all();
            return false;
        };

        self.budget.start_activation();
        loop {
            let Some(description) = self.pending.front() else {
                return false;
            };

            if self.scan.is_none() {
                if description.lower != protocol.upper {
                    soft_panic_or_log!(
                        "batched subscribe {} received description [{:?}, {:?}) after upper {:?}",
                        self.sink_id,
                        description.lower,
                        description.upper,
                        protocol.upper,
                    );
                    protocol.poison.get_or_insert_with(|| {
                        "batched subscribe received a description out of order".into()
                    });
                }
                if description.completion || protocol.poison.is_some() {
                    let description = self.pending.pop_front().expect("checked above");
                    protocol.report(&description, Vec::new());
                    self.release(protocol, &description.upper);
                    continue;
                }
                // Without the wait, a worker lagging behind the mint worker would miss updates
                // that no other worker's share holds.
                let complete = PartialOrder::less_equal(&description.upper.borrow(), &ok_frontier)
                    && PartialOrder::less_equal(&description.upper.borrow(), &err_frontier);
                if !complete {
                    return false;
                }
                let scan = self.open_scan(description);
                self.scan = Some((scan, Vec::new()));
            }

            // The peek budget's placement policy applies, minus the offload: with the budget
            // bounded the scan yields the worker and resumes in the next activation, and with it
            // unbounded the scan runs to its end here.
            let Some(fuel) = self.budget.grant() else {
                return true;
            };
            let (scan, updates) = self.scan.as_mut().expect("opened above");
            let mut remaining = fuel;
            let outcome = scan.step(&mut remaining);
            self.budget.charge(fuel - remaining);
            if let Some(batch) = scan.take_batch() {
                updates.extend(batch);
            }
            let DescriptionScanOutcome::Finished(result) = outcome else {
                continue;
            };

            let (_, mut updates) = self.scan.take().expect("opened above");
            let description = self.pending.pop_front().expect("checked above");
            let shares = match result {
                Ok(()) => {
                    // Equal `(time, row)` pairs that a cut separated meet again here.
                    consolidate_updates(&mut updates);
                    inline_share(&updates).into_iter().collect()
                }
                Err(error) => {
                    // A scan error the error collection does not hold, such as one raised by the
                    // `map_filter_project`, is local to this writer. The mint does not learn of
                    // it and keeps minting until the controller drops the subscribe. The writer
                    // releases all its batches once poisoned, so it cannot scan those later
                    // descriptions and reports each with the poison instead.
                    protocol.poison = Some(error);
                    Vec::new()
                }
            };
            protocol.report(&description, shares);
            self.release(protocol, &description.upper);
        }
    }

    fn open_scan(&self, description: &Description) -> DescriptionScan<OkBatch, ErrBatch> {
        let range = description.range();
        let ok_batches = range.select_batches(&self.held_oks);
        let err_batches = range.select_batches(&self.held_errs);
        // A since beyond the lower would have advanced the times the scan places in the range.
        // The arrangement emits freshly sealed batches with a minimal since.
        let since_ok =
            |since: &Antichain<Timestamp>| PartialOrder::less_equal(since, &description.lower);
        soft_assert_or_log!(
            ok_batches
                .iter()
                .all(|batch| since_ok(batch.description().since()))
                && err_batches
                    .iter()
                    .all(|batch| since_ok(batch.description().since())),
            "batched subscribe {} holds a batch compacted beyond the lower {:?}",
            self.sink_id,
            description.lower,
        );
        DescriptionScan::new(
            ok_batches,
            err_batches,
            self.map_filter_project.clone(),
            range,
            SCAN_CUT_BYTES,
            self.sink_id,
        )
    }

    /// Releases the held batches that end at or before `upper`, which no later description
    /// needs, and every held batch once `protocol` has finished.
    fn release(&mut self, protocol: &Protocol, upper: &Antichain<Timestamp>) {
        if protocol.finished() {
            self.release_all();
            return;
        }
        let needed =
            |batch_upper: &Antichain<Timestamp>| !PartialOrder::less_equal(batch_upper, upper);
        self.held_oks
            .retain(|batch| needed(batch.description().upper()));
        self.held_errs
            .retain(|batch| needed(batch.description().upper()));
    }

    fn release_all(&mut self) {
        self.held_oks = Vec::new();
        self.held_errs = Vec::new();
        self.scan = None;
    }
}

/// Builds the inline share of consolidated `updates`, or `None` if there are none.
fn inline_share(updates: &[(Row, Timestamp, Diff)]) -> Option<Share> {
    if updates.is_empty() {
        return None;
    }
    let byte_len = updates.iter().map(|(row, _, _)| row.byte_len()).sum();
    let mut builder = UpdateCollection::builder(byte_len, updates.len());
    for (row, time, diff) in updates {
        builder.push((row.as_row_ref(), time, *diff));
    }
    Some(Share::Inline(builder.build()))
}

/// Renders the write operator, which runs `writer` on every worker.
fn write<'scope>(
    sink_id: GlobalId,
    descriptions: Stream<'scope, Timestamp, Vec<Description>>,
    ok_batches: Stream<'scope, Timestamp, Vec<OkBatch>>,
    err_batches: Stream<'scope, Timestamp, Vec<ErrBatch>>,
    mut writer: Writer,
) {
    let scope = descriptions.scope();
    let mut op = OperatorBuilder::new(format!("BatchedSubscribeWrite({sink_id})"), scope.clone());
    let mut descriptions_input = op.new_input(descriptions, Pipeline);
    let mut ok_input = op.new_input(ok_batches, Pipeline);
    let mut err_input = op.new_input(err_batches, Pipeline);
    let operator_info = op.operator_info();

    op.build(move |_capabilities| {
        let activator = scope.activator_for(operator_info.address);
        move |frontiers| {
            descriptions_input.for_each(|_, data| writer.pending.extend(data.drain(..)));
            // Batches arrive before the frontier passes them, so every batch a description
            // overlaps is held once the frontiers have reached its upper.
            let finished = writer.finished();
            ok_input.for_each(|_, data| {
                if !finished {
                    writer
                        .held_oks
                        .extend(data.drain(..).filter(|batch| !batch.is_empty()));
                }
            });
            err_input.for_each(|_, data| {
                if !finished {
                    writer
                        .held_errs
                        .extend(data.drain(..).filter(|batch| !batch.is_empty()));
                }
            });
            if writer.work(frontiers[1].frontier(), frontiers[2].frontier()) {
                activator.activate();
            }
        }
    });
}

#[cfg(test)]
mod tests;
