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
//!
//! A share too large to travel inline, and a scan too large to run on the worker, leave the worker
//! as described in [`share`].

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, TraceReader};
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use mz_compute_client::protocol::response::{
    BatchedSubscribeBatch, BatchedSubscribeResponse, Share, StashedShare,
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
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::vec::Broadcast;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::scheduling::SyncActivator;
use tracing::warn;

use crate::compute_state::description_iterator::DescriptionRange;
use crate::compute_state::description_scan::{
    DescriptionScan, DescriptionScanOutcome, DescriptionUpdates,
};
use crate::compute_state::peek_budget::InlineBudget;
use crate::compute_state::peek_stash::{DeliveredBatch, stash_shard_id};
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

use self::share::{PendingShare, StashSpec, Tiers};

mod share;

/// A batch of the sink's ok arrangement.
type OkBatch = <RowRowSpine<Timestamp, Diff> as TraceReader>::Batch;
/// A batch of the sink's error arrangement.
type ErrBatch = <ErrSpine<Timestamp, Diff> as TraceReader>::Batch;

/// The buffer the sink reports through, drained by `ComputeState::process_batched_subscribes`.
type ResponseBuffer = Rc<RefCell<Vec<(GlobalId, QueuedResponse)>>>;

/// A worker's share of a description, on its way to the controller.
pub(crate) enum OwnedShare {
    Inline {
        rows: UpdateCollection,
        /// Consolidated per `(time, error)`.
        errors: Vec<(DataflowErrorSer, Timestamp, Diff)>,
    },
    /// A stash batch, deleted if the share is dropped before it is sent.
    Stashed {
        delivered: DeliveredBatch,
        /// The greatest time of an error update written to the batch, an upper bound on those
        /// it holds.
        last_error_time: Option<Timestamp>,
    },
}

impl OwnedShare {
    /// The share as the protocol carries it. A stashed batch becomes the receiver's to delete.
    fn into_share(self) -> Share {
        match self {
            OwnedShare::Inline { rows, errors } => Share::Inline { rows, errors },
            OwnedShare::Stashed {
                delivered,
                last_error_time,
            } => {
                let batch = delivered.take();
                Share::Stashed {
                    batches: StashedShare {
                        shard_id: batch.shard_id(),
                        encoded_size_bytes: batch.encoded_size_bytes(),
                        batches: vec![batch.into_transmittable_batch()],
                    },
                    last_error_time,
                }
            }
        }
    }
}

/// A batched subscribe response queued on a worker, owning the stash batches it names until it
/// is sent.
///
/// A response that is dropped instead of sent, by a reset of the response buffer or by a protocol
/// that has completed, deletes its stash batches.
pub(crate) enum QueuedResponse {
    Batch {
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
        shares: Vec<OwnedShare>,
        fatal: Option<String>,
    },
    /// The sink was dropped after reporting descriptions up to `frontier`. A sink holds no share
    /// of an unreported description, so the response carries no orphans.
    DroppedAt { frontier: Antichain<Timestamp> },
}

impl QueuedResponse {
    /// The frontier the subscribe has reported through, once this response is sent.
    pub(crate) fn frontier(&self) -> Antichain<Timestamp> {
        match self {
            QueuedResponse::Batch { upper, .. } => upper.clone(),
            QueuedResponse::DroppedAt { .. } => Antichain::new(),
        }
    }

    /// The response as the protocol carries it, which hands the stash batches to the receiver.
    pub(crate) fn into_response(self) -> BatchedSubscribeResponse {
        match self {
            QueuedResponse::Batch {
                lower,
                upper,
                shares,
                fatal,
            } => BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
                lower,
                upper,
                shares: shares.into_iter().map(OwnedShare::into_share).collect(),
                fatal,
            }),
            QueuedResponse::DroppedAt { frontier } => BatchedSubscribeResponse::DroppedAt {
                frontier,
                orphans: Vec::new(),
            },
        }
    }
}

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
        } = KeyCollection::from(err_collection)
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
            err_batches.clone(),
        );

        let protocol = Rc::new(RefCell::new(Some(Protocol {
            sink_id,
            buffer: Some(Rc::clone(&compute_state.batched_subscribe_response_buffer)),
            upper: as_of,
            fatal: None,
        })));
        let protocol_weak = Rc::downgrade(&protocol);
        // The plan applies to rows of `from_desc`.
        let map_filter_project = MapFilterProject::new(sink.from_desc.arity())
            .into_plan()
            .expect("an identity plan is valid")
            .into_nontemporal()
            .expect("an identity plan is not temporal");
        let stash = match &compute_state.peek_stash_persist_location {
            Some(location) => Some(StashSpec::new(
                Arc::clone(&compute_state.persist_clients),
                location.clone(),
                stash_shard_id(self.stash_id),
                self.output_desc.clone(),
            )),
            None => {
                warn!(%sink_id, "no stash location, so batched subscribe shares stay inline");
                None
            }
        };
        let tiers = Tiers::new(
            &compute_state.worker_config,
            stash,
            Arc::clone(&compute_state.peek_permits),
            &compute_state.metrics,
        );
        write(
            sink_id,
            descriptions.broadcast(),
            ok_batches,
            err_batches,
            Writer::new(
                sink_id,
                map_filter_project,
                InlineBudget::new(&compute_state.worker_config),
                tiers,
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
    /// The range the description's scan emits.
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
    /// Whether the chain has ended, by completion or by an empty input frontier.
    done: bool,
}

impl Minter {
    fn new(as_of: Antichain<Timestamp>, up_to: Antichain<Timestamp>) -> Self {
        Self {
            lower: as_of,
            first: true,
            up_to,
            done: false,
        }
    }

    /// Returns the next description, given that the input is complete before `frontier`, or
    /// `None` if the frontier allows none.
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

        // NOTE: A fatal error on a worker does not end the chain. The cutover drops a batch with a
        // fatal error that ends at or below the accepted upper, and a chain that ended there would
        // stall the subscribe. The worker repeats the fatal error on every later description
        // instead.
        self.done = upper.is_empty();
        Some(Description {
            lower: std::mem::replace(&mut self.lower, upper.clone()),
            upper,
            first: std::mem::replace(&mut self.first, false),
            completion: false,
        })
    }
}

/// Renders the mint operator, which runs on one worker and emits the descriptions it cuts.
///
/// Its input frontier is the meet of the ok and the error arrangements' frontiers.
fn mint<'scope>(
    sink_id: GlobalId,
    as_of: Antichain<Timestamp>,
    up_to: Antichain<Timestamp>,
    ok_batches: Stream<'scope, Timestamp, Vec<OkBatch>>,
    err_batches: Stream<'scope, Timestamp, Vec<ErrBatch>>,
) -> Stream<'scope, Timestamp, Vec<Description>> {
    let scope = ok_batches.scope();
    let mint_worker = usize::cast_from(sink_id.hashed()) % scope.peers();
    let is_mint_worker = scope.index() == mint_worker;

    let mut op = OperatorBuilder::new(format!("BatchedSubscribeMint({sink_id})"), scope);
    let (output, descriptions) = op.new_output();
    let mut output = OutputBuilder::<_, CapacityContainerBuilder<Vec<Description>>>::from(output);
    // Only the frontiers of the batches matter here.
    let mut ok_input = op.new_input(ok_batches, Pipeline);
    let mut err_input = op.new_input(err_batches, Pipeline);

    op.build(move |capabilities| {
        let mut capability = capabilities.into_iter().next().filter(|_| is_mint_worker);
        let mut minter = Minter::new(as_of, up_to);
        move |frontiers| {
            ok_input.for_each(|_, _| {});
            err_input.for_each(|_, _| {});
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
    /// The fatal error the worker hit, reported for that description and every later one.
    fatal: Option<String>,
}

impl Protocol {
    /// Reports `description` with `shares`, and with the fatal error once the worker hit one.
    fn report(&mut self, description: &Description, shares: Vec<OwnedShare>) {
        // The mint operator mints nothing after the completing description.
        let Some(buffer) = self.buffer.as_ref() else {
            soft_panic_or_log!(
                "batched subscribe {} received a description after it completed",
                self.sink_id,
            );
            return;
        };
        // A batch with a fatal error still carries its shares, so the receiver deletes their
        // stash batches.
        buffer.borrow_mut().push((
            self.sink_id,
            QueuedResponse::Batch {
                lower: description.lower.clone(),
                upper: description.upper.clone(),
                shares,
                fatal: self.fatal.clone(),
            },
        ));
        self.upper = description.upper.clone();
        if self.upper.is_empty() {
            self.buffer = None;
        }
    }

    /// Whether the worker reports nothing that needs held batches anymore.
    fn finished(&self) -> bool {
        self.buffer.is_none() || self.fatal.is_some()
    }
}

impl Drop for Protocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                QueuedResponse::DroppedAt {
                    frontier: self.upper.clone(),
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
    /// The scan of the oldest pending description.
    scan: Option<Scan>,
    map_filter_project: SafeMfpPlan,
    budget: InlineBudget,
    /// The bytes a scan buffers before it hands them out.
    cut_bytes: usize,
    tiers: Tiers,
    /// Activates the write operator, once it is built.
    activator: Option<SyncActivator>,
    protocol: Rc<RefCell<Option<Protocol>>>,
}

/// The scan of a description, and where it runs.
enum Scan {
    /// On the worker, with the updates it has handed out, accounted at `bytes`. Above
    /// `inline_max_bytes`, if set, the share leaves the inline tier.
    OnWorker {
        scan: DescriptionScan<OkBatch, ErrBatch>,
        updates: DescriptionUpdates,
        bytes: usize,
        inline_max_bytes: Option<usize>,
    },
    /// Away from the worker, which reports the share once it is ready.
    Away(PendingShare),
}

impl Writer {
    fn new(
        sink_id: GlobalId,
        map_filter_project: SafeMfpPlan,
        budget: InlineBudget,
        tiers: Tiers,
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
            cut_bytes: SCAN_CUT_BYTES,
            tiers,
            activator: None,
            protocol,
        }
    }

    /// Whether the writer has no description left to answer and no scan under way.
    fn idle(&self) -> bool {
        self.pending.is_empty() && self.scan.is_none()
    }

    /// Whether the worker has stopped holding batches, because the subscribe completed, the
    /// worker hit a fatal error, or the subscribe was dropped.
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
                    protocol.fatal.get_or_insert_with(|| {
                        "batched subscribe received a description out of order".into()
                    });
                }
                if description.completion || protocol.fatal.is_some() {
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
                let (scan, held_updates) = self.open_scan(description);
                self.scan = Some(if self.tiers.offloads(held_updates) {
                    Scan::Away(self.tiers.start(
                        description,
                        Some(scan),
                        DescriptionUpdates::default(),
                        0,
                        self.activator.clone(),
                    ))
                } else {
                    Scan::OnWorker {
                        scan,
                        updates: DescriptionUpdates::default(),
                        bytes: 0,
                        inline_max_bytes: self.tiers.inline_max_bytes(),
                    }
                });
            }

            let result = match self.scan.as_mut().expect("opened above") {
                // The share task activates the operator once its outcome is ready.
                Scan::Away(pending) => match pending.try_take() {
                    None => return false,
                    Some(result) => result.map(share::Finished::into_shares),
                },
                Scan::OnWorker {
                    scan,
                    updates,
                    bytes,
                    inline_max_bytes,
                } => {
                    // The peek budget's placement policy applies: with the budget bounded the
                    // scan yields the worker and resumes in the next activation, and with it
                    // unbounded the scan runs to its end here. A scan too large for that is
                    // offloaded when it opens.
                    let Some(fuel) = self.budget.grant() else {
                        return true;
                    };
                    let mut remaining = fuel;
                    let outcome = scan.step(&mut remaining);
                    self.budget.charge(fuel - remaining);
                    if let Some(batch) = scan.take_batch() {
                        *bytes = bytes.saturating_add(batch.byte_len());
                        updates.extend(batch);
                    }
                    let ended = matches!(outcome, DescriptionScanOutcome::Finished);
                    if inline_max_bytes.is_some_and(|max_bytes| *bytes > max_bytes) {
                        // Writing to the stash awaits, so the share and what remains of its scan
                        // leave the worker together.
                        let Some(Scan::OnWorker {
                            scan,
                            updates,
                            bytes,
                            ..
                        }) = self.scan.take()
                        else {
                            unreachable!("matched above");
                        };
                        let scan = (!ended).then_some(scan);
                        self.scan = Some(Scan::Away(self.tiers.start(
                            description,
                            scan,
                            updates,
                            bytes,
                            self.activator.clone(),
                        )));
                        continue;
                    }
                    if !ended {
                        continue;
                    }
                    let mut updates = std::mem::take(updates);
                    // Equal `(time, row)` pairs that a cut separated meet again here.
                    updates.consolidate();
                    Ok(inline_share(updates).into_iter().collect())
                }
            };

            self.scan = None;
            let description = self.pending.pop_front().expect("checked above");
            let shares = match result {
                Ok(shares) => {
                    self.tiers.record(&shares);
                    shares
                }
                Err(error) => {
                    // The writer releases all its batches once it hit a fatal error, so it cannot
                    // scan later descriptions and reports each with the fatal error instead.
                    protocol.fatal = Some(error);
                    Vec::new()
                }
            };
            protocol.report(&description, shares);
            self.release(protocol, &description.upper);
        }
    }

    /// Opens the scan of `description`, and returns it with the number of updates in the ok
    /// batches it scans.
    fn open_scan(&self, description: &Description) -> (DescriptionScan<OkBatch, ErrBatch>, usize) {
        let range = description.range();
        let ok_batches = range.select_batches(&self.held_oks);
        let held_updates = ok_batches.iter().map(|batch| batch.len()).sum();
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
        let scan = DescriptionScan::new(
            ok_batches,
            err_batches,
            self.map_filter_project.clone(),
            range,
            self.cut_bytes,
        );
        (scan, held_updates)
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
fn inline_share(updates: DescriptionUpdates) -> Option<OwnedShare> {
    if updates.is_empty() {
        return None;
    }
    let DescriptionUpdates { rows, errors } = updates;
    let byte_len = rows.iter().map(|(row, _, _)| row.byte_len()).sum();
    let mut builder = UpdateCollection::builder(byte_len, rows.len());
    for (row, time, diff) in &rows {
        builder.push((row.as_row_ref(), time, *diff));
    }
    Some(OwnedShare::Inline {
        rows: builder.build(),
        errors,
    })
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
    // NOTE: The output carries nothing. Timely retires an operator whose inputs are complete and
    // that holds no capability, which would drop a share still under way once every input has
    // closed. The output's capability keeps the operator until the writer is idle.
    let (_output, _unused): (_, Stream<'scope, Timestamp, Vec<()>>) = op.new_output();

    op.build(move |capabilities| {
        let mut capability = capabilities.into_iter().next();
        writer.activator = Some(
            scope
                .worker()
                .sync_activator_for(operator_info.address.to_vec()),
        );
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
            let inputs_complete = frontiers.iter().all(|input| input.frontier().is_empty());
            if inputs_complete && writer.idle() {
                drop(capability.take());
            }
        }
    });
}

#[cfg(test)]
mod tests;
