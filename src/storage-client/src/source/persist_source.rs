// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use differential_dataflow::{AsCollection, Collection};
use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use mz_persist_client::operators::shard_source::shard_source;
use mz_persist_types::codec_impls::UnitSchema;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::Activator;

use mz_expr::MfpPlan;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

pub use mz_persist_client::operators::shard_source::FlowControl;
use mz_timely_util::buffer::ConsolidateBuffer;
use mz_timely_util::operator::CollectionExt;

/// Request a particular guarantee about consolidation from the source.
/// It is legal for the source to satisfy a request for consolidation with an ordinary consolidate
/// operator, but the hope is for it to be able to do something more clever.
pub enum Consolidation {
    /// No guarantees about how the resulting collection's data is consolidated.
    Maybe,
    /// Guarantees that the resulting collections is consolidated, but not necessarily exchanged
    /// the way the normal consolidate operator would do it.
    Consolidated,
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// Users of this function have the ability to apply flow control to the output
/// to limit the in-flight data (measured in bytes) it can emit. The flow control
/// input is a timely stream that communicates the frontier at which the data
/// emitted from by this source have been dropped.
///
/// **Note:** Because this function is reading batches from `persist`, it is working
/// at batch granularity. In practice, the source will be overshooting the target
/// flow control upper by an amount that is related to the size of batches.
///
/// If no flow control is desired an empty stream whose frontier immediately advances
/// to the empty antichain can be used. An easy easy of creating such stream is by
/// using [`timely::dataflow::operators::generic::operator::empty`].
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    consolidation: Consolidation,
    yield_fn: YFn,
) -> (
    Collection<G, Row, Diff>,
    Collection<G, DataflowError, Diff>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
        flow_control,
        consolidation,
        yield_fn,
    );
    let (ok_stream, err_stream) = stream.map_fallible("OkErr", |d| d);
    (ok_stream, err_stream, token)
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    consolidation: Consolidation,
    yield_fn: YFn,
) -> (Collection<G, Result<Row, DataflowError>, Diff>, Rc<dyn Any>)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let name = source_id.to_string();
    let (fetched, token) = shard_source(
        scope,
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        flow_control,
        Arc::new(metadata.relation_desc),
        Arc::new(UnitSchema),
    );
    let rows = decode(&fetched, &name, until.clone(), yield_fn).as_collection();

    let rows = match consolidation {
        Consolidation::Maybe => rows,
        Consolidation::Consolidated => rows.consolidate(),
    };

    let rows = match map_filter_project.map(|mfp| mfp.take()) {
        None => rows,
        Some(mfp_plan) => mfp(rows, &name, until, mfp_plan),
    };

    (rows, token)
}

pub fn decode<G, YFn>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    yield_fn: YFn,
) -> Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let scope = fetched.scope();
    let mut builder =
        OperatorBuilder::new(format!("persist_source::decode({})", name), scope.clone());
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    builder.build(move |_caps| {
        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activations = scope.activations();
        let activator = Activator::new(&operator_info.address[..], activations);
        // Maintain a list of work to do
        let mut pending_work = std::collections::VecDeque::new();
        let mut buffer = Default::default();

        move |_frontier| {
            fetched_input.for_each(|time, data| {
                data.swap(&mut buffer);
                let capability = time.retain();
                for fetched_part in buffer.drain(..) {
                    pending_work.push_back(PendingWork {
                        capability: capability.clone(),
                        fetched_part,
                    })
                }
            });

            let mut work = 0;
            let start_time = Instant::now();
            let mut output = updates_output.activate();
            let mut handle = ConsolidateBuffer::new(&mut output, 0);
            while !pending_work.is_empty() && !yield_fn(start_time, work) {
                let done = pending_work.front_mut().unwrap().do_work(
                    &mut work,
                    start_time,
                    &yield_fn,
                    &until,
                    &mut handle,
                );
                if done {
                    pending_work.pop_front();
                }
            }
            if !pending_work.is_empty() {
                activator.activate();
            }
        }
    });

    updates_stream
}

/// Pending work to read from fetched parts
struct PendingWork {
    /// The time at which the work should happen.
    capability: Capability<Timestamp>,
    /// Pending fetched part.
    fetched_part: FetchedPart<SourceData, (), Timestamp, Diff>,
}

impl PendingWork {
    /// Perform work, reading from the fetched part, decoding, and sending outputs, while checking
    /// `yield_fn` whether more fuel is available.
    fn do_work<P, YFn>(
        &mut self,
        work: &mut usize,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        output: &mut ConsolidateBuffer<Timestamp, Result<Row, DataflowError>, Diff, P>,
    ) -> bool
    where
        P: Push<Bundle<Timestamp, (Result<Row, DataflowError>, Timestamp, Diff)>>,
        YFn: Fn(Instant, usize) -> bool,
    {
        while let Some(((key, val), time, diff)) = self.fetched_part.next() {
            if until.less_equal(&time) {
                continue;
            }
            match (key, val) {
                (Ok(SourceData(data)), Ok(())) => {
                    output.give_at(&self.capability, (data, time, diff));
                    *work += 1;
                }
                // TODO(petrosagg): error handling
                (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                    panic!("decoding failed")
                }
            }
            if yield_fn(start_time, *work) {
                return false;
            }
        }
        true
    }
}

pub fn mfp<G>(
    collection: Collection<G, Result<Row, DataflowError>, Diff>,
    name: &str,
    until: Antichain<Timestamp>,
    mfp: MfpPlan,
) -> Collection<G, Result<Row, DataflowError>, Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    collection
        .inner
        .unary(Pipeline, &format!("persist_source::mfp({name})"), |_, _| {
            let mut vector = Vec::new();
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for (row, ts, diff) in vector.drain(..) {
                        match row {
                            Ok(row) => {
                                let arena = mz_repr::RowArena::new();
                                let mut datums_local = datum_vec.borrow_with(&row);
                                for result in mfp.evaluate(
                                    &mut datums_local,
                                    &arena,
                                    ts,
                                    diff,
                                    |time| !until.less_equal(time),
                                    &mut row_builder,
                                ) {
                                    session.give(match result {
                                        Ok((r, t, d)) => (Ok(r), t, d),
                                        Err((e, t, d)) => (Err(e), t, d),
                                    });
                                }
                            }
                            Err(e) => {
                                session.give((Err(e), ts, diff));
                            }
                        }
                    }
                });
            }
        })
        .as_collection()
}
