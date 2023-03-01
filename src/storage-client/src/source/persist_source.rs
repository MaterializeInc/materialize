// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use mz_persist_client::operators::shard_source::shard_source;
use mz_persist_types::codec_impls::UnitSchema;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, OkErr};
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
    yield_fn: YFn,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
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
        yield_fn,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

type OutputData = Result<Row, DataflowError>;
type RawData = (Result<SourceData, String>, Result<(), String>);

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
    yield_fn: YFn,
) -> (Stream<G, (OutputData, Timestamp, Diff)>, Rc<dyn Any>)
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

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.map(|mfp| mfp.take());

    let rows = decode_and_emit(
        &fetched,
        &name,
        // until,
        // map_filter_project,
        yield_fn,
        move |output, capability, ((key, val), time, diff)| {
            let mut work = 0;
            match (key, val) {
                (Ok(SourceData(Ok(row))), Ok(())) => {
                    if let Some(mfp) = &map_filter_project {
                        let arena = mz_repr::RowArena::new();
                        let mut datums_local = datum_vec.borrow_with(&row);
                        for result in mfp.evaluate(
                            &mut datums_local,
                            &arena,
                            time,
                            diff,
                            |time| !until.less_equal(time),
                            &mut row_builder,
                        ) {
                            match result {
                                Ok((row, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        output.give_at(capability, (Ok(row), time, diff));
                                        work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        output.give_at(capability, (Err(err), time, diff));
                                        work += 1;
                                    }
                                }
                            }
                        }
                    } else {
                        output.give_at(capability, (Ok(row), time, diff));
                        work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    output.give_at(capability, (Err(err), time, diff));
                    work += 1;
                }
                // TODO(petrosagg): error handling
                (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                    panic!("decoding failed")
                }
            }
            work
        },
    );
    (rows, token)
}

pub fn decode_and_emit<G, YFn>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    yield_fn: YFn,
    mut emit_fn: impl FnMut(
            &mut ConsolidateBuffer<
                Timestamp,
                OutputData,
                Diff,
                Tee<Timestamp, (OutputData, Timestamp, Diff)>,
            >,
            &Capability<Timestamp>,
            (RawData, Timestamp, Diff),
        ) -> usize
        + 'static,
) -> Stream<G, (OutputData, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_emit({})", name),
        scope.clone(),
    );
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
            'work_loop: while let Some(PendingWork {
                capability,
                fetched_part,
            }) = pending_work.front_mut()
            {
                while let Some(data) = fetched_part.next() {
                    work += emit_fn(&mut handle, capability, data);
                    if yield_fn(start_time, work) {
                        break 'work_loop;
                    }
                }
                pending_work.pop_front();
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
