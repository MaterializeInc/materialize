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
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::Mutex;

use mz_expr::MfpPlan;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

pub use mz_persist_client::operators::shard_source::FlowControl;

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
    persist_clients: Arc<Mutex<PersistClientCache>>,
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
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    yield_fn: YFn,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
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
    );
    let rows = decode_and_mfp(&fetched, &name, until, map_filter_project, yield_fn);
    (rows, token)
}

pub fn decode_and_mfp<G, YFn>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    yield_fn: YFn,
) -> Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let mut builder = AsyncOperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        fetched.scope(),
    );

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let mut map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |mut initial_capabilities| async move {
        initial_capabilities.clear();

        let mut buffer = Vec::new();

        while let Some(event) = fetched_input.next().await {
            let cap = match event {
                Event::Data(cap, data) => {
                    data.swap(&mut buffer);
                    cap
                }
                Event::Progress(_) => continue,
            };

            // SUBTLE: This operator yields back to timely whenever an await returns a
            // Pending result from the overall async/await state machine `poll`. Since
            // this is fetching from remote storage, it will yield and thus we can reset
            // our yield counters here.
            let mut decode_start = Instant::now();

            for fetched_part in buffer.drain(..) {
                // Apply as much logic to `updates` as we can, before we emit anything.
                let (updates_size_hint_min, updates_size_hint_max) = fetched_part.size_hint();
                let mut updates =
                    Vec::with_capacity(updates_size_hint_max.unwrap_or(updates_size_hint_min));
                for ((key, val), time, diff) in fetched_part {
                    if !until.less_equal(&time) {
                        match (key, val) {
                            (Ok(SourceData(Ok(row))), Ok(())) => {
                                if let Some(mfp) = &mut map_filter_project {
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
                                                    updates.push((Ok(row), time, diff));
                                                }
                                            }
                                            Err((err, time, diff)) => {
                                                // Additional `until` filtering due to temporal filters.
                                                if !until.less_equal(&time) {
                                                    updates.push((Err(err), time, diff));
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    updates.push((Ok(row), time, diff));
                                }
                            }
                            (Ok(SourceData(Err(err))), Ok(())) => {
                                updates.push((Err(err), time, diff));
                            }
                            // TODO(petrosagg): error handling
                            (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                                panic!("decoding failed")
                            }
                        }
                    }
                    if yield_fn(decode_start, updates.len()) {
                        // A large part of the point of yielding is to let later operators
                        // reduce down the data, so emit what we have. Note that this means
                        // we don't get to consolidate everything, but that's part of the
                        // tradeoff in tuning yield_fn.
                        differential_dataflow::consolidation::consolidate_updates(&mut updates);

                        {
                            // Do very fine-grained output activation/session
                            // creation to ensure that we don't hold activated
                            // outputs or sessions across await points, which
                            // would prevent messages from being flushed from
                            // the shared timely output buffer.
                            let mut updates_output = updates_output.activate();
                            updates_output.session(&cap).give_vec(&mut updates);
                        }

                        // Force a yield to give back the timely thread, reactivating on our
                        // way out.
                        tokio::task::yield_now().await;
                        decode_start = Instant::now();
                    }
                }
                differential_dataflow::consolidation::consolidate_updates(&mut updates);

                // Do very fine-grained output activation/session creation
                // to ensure that we don't hold activated outputs or
                // sessions across await points, which would prevent
                // messages from being flushed from the shared timely output
                // buffer.
                let mut updates_output = updates_output.activate();
                updates_output.session(&cap).give_vec(&mut updates);
            }
        }
    });

    updates_stream
}
