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

use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_expr::MfpPlan;
use mz_persist_client::cache::PersistClientCache;

use mz_repr::{Diff, GlobalId, Row, Timestamp};

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
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let name = source_id.to_string();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.map(|mfp| mfp.take());

    let (rows, token) = shard_source(
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
