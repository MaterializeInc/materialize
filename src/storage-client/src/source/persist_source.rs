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

use differential_dataflow::lattice::Lattice;
use futures::{future::Either, StreamExt};
use mz_expr::{ColumnSpecs, Interpreter, MfpPlan, ResultSpec, UnmaterializableFunc};
use mz_ore::collections::CollectionExt;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_persist_client::fetch::SerdeLeasedBatchPart;
use mz_persist_client::operators::shard_source::shard_source;
pub use mz_persist_client::operators::shard_source::FlowControl;
use mz_persist_client::stats::PartStats;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::Data;
use mz_persist_types::dyn_struct::DynStruct;
use mz_persist_types::stats::{BytesStats, ColumnStats, DynStats, JsonStats};
use mz_persist_types::Codec64;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{
    ColumnType, Datum, DatumToPersist, DatumToPersistFn, DatumVec, Diff, GlobalId, RelationDesc,
    RelationType, Row, RowArena, ScalarType, Timestamp,
};
use mz_timely_util::buffer::ConsolidateBuffer;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, OkErr};
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Feedback};
use timely::dataflow::operators::{Enter, Leave};
use timely::dataflow::scopes::Child;
use timely::dataflow::ScopeParent;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::timestamp::PathSummary;
use timely::progress::timestamp::Refines;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::scheduling::Activator;
use timely::PartialOrder;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;
use tracing::trace;

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

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
    scope: &mut G,
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
    let (stream, token) = scope.scoped(
        &format!("skip_granular_backpressure({})", source_id),
        |scope| {
            let (stream, token) = persist_source_core(
                scope,
                source_id,
                persist_clients,
                metadata,
                as_of,
                until,
                map_filter_project,
                flow_control.map(|fc| FlowControl {
                    progress_stream: fc.progress_stream.enter(scope),
                    max_inflight_bytes: fc.max_inflight_bytes,
                    summary: Refines::to_inner(fc.summary),
                }),
                yield_fn,
            );
            (stream.leave(), token)
        },
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t.0, r)),
        Err(err) => Err((err, t.0, r)),
    });
    (ok_stream, err_stream, token)
}

type RefinedScope<'g, G> = Child<'g, G, (<G as ScopeParent>::Timestamp, u64)>;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<'g, G, YFn>(
    scope: &mut RefinedScope<'g, G>,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<RefinedScope<'g, G>>>,
    yield_fn: YFn,
) -> (
    Stream<RefinedScope<'g, G>, (Result<Row, DataflowError>, (mz_repr::Timestamp, u64), Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let name = source_id.to_string();
    let desc = metadata.relation_desc.clone();
    let filter_plan = map_filter_project.as_ref().map(|p| (*p).clone());
    let time_range = if let Some(lower) = as_of.as_ref().and_then(|a| a.as_option().copied()) {
        // If we have a lower bound, we can provide a bound on mz_now to our filter pushdown.
        // The range is inclusive, so it's safe to use the maximum timestamp as the upper bound when
        // `until ` is the empty antichain.
        // TODO: continually narrow this as the frontier progresses.
        let upper = until.as_option().copied().unwrap_or(Timestamp::MAX);
        ResultSpec::value_between(Datum::MzTimestamp(lower), Datum::MzTimestamp(upper))
    } else {
        ResultSpec::anything()
    };

    let desc_transformer = match flow_control {
        Some(flow_control) => Some(move |mut scope: _, descs: &Stream<_, _>, chosen_worker| {
            backpressure(
                &mut scope,
                &format!("backpressure({source_id})"),
                descs,
                flow_control,
                chosen_worker,
                None,
            )
        }),
        None => None,
    };

    let (fetched, token) = shard_source(
        &mut scope.clone(),
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        desc_transformer,
        Arc::new(metadata.relation_desc),
        Arc::new(UnitSchema),
        move |stats| {
            if let Some(plan) = &filter_plan {
                let stats = PersistSourceDataStats { desc: &desc, stats };
                filter_may_match(desc.typ(), time_range.clone(), stats, plan)
            } else {
                true
            }
        },
    );
    let rows = decode_and_mfp(&fetched, &name, until, map_filter_project, yield_fn);
    (rows, token)
}

fn filter_may_match(
    relation_type: &RelationType,
    time_range: ResultSpec,
    stats: PersistSourceDataStats,
    plan: &MfpPlan,
) -> bool {
    let arena = RowArena::new();
    let mut ranges = ColumnSpecs::new(relation_type, &arena);
    // TODO: even better if we can use the lower bound of the part itself!
    ranges.push_unmaterializable(UnmaterializableFunc::MzNow, time_range.clone());

    if stats.err_count().into_iter().any(|count| count > 0) {
        // If the error collection is nonempty, we always keep the part.
        return true;
    }

    for (id, _) in relation_type.column_types.iter().enumerate() {
        let result_spec = stats.col_stats(id, &arena);
        ranges.push_column(id, result_spec);
    }
    let result = ranges.mfp_plan_filter(plan).range;
    result.may_contain(Datum::True) || result.may_fail()
}

pub fn decode_and_mfp<G, YFn>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    yield_fn: YFn,
) -> Stream<G, (Result<Row, DataflowError>, G::Timestamp, Diff)>
where
    G: Scope<Timestamp = (mz_repr::Timestamp, u64)>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        scope.clone(),
    );
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |_caps| {
        let name = name.to_owned();
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
                    &name,
                    start_time,
                    &yield_fn,
                    &until,
                    map_filter_project.as_ref(),
                    &mut datum_vec,
                    &mut row_builder,
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
    capability: Capability<(mz_repr::Timestamp, u64)>,
    /// Pending fetched part.
    fetched_part: FetchedPart<SourceData, (), Timestamp, Diff>,
}

impl PendingWork {
    /// Perform work, reading from the fetched part, decoding, and sending outputs, while checking
    /// `yield_fn` whether more fuel is available.
    fn do_work<P, YFn>(
        &mut self,
        work: &mut usize,
        name: &str,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut ConsolidateBuffer<
            (mz_repr::Timestamp, u64),
            Result<Row, DataflowError>,
            Diff,
            P,
        >,
    ) -> bool
    where
        P: Push<
            Bundle<
                (mz_repr::Timestamp, u64),
                (Result<Row, DataflowError>, (mz_repr::Timestamp, u64), Diff),
            >,
        >,
        YFn: Fn(Instant, usize) -> bool,
    {
        let is_filter_pushdown_audit = self.fetched_part.is_filter_pushdown_audit();
        while let Some(((key, val), time, diff)) = self.fetched_part.next() {
            if until.less_equal(&time) {
                continue;
            }
            match (key, val) {
                (Ok(SourceData(Ok(row))), Ok(())) => {
                    if let Some(mfp) = map_filter_project {
                        let arena = mz_repr::RowArena::new();
                        let mut datums_local = datum_vec.borrow_with(&row);
                        for result in mfp.evaluate(
                            &mut datums_local,
                            &arena,
                            time,
                            diff,
                            |time| !until.less_equal(time),
                            row_builder,
                        ) {
                            if let Some(stats) = is_filter_pushdown_audit {
                                panic!("persist filter pushdown correctness violation! {} val={:?} mfp={:?} stats={:?}", name, result, map_filter_project, stats);
                            }
                            match result {
                                Ok((row, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        let mut emit_time = *self.capability.time();
                                        emit_time.0 = time;
                                        output
                                            .give_at(&self.capability, (Ok(row), emit_time, diff));
                                        *work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        let mut emit_time = *self.capability.time();
                                        emit_time.0 = time;
                                        output
                                            .give_at(&self.capability, (Err(err), emit_time, diff));
                                        *work += 1;
                                    }
                                }
                            }
                        }
                    } else {
                        let mut emit_time = *self.capability.time();
                        emit_time.0 = time;
                        output.give_at(&self.capability, (Ok(row), emit_time, diff));
                        *work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    let mut emit_time = *self.capability.time();
                    emit_time.0 = time;
                    output.give_at(&self.capability, (Err(err), emit_time, diff));
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

#[derive(Debug)]
pub(crate) struct PersistSourceDataStats<'a> {
    pub(crate) desc: &'a RelationDesc,
    pub(crate) stats: &'a PartStats,
}

fn downcast_stats<'a, T: Data>(stats: &'a dyn DynStats) -> Option<&'a T::Stats> {
    match stats.as_any().downcast_ref::<T::Stats>() {
        Some(x) => Some(x),
        None => {
            error!(
                "unexpected stats type for {}: {}",
                std::any::type_name::<T>(),
                stats.type_name()
            );
            None
        }
    }
}

impl PersistSourceDataStats<'_> {
    fn json_spec<'a>(len: usize, stats: &'a JsonStats, arena: &'a RowArena) -> ResultSpec<'a> {
        match stats {
            JsonStats::JsonNulls => ResultSpec::value(Datum::JsonNull),
            JsonStats::Bools(bools) => {
                ResultSpec::value_between(bools.lower.into(), bools.upper.into())
            }
            JsonStats::Strings(strings) => ResultSpec::value_between(
                strings.lower.as_str().into(),
                strings.upper.as_str().into(),
            ),
            JsonStats::Numerics(numerics) => ResultSpec::value_between(
                arena.make_datum(|r| Jsonb::decode(&numerics.lower, r)),
                arena.make_datum(|r| Jsonb::decode(&numerics.upper, r)),
            ),
            JsonStats::Maps(maps) => {
                ResultSpec::map_spec(
                    maps.into_iter()
                        .map(|(k, v)| {
                            let mut v_spec = Self::json_spec(v.len, &v.stats, arena);
                            if v.len != len {
                                // This field is not always present, so assume
                                // that accessing it might be null.
                                v_spec = v_spec.union(ResultSpec::null());
                            }
                            (k.as_str().into(), v_spec)
                        })
                        .collect(),
                )
            }
            JsonStats::None => ResultSpec::nothing(),
            JsonStats::Lists | JsonStats::Mixed => ResultSpec::anything(),
        }
    }

    fn col_stats<'a>(&'a self, id: usize, arena: &'a RowArena) -> ResultSpec<'a> {
        let value_range = self.col_values(id, arena).unwrap_or(ResultSpec::anything());
        let json_range = self.col_json(id, arena);

        // If this is not a JSON column or we don't have JSON stats, json_range is
        // [ResultSpec::anything] and this is a noop.
        value_range.intersect(json_range)
    }

    fn col_json<'a>(&'a self, idx: usize, arena: &'a RowArena) -> ResultSpec<'a> {
        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        match typ {
            ColumnType {
                scalar_type: ScalarType::Jsonb,
                nullable: false,
            } => {
                let stats = self
                    .stats
                    .key
                    .col::<Vec<u8>>(name.as_str())
                    .expect("stats type should match column");
                if let Some(byte_stats) = stats {
                    let value_range = match byte_stats {
                        BytesStats::Json(json_stats) => {
                            Self::json_spec(self.stats.key.len, json_stats, arena)
                        }
                        BytesStats::Primitive(_) | BytesStats::Atomic(_) => ResultSpec::anything(),
                    };
                    value_range
                } else {
                    ResultSpec::anything()
                }
            }
            ColumnType {
                scalar_type: ScalarType::Jsonb,
                nullable: true,
            } => {
                let stats = self
                    .stats
                    .key
                    .col::<Option<Vec<u8>>>(name.as_str())
                    .expect("stats type should match column");
                if let Some(option_stats) = stats {
                    let null_range = match option_stats.none {
                        0 => ResultSpec::nothing(),
                        _ => ResultSpec::null(),
                    };
                    let value_range = match &option_stats.some {
                        BytesStats::Json(json_stats) => {
                            Self::json_spec(self.stats.key.len, json_stats, arena)
                        }
                        BytesStats::Primitive(_) | BytesStats::Atomic(_) => ResultSpec::anything(),
                    };
                    null_range.union(value_range)
                } else {
                    ResultSpec::anything()
                }
            }
            _ => ResultSpec::anything(),
        }
    }

    fn len(&self) -> Option<usize> {
        Some(self.stats.key.len)
    }

    fn err_count(&self) -> Option<usize> {
        // Counter-intuitive: We can easily calculate the number of errors that
        // were None from the column stats, but not how many were Some. So, what
        // we do is count the number of Nones, which is the number of Oks, and
        // then subtract that from the total.
        let num_results = self.stats.key.len;
        let num_oks = self
            .stats
            .key
            .col::<Option<Vec<u8>>>("err")
            .expect("err column should be a Vec<u8>")
            .map(|x| x.none);
        num_oks.map(|num_oks| num_results - num_oks)
    }

    fn col_values<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<ResultSpec> {
        struct ColValues<'a>(&'a dyn DynStats, &'a RowArena, Option<usize>);
        impl<'a> DatumToPersistFn<Option<ResultSpec<'a>>> for ColValues<'a> {
            fn call<T: DatumToPersist>(self) -> Option<ResultSpec<'a>> {
                let ColValues(stats, arena, total_count) = self;
                let stats = downcast_stats::<T::Data>(stats)?;
                let make_datum = |lower| arena.make_datum(|packer| T::decode(lower, packer));
                let min = stats.lower().map(make_datum);
                let max = stats.upper().map(make_datum);
                let null_count = stats.none_count();
                let values = match (total_count, min, max) {
                    (Some(total_count), _, _) if total_count == null_count => ResultSpec::nothing(),
                    (_, Some(min), Some(max)) => ResultSpec::value_between(min, max),
                    _ => ResultSpec::value_all(),
                };
                let nulls = if null_count > 0 {
                    ResultSpec::null()
                } else {
                    ResultSpec::nothing()
                };
                Some(values.union(nulls))
            }
        }

        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let ok_stats = self
            .stats
            .key
            .col::<Option<DynStruct>>("ok")
            .expect("ok column should be a struct")?;
        let stats = ok_stats.some.cols.get(name.as_str())?;
        typ.to_persist(ColValues(stats.as_ref(), arena, self.len()))?
    }
}

/// A trait representing a type that can be used in `backpressure`.
pub trait Backpressureable: Clone + 'static {
    /// Return the weight of the object, in bytes.
    fn byte_size(&self) -> usize;
}

impl Backpressureable for (usize, SerdeLeasedBatchPart) {
    fn byte_size(&self) -> usize {
        self.1.encoded_size_bytes()
    }
}

/// Apply flow control to the `data` input, based on the given `FlowControl`.
///
/// The `FlowControl` should have a `progress_stream` that is the pristine, unaltered
/// frontier of the downstream operator we want to backpressure from, a `max_inflight_bytes`,
/// and a `summary`. Note that the `data` input expects all the second part of the tuple
/// timestamp to be 0, and all data to be on the `chosen_worker` worker.
///
/// The `summary` represents the _minimum_ range of timestamp's that needs to be emitted before
/// reasoning about `max_inflight_bytes`. In practice this means that we may overshoot
/// `max_inflight_bytes`.
///
/// The implementation of this operator is very subtle. Many inline comments have been added.
pub fn backpressure<T, G, O>(
    scope: &mut G,
    name: &str,
    data: &Stream<G, O>,
    flow_control: FlowControl<G>,
    chosen_worker: usize,
    // A probe used to inspect this operator during unit-testing
    probe: Option<UnboundedSender<(Antichain<(T, u64)>, usize, usize)>>,
) -> (Stream<G, O>, Rc<dyn Any>)
where
    T: TimelyTimestamp + Lattice + Codec64 + TotalOrder,
    G: Scope<Timestamp = (T, u64)>,
    O: Backpressureable + std::fmt::Debug,
{
    let worker_index = scope.index();

    let (flow_control_stream, flow_control_max_bytes) = (
        flow_control.progress_stream,
        flow_control.max_inflight_bytes,
    );

    // Both the `flow_control` input and the data input are disconnected from the output. We manually
    // manage the output's frontier using a `CapabilitySet`. Note that we also adjust the
    // `flow_control` progress stream using the `summary` here, using a `feedback` operator in a
    // non-circular fashion.
    let (handle, summaried_flow) = scope.feedback(flow_control.summary.clone());
    flow_control_stream.connect_loop(handle);

    let mut builder = AsyncOperatorBuilder::new(
        format!("persist_source_backpressure({})", name),
        scope.clone(),
    );
    let (mut data_output, data_stream) = builder.new_output();

    let mut data_input = builder.new_input_connection(data, Pipeline, vec![Antichain::new()]);
    let mut flow_control_input =
        builder.new_input_connection(&summaried_flow, Pipeline, vec![Antichain::new()]);

    // Helper method used to synthesize current and next frontier for ordered times.
    fn synthesize_frontiers<T: PartialOrder + Clone>(
        mut frontier: Antichain<(T, u64)>,
        mut time: (T, u64),
        part_number: &mut u64,
    ) -> ((T, u64), Antichain<(T, u64)>, Antichain<(T, u64)>) {
        let mut next_frontier = frontier.clone();
        time.1 = *part_number;
        frontier.insert(time.clone());
        *part_number += 1;
        let mut next_time = time.clone();
        next_time.1 = *part_number;
        next_frontier.insert(next_time);
        (time, frontier, next_frontier)
    }

    // _Refine_ the data stream by amending the second input with the part number. This also
    // ensures that we order the parts by time.
    let data_input = async_stream::stream!({
        let mut part_number = 0;
        let mut parts: Vec<((T, u64), O)> = Vec::new();
        loop {
            match data_input.next_mut().await {
                None => {
                    let empty = Antichain::new();
                    parts.sort_by_key(|val| val.0.clone());
                    for (part_time, d) in parts.drain(..) {
                        let (part_time, frontier, next_frontier) = synthesize_frontiers(
                            empty.clone(),
                            part_time.clone(),
                            &mut part_number,
                        );
                        yield Either::Right((part_time, d, frontier, next_frontier))
                    }
                    break;
                }
                Some(Event::Data(cap, data)) => {
                    for d in data.drain(..) {
                        parts.push((cap.time().clone(), d));
                    }
                }
                Some(Event::Progress(prog)) => {
                    let mut i = 0;
                    parts.sort_by_key(|val| val.0.clone());
                    // This can be replaced with `Vec::drain_filter` when it stabilizes.
                    // `drain_filter_swapping` doesn't work as it reorders the vec.
                    while i < parts.len() {
                        if !prog.less_equal(&parts[i].0) {
                            let (part_time, d) = parts.remove(i);
                            let (part_time, frontier, next_frontier) = synthesize_frontiers(
                                prog.clone(),
                                part_time.clone(),
                                &mut part_number,
                            );
                            yield Either::Right((part_time, d, frontier, next_frontier))
                        } else {
                            i += 1;
                        }
                    }
                    yield Either::Left(prog)
                }
            }
        }
    });
    let shutdown_button = builder.build(move |caps| async move {
        // The output capability.
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        // The frontier of our output. This matches the `CapabilitySet` above.
        let mut output_frontier = Antichain::from_elem(TimelyTimestamp::minimum());
        // The frontier of the `flow_control` input.
        let mut flow_control_frontier = Antichain::from_elem(TimelyTimestamp::minimum());

        // Parts we have emitted, but have not yet retired (based on the `flow_control` edge).
        let mut inflight_parts = Vec::new();
        // Parts we have not yet emitted, but do participate in the `input_frontier`.
        let mut pending_parts = std::collections::VecDeque::new();

        // Only one worker is responsible for distributing parts
        if worker_index != chosen_worker {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            return;
        }
        tokio::pin!(data_input);
        'emitting_parts: loop {
            // At the beginning of our main loop, we determine the total size of
            // inflight parts.
            let inflight_bytes: usize = inflight_parts.iter().map(|(_, size)| size).sum();

            // There are 2 main cases where we can continue to emit parts:
            // - The total emitted bytes is less than `flow_control_max_bytes`.
            // - The output frontier is not beyond the `flow_control_frontier`
            //
            // SUBTLE: in the latter case, we may arbitrarily go into the backpressure `else`
            // block, as we wait for progress tracking to keep the `flow_control` frontier
            // up-to-date. This is tested in unit-tests.
            if inflight_bytes < flow_control_max_bytes
                || !PartialOrder::less_equal(&flow_control_frontier, &output_frontier)
            {
                let (time, part, next_frontier) =
                    if let Some((time, part, next_frontier)) = pending_parts.pop_front() {
                        (time, part, next_frontier)
                    } else {
                        match data_input.next().await {
                            Some(Either::Right((time, part, frontier, next_frontier))) => {
                                // Downgrade the output frontier to this part's time. This is useful
                                // "close" timestamp's from previous parts, even if we don't yet
                                // emit this part. Note that this is safe because `data_input` ensures
                                // time-ordering.
                                output_frontier = frontier;
                                cap_set.downgrade(output_frontier.iter());

                                // If the most recent value's time is _beyond_ the
                                // `flow_control` frontier (which takes into account the `summary`), we
                                // have emitted an entire `summary` worth of data, and can store this
                                // value for later.
                                if inflight_bytes >= flow_control_max_bytes
                                    && !PartialOrder::less_than(
                                        &output_frontier,
                                        &flow_control_frontier,
                                    )
                                {
                                    pending_parts.push_back((time, part, next_frontier));
                                    continue 'emitting_parts;
                                }
                                (time, part, next_frontier)
                            }
                            Some(Either::Left(prog)) => {
                                output_frontier = prog;
                                cap_set.downgrade(output_frontier.iter());
                                continue 'emitting_parts;
                            }
                            None => {
                                if pending_parts.is_empty() {
                                    break 'emitting_parts;
                                } else {
                                    continue 'emitting_parts;
                                }
                            }
                        }
                    };

                // Store the value with the _frontier_ the `flow_control_input` must reach
                // to retire it. Note that if this `results_in` is `None`, then we
                // are at `T::MAX`, and give up on flow_control entirely.
                //
                // SUBTLE: If we stop storing these parts, we will likely never check the
                // `flow_control_input` ever again. This won't pile up data as that input
                // only has frontier updates. There may be spurious activations from it though.
                //
                // Also note that we don't attempt to handle overflowing the `u64` part counter.
                if let Some(emission_ts) = flow_control.summary.results_in(&time) {
                    inflight_parts.push((emission_ts, part.byte_size()));
                }

                // Emit the data at the given time, and update the frontier and capabilities
                // to just beyond the part.
                data_output.give(&cap_set.delayed(&time), part).await;
                output_frontier = next_frontier;
                cap_set.downgrade(output_frontier.iter())
            } else {
                let parts_count = inflight_parts.len();
                // We've exhausted our budget, listen for updates to the flow_control
                // input's frontier until we free up new budget. If we don't interact with
                // with this side of the if statement, because the stream has no data, we
                // don't cause unbounded buffering in timely.
                let new_flow_control_frontier = match flow_control_input.next().await {
                    Some(Event::Progress(frontier)) => frontier,
                    Some(Event::Data(_, _)) => {
                        unreachable!("flow_control_input should not contain data")
                    }
                    None => Antichain::new(),
                };

                // Update the `flow_control_frontier` if its advanced.
                flow_control_frontier = new_flow_control_frontier.clone();

                // Retire parts that are processed downstream.
                let retired_parts = inflight_parts
                    .drain_filter_swapping(|(ts, _size)| !flow_control_frontier.less_equal(ts));
                let (retired_size, retired_count): (usize, usize) = retired_parts
                    .fold((0, 0), |(accum_size, accum_count), (_ts, size)| {
                        (accum_size + size, accum_count + 1)
                    });
                trace!(
                    "returning {} parts with {} bytes, frontier: {:?}",
                    retired_count,
                    retired_size,
                    flow_control_frontier,
                );

                // Optionally emit some information for tests to examine.
                if let Some(probe) = probe.as_ref() {
                    let _ = probe.send((new_flow_control_frontier, parts_count, retired_count));
                }
            }
        }
    });
    (data_stream, Rc::new(shutdown_button.press_on_drop()))
}

#[cfg(test)]
mod tests {
    use mz_persist_client::stats::PartStats;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{PartEncoder, Schema};
    use mz_persist_types::part::PartBuilder;
    use timely::dataflow::operators::Probe;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;

    use mz_repr::{
        is_no_stats_type, ColumnType, Datum, DatumToPersist, DatumToPersistFn, RelationDesc, Row,
        RowArena, ScalarType,
    };
    use proptest::prelude::*;

    use super::*;
    use crate::source::persist_source::PersistSourceDataStats;
    use crate::types::sources::SourceData;

    fn scalar_type_stats_roundtrip(scalar_type: ScalarType) {
        // Skip types that we don't keep stats for (yet).
        if is_no_stats_type(&scalar_type) {
            return;
        }

        struct ValidateStats<'a>(PersistSourceDataStats<'a>, &'a RowArena, Datum<'a>);
        impl<'a> DatumToPersistFn<()> for ValidateStats<'a> {
            fn call<T: DatumToPersist>(self) -> () {
                let ValidateStats(stats, arena, datum) = self;
                if let Some(spec) = stats.col_values(0, arena) {
                    assert!(spec.may_contain(datum));
                }
            }
        }

        fn validate_stats(column_type: &ColumnType, datum: Datum<'_>) -> Result<(), String> {
            let schema = RelationDesc::empty().with_column("col", column_type.clone());
            let row = SourceData(Ok(Row::pack(std::iter::once(datum))));

            let mut part = PartBuilder::new::<SourceData, _, _, _>(&schema, &UnitSchema);
            {
                let mut part_mut = part.get_mut();
                <RelationDesc as Schema<SourceData>>::encoder(&schema, part_mut.key)?.encode(&row);
                part_mut.ts.push(1u64);
                part_mut.diff.push(1i64);
            }
            let part = part.finish()?;
            let stats = part.key_stats()?;

            let stats = PersistSourceDataStats {
                stats: &PartStats { key: stats },
                desc: &schema,
            };
            let arena = RowArena::default();
            column_type.to_persist(ValidateStats(stats, &arena, datum));
            Ok(())
        }

        // Non-nullable version of the column.
        let column_type = scalar_type.clone().nullable(false);
        for datum in scalar_type.interesting_datums() {
            assert_eq!(validate_stats(&column_type, datum), Ok(()));
        }

        // Nullable version of the column.
        let column_type = scalar_type.clone().nullable(true);
        for datum in scalar_type.interesting_datums() {
            assert_eq!(validate_stats(&column_type, datum), Ok(()));
        }
        assert_eq!(validate_stats(&column_type, Datum::Null), Ok(()));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_scalar_types_stats_roundtrip() {
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_stats_roundtrip(scalar_type)
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Reports undefined behavior
    fn test_backpressure_non_granular() {
        use Step::*;
        backpressure_runner(
            vec![(50, Part(101)), (50, Part(102)), (100, Part(1))],
            100,
            (1, 0),
            vec![
                // Assert we backpressure only after we have emitted
                // the entire timestamp.
                AssertOutputFrontier((50, 2)),
                AssertBackpressured {
                    frontier: (1, 0),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (51, 0),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                ProcessXParts(2),
                AssertBackpressured {
                    frontier: (101, 0),
                    inflight_parts: 2,
                    retired_parts: 2,
                },
                // Assert we make later progress once processing
                // the parts.
                AssertOutputFrontier((100, 3)),
            ],
            true,
        );

        backpressure_runner(
            vec![
                (50, Part(10)),
                (50, Part(10)),
                (51, Part(100)),
                (52, Part(1000)),
            ],
            50,
            (1, 0),
            vec![
                // Assert we backpressure only after we emitted enough bytes
                AssertOutputFrontier((51, 3)),
                AssertBackpressured {
                    frontier: (1, 0),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                ProcessXParts(3),
                AssertBackpressured {
                    frontier: (52, 0),
                    inflight_parts: 3,
                    retired_parts: 2,
                },
                AssertBackpressured {
                    frontier: (53, 0),
                    inflight_parts: 1,
                    retired_parts: 1,
                },
                // Assert we make later progress once processing
                // the parts.
                AssertOutputFrontier((52, 4)),
            ],
            true,
        );

        backpressure_runner(
            vec![
                (50, Part(98)),
                (50, Part(1)),
                (51, Part(10)),
                (52, Part(100)),
                // Additional parts at the same timestamp
                (52, Part(10)),
                (52, Part(10)),
                (52, Part(10)),
                (52, Part(100)),
                // A later part with a later ts.
                (100, Part(100)),
            ],
            100,
            (1, 0),
            vec![
                AssertOutputFrontier((51, 3)),
                // Assert we backpressure after we have emitted enough bytes.
                // We assert twice here because we get updates as
                // `flow_control` progresses from `(0, 0)`->`(0, 1)`-> a real frontier.
                AssertBackpressured {
                    frontier: (1, 0),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (51, 0),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                ProcessXParts(1),
                // Our output frontier doesn't move, as the downstream frontier hasn't moved past
                // 50.
                AssertOutputFrontier((51, 3)),
                // After we process all of `50`, we can start emitting data at `52`, but only until
                // we exhaust out budget. We don't need to emit all of `52` because we have emitted
                // all of `51`.
                ProcessXParts(1),
                AssertOutputFrontier((52, 4)),
                AssertBackpressured {
                    frontier: (52, 0),
                    inflight_parts: 3,
                    retired_parts: 2,
                },
                // After processing `50` and `51`, the minimum time is `52`, so we ensure that,
                // regardless of byte count, we emit the entire time (but do NOT emit the part at
                // time `100`.
                ProcessXParts(1),
                // Clear the previous `51` part, and start filling up `inflight_parts` with other
                // parts at `52`
                // This is an intermediate state.
                AssertBackpressured {
                    frontier: (53, 0),
                    inflight_parts: 2,
                    retired_parts: 1,
                },
                // After we process all of `52`, we can continue to the next time.
                ProcessXParts(5),
                AssertBackpressured {
                    frontier: (101, 0),
                    inflight_parts: 5,
                    retired_parts: 5,
                },
                AssertOutputFrontier((100, 9)),
            ],
            true,
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Reports undefined behavior
    fn test_backpressure_granular() {
        use Step::*;
        backpressure_runner(
            vec![(50, Part(101)), (50, Part(101))],
            100,
            (0, 1),
            vec![
                // Advance our frontier to outputting a single part.
                AssertOutputFrontier((50, 1)),
                // Receive backpressure updates until our frontier is up-to-date but
                // not beyond the parts (while considering the summary).
                AssertBackpressured {
                    frontier: (0, 1),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (50, 1),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                // Process that part.
                ProcessXParts(1),
                // Assert that we clear the backpressure status
                AssertBackpressured {
                    frontier: (50, 2),
                    inflight_parts: 1,
                    retired_parts: 1,
                },
                // Ensure we make progress to the next part.
                AssertOutputFrontier((50, 2)),
            ],
            false,
        );

        backpressure_runner(
            vec![
                (50, Part(10)),
                (50, Part(10)),
                (51, Part(35)),
                (52, Part(100)),
            ],
            50,
            (0, 1),
            vec![
                // we can emit 3 parts before we hit the backpressure limit
                AssertOutputFrontier((51, 3)),
                AssertBackpressured {
                    frontier: (0, 1),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (50, 1),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                // Retire the single part.
                ProcessXParts(1),
                AssertBackpressured {
                    frontier: (50, 2),
                    inflight_parts: 3,
                    retired_parts: 1,
                },
                // Ensure we make progress, and then
                // can retire the next 2 parts.
                AssertOutputFrontier((52, 4)),
                ProcessXParts(2),
                AssertBackpressured {
                    frontier: (52, 4),
                    inflight_parts: 3,
                    retired_parts: 2,
                },
            ],
            false,
        );
    }

    type Time = (u64, u64);
    #[derive(Clone, Debug)]
    struct Part(usize);
    impl Backpressureable for Part {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    /// Actions taken by `backpressure_runner`.
    enum Step {
        /// Assert that the output frontier of the `backpressure` operator has AT LEAST made it
        /// this far. This is a single time because we assume
        AssertOutputFrontier(Time),
        /// Assert that we have entered the backpressure flow in the `backpressure` operator. This
        /// allows us to assert what feedback frontier we got to, and how many inflight parts we
        /// retired.
        AssertBackpressured {
            frontier: Time,
            inflight_parts: usize,
            retired_parts: usize,
        },
        /// Process X parts in the downstream operator. This affects the feedback frontier.
        ProcessXParts(usize),
    }

    /// A function that runs the `steps` to ensure that `backpressure` works as expected.
    fn backpressure_runner(
        // The input data to the `backpressure` operator
        input: Vec<(u64, Part)>,
        // The maximum inflight bytes the `backpressure` operator allows through.
        max_inflight_bytes: usize,
        // The feedback summary used by the `backpressure` operator.
        summary: Time,
        // List of steps to run through.
        steps: Vec<Step>,
        // Whether or not to consume records in the non-granular scope. This is useful when the
        // `summary` is something like `(1, 0)`.
        non_granular_consumer: bool,
    ) {
        timely::execute::execute_directly(move |worker| {
            let (backpressure_probe, consumer_tx, mut backpressure_status_rx, finalizer_tx, _token) =
                // Set up the top-level non-granular scope.
                worker.dataflow::<u64, _, _>(|scope| {
                    let (non_granular_feedback_handle, non_granular_feedback) =
                        if non_granular_consumer {
                            let (h, f) = scope.feedback(Default::default());
                            (Some(h), Some(f))
                        } else {
                            (None, None)
                        };
                    let (
                        backpressure_probe,
                        consumer_tx,
                        backpressure_status_rx,
                        token,
                        backpressured,
                        finalizer_tx,
                    ) = scope.scoped::<(u64, u64), _, _>("hybrid", |scope| {
                        let (input, finalizer_tx) =
                            iterator_operator(scope.clone(), input.into_iter());

                        let (flow_control, granular_feedback_handle) = if non_granular_consumer {
                            (
                                FlowControl {
                                    progress_stream: non_granular_feedback.unwrap().enter(scope),
                                    max_inflight_bytes,
                                    summary,
                                },
                                None,
                            )
                        } else {
                            let (granular_feedback_handle, granular_feedback) =
                                scope.feedback(Default::default());
                            (
                                FlowControl {
                                    progress_stream: granular_feedback,
                                    max_inflight_bytes,
                                    summary,
                                },
                                Some(granular_feedback_handle),
                            )
                        };

                        let (backpressure_status_tx, backpressure_status_rx) = unbounded_channel();

                        let (backpressured, token) = backpressure(
                            scope,
                            "test",
                            &input,
                            flow_control,
                            0,
                            Some(backpressure_status_tx),
                        );

                        // If we want to granularly consume the output, we setup the consumer here.
                        let tx = if !non_granular_consumer {
                            Some(consumer_operator(
                                scope.clone(),
                                &backpressured,
                                granular_feedback_handle.unwrap(),
                            ))
                        } else {
                            None
                        };

                        (
                            backpressured.probe(),
                            tx,
                            backpressure_status_rx,
                            token,
                            backpressured.leave(),
                            finalizer_tx,
                        )
                    });

                    // If we want to non-granularly consume the output, we setup the consumer here.
                    let consumer_tx = if non_granular_consumer {
                        consumer_operator(
                            scope.clone(),
                            &backpressured,
                            non_granular_feedback_handle.unwrap(),
                        )
                    } else {
                        consumer_tx.unwrap()
                    };

                    (
                        backpressure_probe,
                        consumer_tx,
                        backpressure_status_rx,
                        finalizer_tx,
                        token,
                    )
                });

            use Step::*;
            for step in steps {
                match step {
                    AssertOutputFrontier(time) => {
                        eprintln!("checking advance to {time:?}");
                        backpressure_probe.with_frontier(|front| {
                            eprintln!("current backpressure output frontier: {front:?}");
                        });
                        while backpressure_probe.less_than(&time) {
                            worker.step();
                            backpressure_probe.with_frontier(|front| {
                                eprintln!("current backpressure output frontier: {front:?}");
                            });
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                    }
                    ProcessXParts(parts) => {
                        eprintln!("processing {parts:?} parts");
                        for _ in 0..parts {
                            consumer_tx.send(()).unwrap();
                        }
                    }
                    AssertBackpressured {
                        frontier,
                        inflight_parts,
                        retired_parts,
                    } => {
                        let frontier = Antichain::from_elem(frontier);
                        eprintln!(
                            "asserting backpressured at {frontier:?}, with {inflight_parts:?} inflight parts \
                            and {retired_parts:?} retired"
                        );
                        let (new_frontier, new_count, new_retired_count) = loop {
                            if let Ok(val) = backpressure_status_rx.try_recv() {
                                break val;
                            }
                            worker.step();
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        };
                        assert_eq!(
                            (frontier, inflight_parts, retired_parts),
                            (new_frontier, new_count, new_retired_count)
                        );
                    }
                }
            }
            // Send the input to the empty frontier.
            let _ = finalizer_tx.send(());
        });
    }

    /// An operator that emits `Part`'s at the specified timestamps. Does not
    /// drop its capability until it gets a signal from the `Sender` it returns.
    fn iterator_operator<
        G: Scope<Timestamp = (u64, u64)>,
        I: Iterator<Item = (u64, Part)> + 'static,
    >(
        scope: G,
        mut input: I,
    ) -> (Stream<G, Part>, oneshot::Sender<()>) {
        let (finalizer_tx, finalizer_rx) = oneshot::channel();
        let mut iterator = AsyncOperatorBuilder::new("iterator".to_string(), scope);
        let (mut output_handle, output) = iterator.new_output::<Vec<Part>>();

        iterator.build(|mut caps| async move {
            let mut capability = Some(caps.pop().unwrap());
            let mut last = None;
            while let Some(element) = input.next() {
                let time = element.0.clone();
                let part = element.1;
                last = Some((time, 0));
                output_handle
                    .give(&capability.as_ref().unwrap().delayed(&last.unwrap()), part)
                    .await;
            }
            if let Some(last) = last {
                capability
                    .as_mut()
                    .unwrap()
                    .downgrade(&(last.0 + 1, last.1));
            }

            let _ = finalizer_rx.await;
            capability.take();
        });

        (output, finalizer_tx)
    }

    /// An operator that consumes its input ONLY when given a signal to do from
    /// the `UnboundedSender` it returns. Each `send` corresponds with 1 `Data` event
    /// being processed. Also connects the `feedback` handle to its output.
    fn consumer_operator<G: Scope, O: Backpressureable + std::fmt::Debug>(
        scope: G,
        input: &Stream<G, O>,
        feedback: timely::dataflow::operators::feedback::Handle<G, std::convert::Infallible>,
    ) -> UnboundedSender<()> {
        let (tx, mut rx) = unbounded_channel::<()>();
        let mut consumer = AsyncOperatorBuilder::new("consumer".to_string(), scope);
        let mut input = consumer.new_input(input, Pipeline);
        let (_output_handle, output) = consumer.new_output::<Vec<std::convert::Infallible>>();

        consumer.build(|_caps| async move {
            while let Some(()) = rx.recv().await {
                // Consume exactly one messages (unless the input is exhausted).
                while let Some(Event::Progress(_)) = input.next().await {}
            }
        });
        output.connect_loop(feedback);

        tx
    }
}
