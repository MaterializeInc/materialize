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

use mz_expr::{ColumnSpecs, Interpreter, MfpPlan, ResultSpec, UnmaterializableFunc};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_persist_client::operators::shard_source::shard_source;
pub use mz_persist_client::operators::shard_source::FlowControl;
use mz_persist_client::stats::PartStats;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::Data;
use mz_persist_types::dyn_struct::DynStruct;
use mz_persist_types::stats::{BytesStats, ColumnStats, DynStats, JsonStats};

use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{
    ColumnType, Datum, DatumToPersist, DatumToPersistFn, DatumVec, Diff, GlobalId, RelationDesc,
    RelationType, Row, RowArena, ScalarType, Timestamp,
};
use mz_timely_util::buffer::ConsolidateBuffer;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::Activator;
use tracing::error;

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
    let (fetched, token) = shard_source(
        &mut scope.clone(),
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        flow_control,
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

    let total_count = stats.len();
    for (id, _) in relation_type.column_types.iter().enumerate() {
        let min = stats.col_min(id, &arena);
        let max = stats.col_max(id, &arena);
        let nulls = stats.col_null_count(id);
        let json_range = stats.col_json(id, &arena);

        let value_range = match (total_count, min, max, nulls) {
            (Some(total_count), _, _, Some(nulls)) if total_count == nulls => ResultSpec::nothing(),
            (_, Some(min), Some(max), _) => ResultSpec::value_between(min, max),
            _ => ResultSpec::value_all(),
        };

        // If this is not a JSON column or we don't have JSON stats, json_range is
        // [ResultSpec::anything] and this is a noop.
        let value_range = value_range.intersect(json_range);

        let null_range = match nulls {
            Some(0) => ResultSpec::nothing(),
            _ => ResultSpec::null(),
        };
        ranges.push_column(id, value_range.union(null_range));
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
) -> Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
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
        name: &str,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut ConsolidateBuffer<Timestamp, Result<Row, DataflowError>, Diff, P>,
    ) -> bool
    where
        P: Push<Bundle<Timestamp, (Result<Row, DataflowError>, Timestamp, Diff)>>,
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
                                        output.give_at(&self.capability, (Ok(row), time, diff));
                                        *work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        output.give_at(&self.capability, (Err(err), time, diff));
                                        *work += 1;
                                    }
                                }
                            }
                        }
                    } else {
                        output.give_at(&self.capability, (Ok(row), time, diff));
                        *work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    output.give_at(&self.capability, (Err(err), time, diff));
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

    fn col_min<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<Datum<'a>> {
        struct ColMin<'a>(&'a dyn DynStats, &'a RowArena);
        impl<'a> DatumToPersistFn<Option<Datum<'a>>> for ColMin<'a> {
            fn call<T: DatumToPersist>(self) -> Option<Datum<'a>> {
                let ColMin(stats, arena) = self;
                downcast_stats::<T::Data>(stats)?
                    .lower()
                    .map(|val| arena.make_datum(|packer| T::decode(val, packer)))
            }
        }

        if self.len() <= self.col_null_count(idx) {
            return None;
        }
        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let ok_stats = self
            .stats
            .key
            .col::<Option<DynStruct>>("ok")
            .expect("ok column should be a struct")?;
        let stats = ok_stats.some.cols.get(name.as_str())?;
        typ.to_persist(ColMin(stats.as_ref(), arena))?
    }

    fn col_max<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<Datum<'a>> {
        struct ColMax<'a>(&'a dyn DynStats, &'a RowArena);
        impl<'a> DatumToPersistFn<Option<Datum<'a>>> for ColMax<'a> {
            fn call<T: DatumToPersist>(self) -> Option<Datum<'a>> {
                let ColMax(stats, arena) = self;
                downcast_stats::<T::Data>(stats)?
                    .upper()
                    .map(|val| arena.make_datum(|packer| T::decode(val, packer)))
            }
        }

        if self.len() <= self.col_null_count(idx) {
            return None;
        }
        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let ok_stats = self
            .stats
            .key
            .col::<Option<DynStruct>>("ok")
            .expect("ok column should be a struct")?;
        let stats = ok_stats.some.cols.get(name.as_str())?;
        typ.to_persist(ColMax(stats.as_ref(), arena))?
    }

    fn col_null_count(&self, idx: usize) -> Option<usize> {
        struct ColNullCount<'a>(&'a dyn DynStats);
        impl<'a> DatumToPersistFn<Option<usize>> for ColNullCount<'a> {
            fn call<T: DatumToPersist>(self) -> Option<usize> {
                let ColNullCount(stats) = self;
                let stats = downcast_stats::<T::Data>(stats)?;
                Some(stats.none_count())
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
        typ.to_persist(ColNullCount(stats.as_ref()))?
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::stats::PartStats;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{PartEncoder, Schema};
    use mz_persist_types::part::PartBuilder;
    use mz_repr::{
        is_no_stats_type, ColumnType, Datum, DatumToPersist, DatumToPersistFn, RelationDesc, Row,
        RowArena, ScalarType,
    };
    use proptest::prelude::*;

    use crate::source::persist_source::PersistSourceDataStats;
    use crate::types::sources::SourceData;

    fn scalar_type_stats_roundtrip(scalar_type: ScalarType) {
        // Skip types that we don't keep stats for (yet).
        if is_no_stats_type(&scalar_type) {
            return;
        }

        struct ValidateStatsSome<'a>(PersistSourceDataStats<'a>, &'a RowArena, Datum<'a>);
        impl<'a> DatumToPersistFn<()> for ValidateStatsSome<'a> {
            fn call<T: DatumToPersist>(self) -> () {
                let ValidateStatsSome(stats, arena, datum) = self;
                if let Some(lower) = stats.col_min(0, arena) {
                    assert!(lower <= datum, "{} vs {} stats={:?}", lower, datum, stats);
                }
                if let Some(upper) = stats.col_max(0, arena) {
                    assert!(upper >= datum, "{} vs {}", upper, datum);
                }
                assert_eq!(stats.col_null_count(0), Some(0));
            }
        }

        struct ValidateStatsNone<'a>(PersistSourceDataStats<'a>, &'a RowArena);
        impl<'a> DatumToPersistFn<()> for ValidateStatsNone<'a> {
            fn call<T: DatumToPersist>(self) -> () {
                let ValidateStatsNone(stats, arena) = self;
                assert_eq!(stats.col_min(0, arena), None);
                assert_eq!(stats.col_max(0, arena), None);
                assert_eq!(stats.col_null_count(0), Some(1));
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
            if datum.is_null() {
                column_type.to_persist(ValidateStatsNone(stats, &arena));
            } else {
                column_type.to_persist(ValidateStatsSome(stats, &arena, datum));
            }
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
}
