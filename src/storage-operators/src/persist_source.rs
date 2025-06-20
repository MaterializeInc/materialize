// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_dyncfg::ConfigSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use futures::{StreamExt, future::Either};
use mz_expr::{ColumnSpecs, Interpreter, MfpPlan, ResultSpec, UnmaterializableFunc};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::{PersistConfig, RetryParameters};
use mz_persist_client::fetch::{ExchangeableBatchPart, ShardSourcePart};
use mz_persist_client::fetch::{FetchedBlob, FetchedPart};
use mz_persist_client::operators::shard_source::{
    ErrorHandler, FilterResult, SnapshotMode, shard_source,
};
use mz_persist_types::Codec64;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::{ColumnEncoder, Schema};
use mz_repr::{Datum, DatumVec, Diff, GlobalId, RelationDesc, Row, RowArena, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, TxnsCodecRow};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::stats::RelationPartStats;
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::probe::ProbeNotify;
use mz_txn_wal::operator::{TxnsContext, txns_progress};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::communication::Push;
use timely::dataflow::ScopeParent;
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandleCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, Leave, OkErr};
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Feedback};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::progress::timestamp::PathSummary;
use timely::scheduling::Activator;
use tokio::sync::mpsc::UnboundedSender;
use tracing::trace;

use crate::metrics::BackpressureMetrics;

/// This opaque token represents progress within a timestamp, allowing finer-grained frontier
/// progress than would otherwise be possible.
///
/// This is "opaque" since we'd like to reserve the right to change the definition in the future
/// without downstreams being able to rely on the precise representation. (At the moment, this
/// is a simple batch counter, though we may change it to eg. reflect progress through the keyspace
/// in the future.)
#[derive(
    Copy, Clone, PartialEq, Default, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, Hash,
)]
pub struct Subtime(u64);

impl PartialOrder for Subtime {
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
    }
}

impl TotalOrder for Subtime {}

impl PathSummary<Subtime> for Subtime {
    fn results_in(&self, src: &Subtime) -> Option<Subtime> {
        self.0.results_in(&src.0).map(Subtime)
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.0.followed_by(&other.0).map(Subtime)
    }
}

impl TimelyTimestamp for Subtime {
    type Summary = Subtime;

    fn minimum() -> Self {
        Subtime(0)
    }
}

impl Subtime {
    /// The smallest non-zero summary for the opaque timestamp type.
    pub const fn least_summary() -> Self {
        Subtime(1)
    }
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
pub fn persist_source<G>(
    scope: &mut G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: &TxnsContext,
    // In case we need to use a dyncfg to decide which operators to render in a
    // dataflow.
    worker_dyncfgs: &ConfigSet,
    metadata: CollectionMetadata,
    read_schema: Option<RelationDesc>,
    as_of: Option<Antichain<Timestamp>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    max_inflight_bytes: Option<usize>,
    start_signal: impl Future<Output = ()> + 'static,
    error_handler: ErrorHandler,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Vec<PressOnDropButton>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let shard_metrics = persist_clients.shard_metrics(&metadata.data_shard, &source_id.to_string());

    let mut tokens = vec![];

    let stream = scope.scoped(&format!("granular_backpressure({})", source_id), |scope| {
        let (flow_control, flow_control_probe) = match max_inflight_bytes {
            Some(max_inflight_bytes) => {
                let backpressure_metrics = BackpressureMetrics {
                    emitted_bytes: Arc::clone(&shard_metrics.backpressure_emitted_bytes),
                    last_backpressured_bytes: Arc::clone(
                        &shard_metrics.backpressure_last_backpressured_bytes,
                    ),
                    retired_bytes: Arc::clone(&shard_metrics.backpressure_retired_bytes),
                };

                let probe = mz_timely_util::probe::Handle::default();
                let progress_stream = mz_timely_util::probe::source(
                    scope.clone(),
                    format!("decode_backpressure_probe({source_id})"),
                    probe.clone(),
                );
                let flow_control = FlowControl {
                    progress_stream,
                    max_inflight_bytes,
                    summary: (Default::default(), Subtime::least_summary()),
                    metrics: Some(backpressure_metrics),
                };
                (Some(flow_control), Some(probe))
            }
            None => (None, None),
        };

        // Our default listen sleeps are tuned for the case of a shard that is
        // written once a second, but txn-wal allows these to be lazy.
        // Override the tuning to reduce crdb load. The pubsub fallback
        // responsibility is then replaced by manual "one state" wakeups in the
        // txns_progress operator.
        let cfg = Arc::clone(&persist_clients.cfg().configs);
        let subscribe_sleep = match metadata.txns_shard {
            Some(_) => Some(move || mz_txn_wal::operator::txns_data_shard_retry_params(&cfg)),
            None => None,
        };

        let (stream, source_tokens) = persist_source_core(
            scope,
            source_id,
            Arc::clone(&persist_clients),
            metadata.clone(),
            read_schema,
            as_of.clone(),
            snapshot_mode,
            until.clone(),
            map_filter_project,
            flow_control,
            subscribe_sleep,
            start_signal,
            error_handler,
        );
        tokens.extend(source_tokens);

        let stream = match flow_control_probe {
            Some(probe) => stream.probe_notify_with(vec![probe]),
            None => stream,
        };

        stream.leave()
    });

    // If a txns_shard was provided, then this shard is in the txn-wal
    // system. This means the "logical" upper may be ahead of the "physical"
    // upper. Render a dataflow operator that passes through the input and
    // translates the progress frontiers as necessary.
    let (stream, txns_tokens) = match metadata.txns_shard {
        Some(txns_shard) => txns_progress::<SourceData, (), Timestamp, i64, _, TxnsCodecRow, _, _>(
            stream,
            &source_id.to_string(),
            txns_ctx,
            worker_dyncfgs,
            move || {
                let (c, l) = (
                    Arc::clone(&persist_clients),
                    metadata.persist_location.clone(),
                );
                async move { c.open(l).await.expect("location is valid") }
            },
            txns_shard,
            metadata.data_shard,
            as_of
                .expect("as_of is provided for table sources")
                .into_option()
                .expect("shard is not closed"),
            until,
            Arc::new(metadata.relation_desc),
            Arc::new(UnitSchema),
        ),
        None => (stream, vec![]),
    };
    tokens.extend(txns_tokens);
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t.0, r)),
        Err(err) => Err((err, t.0, r)),
    });
    (ok_stream, err_stream, tokens)
}

type RefinedScope<'g, G> = Child<'g, G, (<G as ScopeParent>::Timestamp, Subtime)>;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<'g, G>(
    scope: &RefinedScope<'g, G>,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    read_schema: Option<RelationDesc>,
    as_of: Option<Antichain<Timestamp>>,
    snapshot_mode: SnapshotMode,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<RefinedScope<'g, G>>>,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
    start_signal: impl Future<Output = ()> + 'static,
    error_handler: ErrorHandler,
) -> (
    Stream<
        RefinedScope<'g, G>,
        (
            Result<Row, DataflowError>,
            (mz_repr::Timestamp, Subtime),
            Diff,
        ),
    >,
    Vec<PressOnDropButton>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let cfg = persist_clients.cfg().clone();
    let name = source_id.to_string();
    let filter_plan = map_filter_project.as_ref().map(|p| (*p).clone());

    // N.B. `read_schema` may be a subset of the total columns for this shard.
    let read_desc = match read_schema {
        Some(desc) => desc,
        None => metadata.relation_desc,
    };

    let desc_transformer = match flow_control {
        Some(flow_control) => Some(move |mut scope: _, descs: &Stream<_, _>, chosen_worker| {
            let (stream, token) = backpressure(
                &mut scope,
                &format!("backpressure({source_id})"),
                descs,
                flow_control,
                chosen_worker,
                None,
            );
            (stream, vec![token])
        }),
        None => None,
    };

    let metrics = Arc::clone(persist_clients.metrics());
    let filter_name = name.clone();
    // The `until` gives us an upper bound on the possible values of `mz_now` this query may see.
    // Ranges are inclusive, so it's safe to use the maximum timestamp as the upper bound when
    // `until ` is the empty antichain.
    let upper = until.as_option().cloned().unwrap_or(Timestamp::MAX);
    let (fetched, token) = shard_source(
        &mut scope.clone(),
        &name,
        move || {
            let (c, l) = (
                Arc::clone(&persist_clients),
                metadata.persist_location.clone(),
            );
            async move { c.open(l).await.unwrap() }
        },
        metadata.data_shard,
        as_of,
        snapshot_mode,
        until.clone(),
        desc_transformer,
        Arc::new(read_desc.clone()),
        Arc::new(UnitSchema),
        move |stats, frontier| {
            let Some(lower) = frontier.as_option().copied() else {
                // If the frontier has advanced to the empty antichain,
                // we'll never emit any rows from any part.
                return FilterResult::Discard;
            };

            if lower > upper {
                // The frontier timestamp is larger than the until of the dataflow:
                // anything from this part will necessarily be filtered out.
                return FilterResult::Discard;
            }

            let time_range =
                ResultSpec::value_between(Datum::MzTimestamp(lower), Datum::MzTimestamp(upper));
            if let Some(plan) = &filter_plan {
                let metrics = &metrics.pushdown.part_stats;
                let stats = RelationPartStats::new(&filter_name, metrics, &read_desc, stats);
                filter_result(&read_desc, time_range, stats, plan)
            } else {
                FilterResult::Keep
            }
        },
        listen_sleep,
        start_signal,
        error_handler,
    );
    let rows = decode_and_mfp(cfg, &fetched, &name, until, map_filter_project);
    (rows, token)
}

fn filter_result(
    relation_desc: &RelationDesc,
    time_range: ResultSpec,
    stats: RelationPartStats,
    plan: &MfpPlan,
) -> FilterResult {
    let arena = RowArena::new();
    let mut ranges = ColumnSpecs::new(relation_desc.typ(), &arena);
    ranges.push_unmaterializable(UnmaterializableFunc::MzNow, time_range);

    let may_error = stats.err_count().map_or(true, |count| count > 0);

    // N.B. We may have pushed down column "demands" into Persist, so this
    // relation desc may have a different set of columns than the stats.
    for (pos, (idx, _, _)) in relation_desc.iter_all().enumerate() {
        let result_spec = stats.col_stats(idx, &arena);
        ranges.push_column(pos, result_spec);
    }
    let result = ranges.mfp_plan_filter(plan).range;
    let may_error = may_error || result.may_fail();
    let may_keep = result.may_contain(Datum::True);
    let may_skip = result.may_contain(Datum::False) || result.may_contain(Datum::Null);
    if relation_desc.len() == 0 && !may_error && !may_skip {
        let Ok(mut key) = <RelationDesc as Schema<SourceData>>::encoder(relation_desc) else {
            return FilterResult::Keep;
        };
        key.append(&SourceData(Ok(Row::default())));
        let key = key.finish();
        let Ok(mut val) = <UnitSchema as Schema<()>>::encoder(&UnitSchema) else {
            return FilterResult::Keep;
        };
        val.append(&());
        let val = val.finish();

        FilterResult::ReplaceWith {
            key: Arc::new(key),
            val: Arc::new(val),
        }
    } else if may_error || may_keep {
        FilterResult::Keep
    } else {
        FilterResult::Discard
    }
}

pub fn decode_and_mfp<G>(
    cfg: PersistConfig,
    fetched: &Stream<G, FetchedBlob<SourceData, (), Timestamp, StorageDiff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
) -> Stream<G, (Result<Row, DataflowError>, G::Timestamp, Diff)>
where
    G: Scope<Timestamp = (mz_repr::Timestamp, Subtime)>,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        scope.clone(),
    );
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) =
        builder.new_output::<ConsolidatingContainerBuilder<_>>();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |_caps| {
        let name = name.to_owned();
        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activations = scope.activations();
        let activator = Activator::new(operator_info.address, activations);
        // Maintain a list of work to do
        let mut pending_work = std::collections::VecDeque::new();

        move |_frontier| {
            fetched_input.for_each(|time, data| {
                let capability = time.retain();
                for fetched_blob in data.drain(..) {
                    pending_work.push_back(PendingWork {
                        capability: capability.clone(),
                        part: PendingPart::Unparsed(fetched_blob),
                    })
                }
            });

            // Get dyncfg values once per schedule to amortize the cost of
            // loading the atomics.
            let yield_fuel = cfg.storage_source_decode_fuel();
            let yield_fn = |_, work| work >= yield_fuel;

            let mut work = 0;
            let start_time = Instant::now();
            let mut output = updates_output.activate();
            while !pending_work.is_empty() && !yield_fn(start_time, work) {
                let done = pending_work.front_mut().unwrap().do_work(
                    &cfg,
                    &mut work,
                    &name,
                    start_time,
                    yield_fn,
                    &until,
                    map_filter_project.as_ref(),
                    &mut datum_vec,
                    &mut row_builder,
                    &mut output,
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
    capability: Capability<(mz_repr::Timestamp, Subtime)>,
    /// Pending fetched part.
    part: PendingPart,
}

enum PendingPart {
    Unparsed(FetchedBlob<SourceData, (), Timestamp, StorageDiff>),
    Parsed {
        part: ShardSourcePart<SourceData, (), Timestamp, StorageDiff>,
    },
}

impl PendingPart {
    /// Returns the contained `FetchedPart`, first parsing it from a
    /// `FetchedBlob` if necessary.
    ///
    /// Also returns a bool, which is true if the part is known (from pushdown
    /// stats) to be free of `SourceData(Err(_))`s. It will be false if the part
    /// is known to contain errors or if it's unknown.
    fn part_mut(
        &mut self,
        cfg: &PersistConfig,
    ) -> &mut FetchedPart<SourceData, (), Timestamp, StorageDiff> {
        match self {
            PendingPart::Unparsed(x) => {
                *self = PendingPart::Parsed {
                    part: x.parse(cfg.clone()),
                };
                // Won't recurse any further.
                self.part_mut(cfg)
            }
            PendingPart::Parsed { part } => &mut part.part,
        }
    }
}

impl PendingWork {
    /// Perform work, reading from the fetched part, decoding, and sending outputs, while checking
    /// `yield_fn` whether more fuel is available.
    fn do_work<P, YFn>(
        &mut self,
        cfg: &PersistConfig,
        work: &mut usize,
        name: &str,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut OutputHandleCore<
            '_,
            (mz_repr::Timestamp, Subtime),
            ConsolidatingContainerBuilder<
                Vec<(
                    Result<Row, DataflowError>,
                    (mz_repr::Timestamp, Subtime),
                    Diff,
                )>,
            >,
            P,
        >,
    ) -> bool
    where
        P: Push<
            Message<
                (mz_repr::Timestamp, Subtime),
                Vec<(
                    Result<Row, DataflowError>,
                    (mz_repr::Timestamp, Subtime),
                    Diff,
                )>,
            >,
        >,
        YFn: Fn(Instant, usize) -> bool,
    {
        let mut session = output.session_with_builder(&self.capability);
        let fetched_part = self.part.part_mut(cfg);
        let is_filter_pushdown_audit = fetched_part.is_filter_pushdown_audit();
        let mut row_buf = None;
        while let Some(((key, val), time, diff)) =
            fetched_part.next_with_storage(&mut row_buf, &mut None)
        {
            if until.less_equal(&time) {
                continue;
            }
            match (key, val) {
                (Ok(SourceData(Ok(row))), Ok(())) => {
                    if let Some(mfp) = map_filter_project {
                        // We originally accounted work as the number of outputs, to give downstream
                        // operators a chance to reduce down anything we've emitted. This mfp call
                        // might have a restrictive filter, which would have been counted as no
                        // work. However, in practice, we've been decode_and_mfp be a source of
                        // interactivity loss during rehydration, so we now also count each mfp
                        // evaluation against our fuel.
                        *work += 1;
                        let arena = mz_repr::RowArena::new();
                        let mut datums_local = datum_vec.borrow_with(&row);
                        for result in mfp.evaluate(
                            &mut datums_local,
                            &arena,
                            time,
                            diff.into(),
                            |time| !until.less_equal(time),
                            row_builder,
                        ) {
                            // Earlier we decided this Part doesn't need to be fetched, but to
                            // audit our logic we fetched it any way. If the MFP returned data it
                            // means our earlier decision to not fetch this part was incorrect.
                            if let Some(_stats) = &is_filter_pushdown_audit {
                                // NB: The tag added by this scope is used for alerting. The panic
                                // message may be changed arbitrarily, but the tag key and val must
                                // stay the same.
                                sentry::with_scope(
                                    |scope| {
                                        scope
                                            .set_tag("alert_id", "persist_pushdown_audit_violation")
                                    },
                                    || {
                                        // TODO: include more (redacted) information here.
                                        panic!(
                                            "persist filter pushdown correctness violation! {}",
                                            name
                                        );
                                    },
                                );
                            }
                            match result {
                                Ok((row, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        let mut emit_time = *self.capability.time();
                                        emit_time.0 = time;
                                        session.give((Ok(row), emit_time, diff));
                                        *work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        let mut emit_time = *self.capability.time();
                                        emit_time.0 = time;
                                        session.give((Err(err), emit_time, diff));
                                        *work += 1;
                                    }
                                }
                            }
                        }
                        // At the moment, this is the only case where we can re-use the allocs for
                        // the `SourceData`/`Row` we decoded. This could be improved if this timely
                        // operator used a different container than `Vec<Row>`.
                        drop(datums_local);
                        row_buf.replace(SourceData(Ok(row)));
                    } else {
                        let mut emit_time = *self.capability.time();
                        emit_time.0 = time;
                        // Clone row so we retain our row allocation.
                        session.give((Ok(row.clone()), emit_time, diff.into()));
                        row_buf.replace(SourceData(Ok(row)));
                        *work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    let mut emit_time = *self.capability.time();
                    emit_time.0 = time;
                    session.give((Err(err), emit_time, diff.into()));
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

/// A trait representing a type that can be used in `backpressure`.
pub trait Backpressureable: Clone + 'static {
    /// Return the weight of the object, in bytes.
    fn byte_size(&self) -> usize;
}

impl<T: Clone + 'static> Backpressureable for (usize, ExchangeableBatchPart<T>) {
    fn byte_size(&self) -> usize {
        self.1.encoded_size_bytes()
    }
}

/// Flow control configuration.
#[derive(Debug)]
pub struct FlowControl<G: Scope> {
    /// Stream providing in-flight frontier updates.
    ///
    /// As implied by its type, this stream never emits data, only progress updates.
    ///
    /// TODO: Replace `Infallible` with `!` once the latter is stabilized.
    pub progress_stream: Stream<G, Infallible>,
    /// Maximum number of in-flight bytes.
    pub max_inflight_bytes: usize,
    /// The minimum range of timestamps (be they granular or not) that must be emitted,
    /// ignoring `max_inflight_bytes` to ensure forward progress is made.
    pub summary: <G::Timestamp as TimelyTimestamp>::Summary,

    /// Optional metrics for the `backpressure` operator to keep up-to-date.
    pub metrics: Option<BackpressureMetrics>,
}

/// Apply flow control to the `data` input, based on the given `FlowControl`.
///
/// The `FlowControl` should have a `progress_stream` that is the pristine, unaltered
/// frontier of the downstream operator we want to backpressure from, a `max_inflight_bytes`,
/// and a `summary`. Note that the `data` input expects all the second part of the tuple
/// timestamp to be 0, and all data to be on the `chosen_worker` worker.
///
/// The `summary` represents the _minimum_ range of timestamps that needs to be emitted before
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
    probe: Option<UnboundedSender<(Antichain<(T, Subtime)>, usize, usize)>>,
) -> (Stream<G, O>, PressOnDropButton)
where
    T: TimelyTimestamp + Lattice + Codec64 + TotalOrder,
    G: Scope<Timestamp = (T, Subtime)>,
    O: Backpressureable + std::fmt::Debug,
{
    let worker_index = scope.index();

    let (flow_control_stream, flow_control_max_bytes, metrics) = (
        flow_control.progress_stream,
        flow_control.max_inflight_bytes,
        flow_control.metrics,
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
    let (data_output, data_stream) = builder.new_output();

    let mut data_input = builder.new_disconnected_input(data, Pipeline);
    let mut flow_control_input = builder.new_disconnected_input(&summaried_flow, Pipeline);

    // Helper method used to synthesize current and next frontier for ordered times.
    fn synthesize_frontiers<T: PartialOrder + Clone>(
        mut frontier: Antichain<(T, Subtime)>,
        mut time: (T, Subtime),
        part_number: &mut u64,
    ) -> (
        (T, Subtime),
        Antichain<(T, Subtime)>,
        Antichain<(T, Subtime)>,
    ) {
        let mut next_frontier = frontier.clone();
        time.1 = Subtime(*part_number);
        frontier.insert(time.clone());
        *part_number += 1;
        let mut next_time = time.clone();
        next_time.1 = Subtime(*part_number);
        next_frontier.insert(next_time);
        (time, frontier, next_frontier)
    }

    // _Refine_ the data stream by amending the second input with the part number. This also
    // ensures that we order the parts by time.
    let data_input = async_stream::stream!({
        let mut part_number = 0;
        let mut parts: Vec<((T, Subtime), O)> = Vec::new();
        loop {
            match data_input.next().await {
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
                Some(Event::Data(time, data)) => {
                    for d in data {
                        parts.push((time.clone(), d));
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

                let byte_size = part.byte_size();
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
                    inflight_parts.push((emission_ts, byte_size));
                }

                // Emit the data at the given time, and update the frontier and capabilities
                // to just beyond the part.
                data_output.give(&cap_set.delayed(&time), part);

                if let Some(metrics) = &metrics {
                    metrics.emitted_bytes.inc_by(u64::cast_from(byte_size))
                }

                output_frontier = next_frontier;
                cap_set.downgrade(output_frontier.iter())
            } else {
                if let Some(metrics) = &metrics {
                    metrics
                        .last_backpressured_bytes
                        .set(u64::cast_from(inflight_bytes))
                }
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
                flow_control_frontier.clone_from(&new_flow_control_frontier);

                // Retire parts that are processed downstream.
                let retired_parts = inflight_parts
                    .drain_filter_swapping(|(ts, _size)| !flow_control_frontier.less_equal(ts));
                let (retired_size, retired_count): (usize, usize) = retired_parts
                    .fold((0, 0), |(accum_size, accum_count), (_ts, size)| {
                        (accum_size + size, accum_count + 1)
                    });
                trace!(
                    "returning {} parts with {} bytes, frontier: {:?}",
                    retired_count, retired_size, flow_control_frontier,
                );

                if let Some(metrics) = &metrics {
                    metrics.retired_bytes.inc_by(u64::cast_from(retired_size))
                }

                // Optionally emit some information for tests to examine.
                if let Some(probe) = probe.as_ref() {
                    let _ = probe.send((new_flow_control_frontier, parts_count, retired_count));
                }
            }
        }
    });
    (data_stream, shutdown_button.press_on_drop())
}

#[cfg(test)]
mod tests {
    use timely::container::CapacityContainerBuilder;
    use timely::dataflow::operators::{Enter, Probe};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;

    use super::*;

    #[mz_ore::test]
    fn test_backpressure_non_granular() {
        use Step::*;
        backpressure_runner(
            vec![(50, Part(101)), (50, Part(102)), (100, Part(1))],
            100,
            (1, Subtime(0)),
            vec![
                // Assert we backpressure only after we have emitted
                // the entire timestamp.
                AssertOutputFrontier((50, Subtime(2))),
                AssertBackpressured {
                    frontier: (1, Subtime(0)),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (51, Subtime(0)),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                ProcessXParts(2),
                AssertBackpressured {
                    frontier: (101, Subtime(0)),
                    inflight_parts: 2,
                    retired_parts: 2,
                },
                // Assert we make later progress once processing
                // the parts.
                AssertOutputFrontier((100, Subtime(3))),
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
            (1, Subtime(0)),
            vec![
                // Assert we backpressure only after we emitted enough bytes
                AssertOutputFrontier((51, Subtime(3))),
                AssertBackpressured {
                    frontier: (1, Subtime(0)),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                ProcessXParts(3),
                AssertBackpressured {
                    frontier: (52, Subtime(0)),
                    inflight_parts: 3,
                    retired_parts: 2,
                },
                AssertBackpressured {
                    frontier: (53, Subtime(0)),
                    inflight_parts: 1,
                    retired_parts: 1,
                },
                // Assert we make later progress once processing
                // the parts.
                AssertOutputFrontier((52, Subtime(4))),
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
            (1, Subtime(0)),
            vec![
                AssertOutputFrontier((51, Subtime(3))),
                // Assert we backpressure after we have emitted enough bytes.
                // We assert twice here because we get updates as
                // `flow_control` progresses from `(0, 0)`->`(0, 1)`-> a real frontier.
                AssertBackpressured {
                    frontier: (1, Subtime(0)),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (51, Subtime(0)),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                ProcessXParts(1),
                // Our output frontier doesn't move, as the downstream frontier hasn't moved past
                // 50.
                AssertOutputFrontier((51, Subtime(3))),
                // After we process all of `50`, we can start emitting data at `52`, but only until
                // we exhaust out budget. We don't need to emit all of `52` because we have emitted
                // all of `51`.
                ProcessXParts(1),
                AssertOutputFrontier((52, Subtime(4))),
                AssertBackpressured {
                    frontier: (52, Subtime(0)),
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
                    frontier: (53, Subtime(0)),
                    inflight_parts: 2,
                    retired_parts: 1,
                },
                // After we process all of `52`, we can continue to the next time.
                ProcessXParts(5),
                AssertBackpressured {
                    frontier: (101, Subtime(0)),
                    inflight_parts: 5,
                    retired_parts: 5,
                },
                AssertOutputFrontier((100, Subtime(9))),
            ],
            true,
        );
    }

    #[mz_ore::test]
    fn test_backpressure_granular() {
        use Step::*;
        backpressure_runner(
            vec![(50, Part(101)), (50, Part(101))],
            100,
            (0, Subtime(1)),
            vec![
                // Advance our frontier to outputting a single part.
                AssertOutputFrontier((50, Subtime(1))),
                // Receive backpressure updates until our frontier is up-to-date but
                // not beyond the parts (while considering the summary).
                AssertBackpressured {
                    frontier: (0, Subtime(1)),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (50, Subtime(1)),
                    inflight_parts: 1,
                    retired_parts: 0,
                },
                // Process that part.
                ProcessXParts(1),
                // Assert that we clear the backpressure status
                AssertBackpressured {
                    frontier: (50, Subtime(2)),
                    inflight_parts: 1,
                    retired_parts: 1,
                },
                // Ensure we make progress to the next part.
                AssertOutputFrontier((50, Subtime(2))),
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
            (0, Subtime(1)),
            vec![
                // we can emit 3 parts before we hit the backpressure limit
                AssertOutputFrontier((51, Subtime(3))),
                AssertBackpressured {
                    frontier: (0, Subtime(1)),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                AssertBackpressured {
                    frontier: (50, Subtime(1)),
                    inflight_parts: 3,
                    retired_parts: 0,
                },
                // Retire the single part.
                ProcessXParts(1),
                AssertBackpressured {
                    frontier: (50, Subtime(2)),
                    inflight_parts: 3,
                    retired_parts: 1,
                },
                // Ensure we make progress, and then
                // can retire the next 2 parts.
                AssertOutputFrontier((52, Subtime(4))),
                ProcessXParts(2),
                AssertBackpressured {
                    frontier: (52, Subtime(4)),
                    inflight_parts: 3,
                    retired_parts: 2,
                },
            ],
            false,
        );
    }

    type Time = (u64, Subtime);
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
                    ) = scope.scoped::<(u64, Subtime), _, _>("hybrid", |scope| {
                        let (input, finalizer_tx) =
                            iterator_operator(scope.clone(), input.into_iter());

                        let (flow_control, granular_feedback_handle) = if non_granular_consumer {
                            (
                                FlowControl {
                                    progress_stream: non_granular_feedback.unwrap().enter(scope),
                                    max_inflight_bytes,
                                    summary,
                                    metrics: None
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
                                    metrics: None,
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
        G: Scope<Timestamp = (u64, Subtime)>,
        I: Iterator<Item = (u64, Part)> + 'static,
    >(
        scope: G,
        mut input: I,
    ) -> (Stream<G, Part>, oneshot::Sender<()>) {
        let (finalizer_tx, finalizer_rx) = oneshot::channel();
        let mut iterator = AsyncOperatorBuilder::new("iterator".to_string(), scope);
        let (output_handle, output) = iterator.new_output::<CapacityContainerBuilder<Vec<Part>>>();

        iterator.build(|mut caps| async move {
            let mut capability = Some(caps.pop().unwrap());
            let mut last = None;
            while let Some(element) = input.next() {
                let time = element.0.clone();
                let part = element.1;
                last = Some((time, Subtime(0)));
                output_handle.give(&capability.as_ref().unwrap().delayed(&last.unwrap()), part);
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
        feedback: timely::dataflow::operators::feedback::Handle<G, Vec<std::convert::Infallible>>,
    ) -> UnboundedSender<()> {
        let (tx, mut rx) = unbounded_channel::<()>();
        let mut consumer = AsyncOperatorBuilder::new("consumer".to_string(), scope);
        let (output_handle, output) =
            consumer.new_output::<CapacityContainerBuilder<Vec<std::convert::Infallible>>>();
        let mut input = consumer.new_input_for(input, Pipeline, &output_handle);

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
