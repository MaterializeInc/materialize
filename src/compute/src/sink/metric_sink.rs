// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Render arm for `MetricSinkConnection`.
//!
//! A metric sink funnels every row of its source collection to one worker per process, folds
//! it into a [`SinkState`], and exposes that state to the process's Prometheus registry through
//! a [`SinkCollector`]. `SinkState` is shared between the timely operator (the sole writer) and
//! `SinkCollector::collect` (the reader, invoked from whatever thread scrapes the registry) via
//! `Arc<Mutex<_>>`. Both sides only ever hold the lock across a short, synchronous section: the
//! operator has no `await` points (it is a synchronous `builder_rc` operator), and the collector
//! only clones out the data it needs to build `MetricFamily` protos before releasing the lock.
//!
//! The planner (`optimize::metric_sink::shape_metric_sink_source`) does the pure row-wise
//! shaping: it coalesces `labels`/`help` to their identity element and computes the `metric_kind`
//! and `name_valid` columns `extract_row` reads below, so this module no longer parses
//! `metric_type` strings or validates `metric_name` itself. Dedup, collision detection, and
//! family-conflict counting stay here because they need the cross-row state of the fold, which a
//! per-row `Map` in MIR can't express.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use differential_dataflow::{Hashable, VecCollection};
use mz_compute_types::sinks::{ComputeSinkDesc, MetricSinkConnection};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_repr::{ColumnName, Datum, DatumVec, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::probe::{Handle, ProbeNotify};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Counter as ProtoCounter, Gauge as ProtoGauge, LabelPair, Metric as ProtoMetric, MetricFamily,
    MetricType,
};
use prometheus::{Gauge, Opts};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::Antichain;

use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;

impl<'scope> SinkRender<'scope> for MetricSinkConnection {
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
        output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        let cols = ColumnIndices::resolve(&sink.from_desc);

        let scope = sinked_collection.scope();
        let worker_id = scope.index();
        // The registry is process-local, so every row must land on the same worker or the
        // series would be split across processes. Which worker is chosen doesn't matter, only
        // that all workers agree, so hash the sink's own id.
        let active_worker_id = usize::cast_from(sink_id.hashed()) % scope.peers();

        let ok_stream = sinked_collection
            .inner
            .probe_notify_with(vec![output_probe.clone()]);
        let err_stream = err_collection.inner;

        let state = Arc::new(Mutex::new(SinkState::default()));

        // Only the active worker registers a collector. The `MetricsRegistry` is shared by every
        // worker in the process, so registering from more than one worker would either duplicate
        // the collector or race on its `Desc` ids.
        let drop_handle = (worker_id == active_worker_id).then(|| {
            let collector = SinkCollector::new(sink_id, Arc::clone(&state));
            compute_state
                .metrics_registry
                .register_collector_with_dropper(collector)
        });

        let mut op = OperatorBuilder::new(format!("MetricSink({sink_id})"), scope);
        let mut ok_input = op.new_input(
            ok_stream,
            Exchange::new(move |_: &(Row, Timestamp, Diff)| u64::cast_from(active_worker_id)),
        );
        let mut err_input = op.new_input(
            err_stream,
            Exchange::new(move |_: &(DataflowErrorSer, Timestamp, Diff)| {
                u64::cast_from(active_worker_id)
            }),
        );

        op.build(move |_capabilities| {
            // Recycled across activations: unpacking a row into `Datum`s otherwise allocates a
            // fresh `Vec` per row on this hot path.
            let mut datum_vec = DatumVec::new();
            move |frontiers| {
                if worker_id != active_worker_id {
                    // Drain so the operator isn't rescheduled forever. There is no state to
                    // fold into on this worker.
                    ok_input.for_each(|_, _| {});
                    err_input.for_each(|_, _| {});
                    return;
                }

                let mut st = state.lock().expect("sink state mutex poisoned");

                // Buffer every incoming update under its timestamp. The input is not
                // consolidated and timely does not guarantee that all diffs at a timestamp arrive
                // in one activation, so nothing is folded into `working` until the timestamp is
                // closed (see `integrate`).
                ok_input.for_each(|_, data| {
                    for (row, time, diff) in data.drain(..) {
                        let datums = datum_vec.borrow_with(&row);
                        let (name, metric_kind, name_valid, labels, value, help) =
                            extract_row(&cols, &datums);
                        st.stage_ok(
                            name,
                            metric_kind,
                            name_valid,
                            &labels,
                            value,
                            help,
                            time,
                            diff.into_inner(),
                        );
                    }
                });
                err_input.for_each(|_, data| {
                    for (_err, time, diff) in data.drain(..) {
                        st.stage_err(time, diff.into_inner());
                    }
                });

                // Combined ok+err input frontier. A timestamp is closed once neither input can
                // still produce data at it, so folding a closed time observes all of its diffs.
                let mut frontier = Antichain::new();
                for f in frontiers {
                    frontier.extend(f.frontier().iter().copied());
                }

                st.integrate(&frontier);
                st.frontier_ms = frontier
                    .as_option()
                    .map(|t| u64::from(*t))
                    .unwrap_or(u64::MAX);
                st.publish_if_healthy();
            }
        });

        Some(Rc::new(drop_handle))
    }
}

/// Column indices resolved once from the sink's source relation.
///
/// The SQL planner guarantees (`validate_metric_sink_desc`) that the relation exposes
/// `metric_name`, `labels`, `value`, and `help` of the required type, and the shaping `Map` it
/// adds (`shape_metric_sink_source`) guarantees `metric_kind` and `name_valid`. `metric_name` and
/// `value` may still be `Datum::Null`. The rest are non-null by construction. Column position
/// within the row is unconstrained.
struct ColumnIndices {
    metric_name: usize,
    labels: usize,
    value: usize,
    help: usize,
    metric_kind: usize,
    name_valid: usize,
}

impl ColumnIndices {
    fn resolve(desc: &RelationDesc) -> Self {
        let idx = |name: &str| {
            desc.get_by_name(&ColumnName::from(name))
                .expect("column existence validated by the SQL planner")
                .0
        };
        ColumnIndices {
            metric_name: idx("metric_name"),
            labels: idx("labels"),
            value: idx("value"),
            help: idx("help"),
            metric_kind: idx("metric_kind"),
            name_valid: idx("name_valid"),
        }
    }
}

/// Extracts `(metric_name, metric_kind, name_valid, sorted labels, value, help)` from one shaped
/// source row.
///
/// The planner already did the row-wise shaping (see `optimize::metric_sink::
/// shape_metric_sink_source`): `labels`/`help` are coalesced to their identity element, and
/// `metric_kind`/`name_valid` classify the row without this needing to parse `metric_type` or
/// validate `metric_name` itself. `metric_name` may still be `Datum::Null`, which takes the same
/// skip path as any other invalid name because `name_valid` is computed from it upstream and is
/// `false` (or, defensively, `Datum::Null`, treated the same as `false`) whenever `metric_name`
/// is. `value` has no identity element (there is no numeric value that means "absent"), so it
/// stays `Option<f64>`. A null value is a real row identity, just one that never contributes to a
/// series' published value. Strings borrow from `datums`, so the caller must own what it needs
/// (see `SinkState::stage_ok`) before the row backing `datums` is dropped.
fn extract_row<'a>(
    cols: &ColumnIndices,
    datums: &[Datum<'a>],
) -> (
    &'a str,
    Option<MetricKind>,
    bool,
    Vec<(&'a str, &'a str)>,
    Option<f64>,
    &'a str,
) {
    let metric_name = match datums[cols.metric_name] {
        Datum::Null => "",
        d => d.unwrap_str(),
    };
    let metric_kind = MetricKind::from_datum(datums[cols.metric_kind]);
    let name_valid = matches!(datums[cols.name_valid], Datum::True);
    let mut labels: Vec<(&str, &str)> = datums[cols.labels]
        .unwrap_map()
        .iter()
        .map(|(k, v)| (k, v.unwrap_str()))
        .collect();
    labels.sort();
    let value = match datums[cols.value] {
        Datum::Null => None,
        d => Some(d.unwrap_float64()),
    };
    let help = datums[cols.help].unwrap_str();
    (metric_name, metric_kind, name_valid, labels, value, help)
}

/// Full identity of one source row: metric name, sorted labels, value, metric kind, name
/// validity, and help.
///
/// The value is stored as `Option<u64>`, the `f64::to_bits` pattern of a real value or `None` for
/// a null `value` column. A null value is its own distinct row identity, not a stand-in for any
/// particular number, so it is kept apart from every `Some(_)` identity rather than coerced to
/// one. `Option<u64>` stays totally ordered (derived `Ord` places `None` before every `Some`), so
/// the tuple is still usable as a map key. Recover a real value with `f64::from_bits`. The name
/// and labels lead the tuple so that a `BTreeMap<RowKey, _>` keeps all rows of one `(metric_name,
/// labels)` series adjacent.
///
/// `metric_kind` is the planner's classification (`None` for any unsupported `metric_type`), not
/// the raw string: two source rows that differ only in *which* unsupported type they carry (e.g.
/// `"histogram"` vs. `"summary"`) now share one identity instead of two. That only affects the
/// granularity of the `skipped` count for rows that are never published either way.
type RowKey = (
    String,
    Vec<(String, String)>,
    Option<u64>,
    Option<MetricKind>,
    bool,
    String,
);

/// Key into [`SinkState::published`]: a metric name paired with its sorted label vector.
type PublishedKey = (String, Vec<(String, String)>);
/// Value in [`SinkState::published`]: the series' value, kind, and help string.
type PublishedValue = (f64, MetricKind, String);

/// Working and published metric state for one metric sink.
///
/// Because the input is not consolidated and a timestamp's diffs may span several operator
/// activations, incoming updates are buffered by timestamp in `pending_ok`/`pending_err` and only
/// folded into `working` once the input frontier has closed that timestamp. `working` accumulates
/// a signed multiplicity per full row identity, and a row is live iff its accumulated diff is
/// positive. `published` is what the collector exposes and is rebuilt from the live set of
/// `working` only while `errors` is zero, so an errored dataflow freezes the last healthy values.
#[derive(Default)]
struct SinkState {
    /// Ok-collection updates awaiting their timestamp closing, accumulated per identity.
    pending_ok: BTreeMap<Timestamp, BTreeMap<RowKey, i64>>,
    /// Err-collection diffs awaiting their timestamp closing.
    pending_err: BTreeMap<Timestamp, i64>,
    /// Accumulated multiplicity per row identity over all closed timestamps.
    working: BTreeMap<RowKey, i64>,
    published: BTreeMap<PublishedKey, PublishedValue>,
    /// Net count of live errors on the sink's input. Can rise and fall as errors are
    /// retracted. Publication is frozen while this is nonzero.
    errors: i64,
    frontier_ms: u64,
    skipped: u64,
    conflicts: u64,
    collisions: u64,
    /// Count of live `(metric_name, labels)` groups whose only live rows carry a null `value`,
    /// so the series is currently absent from `published` (a gap) rather than published as some
    /// number.
    null_values: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MetricKind {
    Gauge,
    Counter,
}

impl MetricKind {
    /// Recovers the classification the planner's `metric_kind` column already computed (`0` =
    /// gauge, `1` = counter). Anything else, including `Datum::Null` for an unsupported
    /// `metric_type`, is treated as unsupported: `shape_metric_sink_source` only ever emits
    /// those three values, so this is a defensive fallback, not an expected case.
    fn from_datum(d: Datum) -> Option<Self> {
        match d {
            Datum::Int32(0) => Some(MetricKind::Gauge),
            Datum::Int32(1) => Some(MetricKind::Counter),
            _ => None,
        }
    }

    fn proto_type(self) -> MetricType {
        match self {
            MetricKind::Gauge => MetricType::GAUGE,
            MetricKind::Counter => MetricType::COUNTER,
        }
    }
}

/// Matches Prometheus's label name grammar: `[a-zA-Z_][a-zA-Z0-9_]*`.
///
/// Unlike the metric-name grammar (`name_valid`, computed in MIR), this stays in Rust: it
/// applies to every key of the `labels` map, an unbounded per-row collection that doesn't fit a
/// scalar `Map` expression. See the TODO on `shape_metric_sink_source` for the full move this is
/// part of.
fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

impl SinkState {
    /// Buffers one ok-collection update under its timestamp.
    ///
    /// `diff` follows differential dataflow sign conventions and is accumulated per full row
    /// identity, so an un-consolidated input carrying the same identity twice sums rather than
    /// being mistaken for a second live row. Takes borrowed strings (see `extract_row`) and owns
    /// them only here, once, at the point the identity is committed to `pending_ok`.
    fn stage_ok(
        &mut self,
        metric_name: &str,
        metric_kind: Option<MetricKind>,
        name_valid: bool,
        labels: &[(&str, &str)],
        value: Option<f64>,
        help: &str,
        time: Timestamp,
        diff: i64,
    ) {
        let key = (
            metric_name.to_string(),
            labels
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value.map(f64::to_bits),
            metric_kind,
            name_valid,
            help.to_string(),
        );
        *self
            .pending_ok
            .entry(time)
            .or_default()
            .entry(key)
            .or_default() += diff;
    }

    /// Buffers one err-collection diff under its timestamp.
    fn stage_err(&mut self, time: Timestamp, diff: i64) {
        *self.pending_err.entry(time).or_default() += diff;
    }

    /// Folds every buffered timestamp the `frontier` has closed into `working` and `errors`.
    ///
    /// A timestamp is closed once the combined ok+err frontier can no longer produce data at it,
    /// which guarantees all of its diffs are already buffered. Accumulated entries that reach a
    /// multiplicity of zero are dropped. `skipped` is recomputed here from the live set, so it
    /// counts the input rows currently dropped for an unsupported type or invalid name.
    fn integrate(&mut self, frontier: &Antichain<Timestamp>) {
        let closed_ok: Vec<Timestamp> = self
            .pending_ok
            .keys()
            .filter(|t| !frontier.less_equal(t))
            .copied()
            .collect();
        for time in closed_ok {
            let rows = self.pending_ok.remove(&time).expect("key from keys()");
            for (key, diff) in rows {
                *self.working.entry(key).or_default() += diff;
            }
        }

        let closed_err: Vec<Timestamp> = self
            .pending_err
            .keys()
            .filter(|t| !frontier.less_equal(t))
            .copied()
            .collect();
        for time in closed_err {
            self.errors += self.pending_err.remove(&time).expect("key from keys()");
        }

        self.working.retain(|_, acc| *acc != 0);
        self.skipped = count_skipped(&self.working);
    }

    /// Rebuilds `published` and the `collisions`/`conflicts` counts from the live set of
    /// `working`, but only while the dataflow is free of live errors. While `errors > 0`,
    /// publication stays frozen at the last healthy snapshot. `working` keeps integrating closed
    /// timestamps in the meantime, so the next healthy publish reflects everything that happened
    /// during the freeze.
    fn publish_if_healthy(&mut self) {
        if self.errors == 0 {
            let (published, collisions, null_values) = rebuild_published(&self.working);
            self.published = published;
            self.collisions = collisions;
            self.null_values = null_values;
            self.conflicts = count_conflicts(&self.published);
        }
    }
}

/// Counts live working rows dropped for an unsupported `metric_type` or an invalid Prometheus
/// metric or label name.
fn count_skipped(working: &BTreeMap<RowKey, i64>) -> u64 {
    let mut skipped = 0u64;
    for ((_name, labels, _bits, metric_kind, name_valid, _help), acc) in working {
        if *acc <= 0 {
            continue;
        }
        let unsupported = metric_kind.is_none();
        let invalid = !name_valid || !labels.iter().all(|(k, _)| is_valid_label_name(k));
        if unsupported || invalid {
            skipped += 1;
        }
    }
    skipped
}

/// Collapses the live, representable rows of `working` into one published entry per
/// `(metric_name, labels)` series and counts colliding and null-suppressed series.
///
/// A series collides when more than one distinct live *non-null* value exists for its
/// `(metric_name, labels)`. That is a genuine conflict of two source rows, unlike an ordinary
/// value update whose old row is retracted and new row inserted within the same closed
/// timestamp, which leaves a single live value. When a series collides the winner is chosen
/// deterministically as the row with the numerically smallest value, breaking ties by metric type
/// then help. A null `value` carries no number to compare or publish: a series whose live rows
/// are all null-valued is absent from `published` (a gap) and counted in the returned
/// `null_values` instead of `collisions`. A series with at least one live non-null value publishes
/// normally and is not counted in `null_values`, even if null-valued rows are also live for it.
fn rebuild_published(
    working: &BTreeMap<RowKey, i64>,
) -> (BTreeMap<PublishedKey, PublishedValue>, u64, u64) {
    // `working` orders rows by `(name, labels, ...)`, so all rows of one series are adjacent.
    let mut grouped: BTreeMap<PublishedKey, Vec<(Option<f64>, MetricKind, String)>> =
        BTreeMap::new();
    for ((name, labels, bits, metric_kind, name_valid, help), acc) in working {
        if *acc <= 0 {
            continue;
        }
        let Some(kind) = metric_kind else {
            continue;
        };
        if !name_valid || !labels.iter().all(|(k, _)| is_valid_label_name(k)) {
            continue;
        }
        grouped
            .entry((name.clone(), labels.clone()))
            .or_default()
            .push((bits.map(f64::from_bits), *kind, help.clone()));
    }

    let mut published = BTreeMap::new();
    let mut collisions = 0u64;
    let mut null_values = 0u64;
    for (key, candidates) in grouped {
        let mut non_null: Vec<PublishedValue> = candidates
            .into_iter()
            .filter_map(|(value, kind, help)| value.map(|v| (v, kind, help)))
            .collect();
        if non_null.is_empty() {
            null_values += 1;
            continue;
        }
        let mut distinct: Vec<u64> = non_null.iter().map(|(v, _, _)| v.to_bits()).collect();
        distinct.sort_unstable();
        distinct.dedup();
        if distinct.len() > 1 {
            collisions += 1;
        }
        non_null.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        let winner = non_null
            .into_iter()
            .next()
            .expect("checked non-empty above");
        published.insert(key, winner);
    }
    (published, collisions, null_values)
}

/// Counts published series whose own `metric_type`/`help` disagree with their family's winning
/// type/help. See [`build_families`] for how the winner is chosen.
fn count_conflicts(published: &BTreeMap<PublishedKey, PublishedValue>) -> u64 {
    let mut conflicts = 0u64;
    let mut winner: Option<(&str, MetricKind, &str)> = None;
    for ((name, _labels), (_value, kind, help)) in published {
        winner = match winner {
            Some((n, k, h)) if n == name.as_str() => {
                if k != *kind || h != help.as_str() {
                    conflicts += 1;
                }
                Some((n, k, h))
            }
            _ => Some((name.as_str(), *kind, help.as_str())),
        };
    }
    conflicts
}

/// Groups `published` by metric name into one `MetricFamily` per name, since Prometheus requires
/// a single type and help string per family. Within a group, the entry with the
/// lexicographically smallest label vector wins the family's type and help; `BTreeMap`'s
/// `(name, labels)` key ordering already sorts each group that way, so the first entry seen for a
/// given name is that winner.
///
/// `MetricFamily.type_` and `Metric.{gauge,counter}` are `protobuf`-crate wrapper types
/// (`EnumOrUnknown`/`MessageField`). The workspace bans depending on `protobuf` directly outside
/// a couple of wrapper crates (see `deny.toml`), so this builds them through the blanket
/// `From<MetricType>`/`From<Option<_>>` conversions rather than naming those wrapper types.
fn build_families(published: &BTreeMap<PublishedKey, PublishedValue>) -> Vec<MetricFamily> {
    let mut families = Vec::new();
    let mut group_name: Option<&str> = None;
    let mut family: Option<MetricFamily> = None;
    let mut family_kind = MetricKind::Gauge;

    for ((name, labels), (value, kind, help)) in published {
        if group_name != Some(name.as_str()) {
            if let Some(f) = family.take() {
                families.push(f);
            }
            let mut mf = MetricFamily::new();
            mf.name = Some(name.clone());
            mf.help = Some(help.clone());
            mf.type_ = Some(kind.proto_type().into());
            family = Some(mf);
            family_kind = *kind;
            group_name = Some(name.as_str());
        }

        let mut metric = ProtoMetric::new();
        metric.label = labels
            .iter()
            .map(|(k, v)| {
                let mut lp = LabelPair::new();
                lp.name = Some(k.clone());
                lp.value = Some(v.clone());
                lp
            })
            .collect();
        match family_kind {
            MetricKind::Gauge => {
                let mut g = ProtoGauge::new();
                g.value = Some(*value);
                metric.gauge = Some(g).into();
            }
            MetricKind::Counter => {
                let mut c = ProtoCounter::new();
                c.value = Some(*value);
                metric.counter = Some(c).into();
            }
        }
        family
            .as_mut()
            .expect("initialized above for the first entry of every group")
            .metric
            .push(metric);
    }
    if let Some(f) = family.take() {
        families.push(f);
    }
    families
}

/// A `prometheus::core::Collector` that exposes a metric sink's [`SinkState`].
///
/// The companion gauges (`mz_metric_sink_*`) are declared statically, each carrying a `sink`
/// const label so that per-sink series get distinct `Desc` ids on registration. The user-defined
/// series are entirely dynamic: their names come from the sink's source query, so they are built
/// directly as [`MetricFamily`] protos in `collect` and are not declared via `desc`. Prometheus's
/// registry only uses `desc` for registration-time collision detection, not to validate the
/// output of `collect`, so this is safe.
#[derive(Clone)]
struct SinkCollector {
    state: Arc<Mutex<SinkState>>,
    frontier_gauge: Gauge,
    errors_gauge: Gauge,
    skipped_gauge: Gauge,
    conflicts_gauge: Gauge,
    collisions_gauge: Gauge,
    null_values_gauge: Gauge,
}

impl SinkCollector {
    fn new(sink_id: GlobalId, state: Arc<Mutex<SinkState>>) -> Self {
        let gauge = |name: &str, help: &str| {
            Gauge::with_opts(Opts::new(name, help).const_label("sink", sink_id.to_string()))
                .expect("static metric sink companion gauge options are valid")
        };
        SinkCollector {
            state,
            frontier_gauge: gauge(
                "mz_metric_sink_frontier_ms",
                "The metric sink's input frontier, in milliseconds since the epoch.",
            ),
            errors_gauge: gauge(
                "mz_metric_sink_errors",
                "The number of live errors on the metric sink's input.",
            ),
            skipped_gauge: gauge(
                "mz_metric_sink_skipped",
                "The number of input rows skipped for an unsupported metric type or an invalid name.",
            ),
            conflicts_gauge: gauge(
                "mz_metric_sink_conflicts",
                "The number of published series whose type or help disagree with their family's chosen type or help.",
            ),
            collisions_gauge: gauge(
                "mz_metric_sink_collisions",
                "The number of series with more than one distinct live value for the same metric name and labels.",
            ),
            null_values_gauge: gauge(
                "mz_metric_sink_null_values",
                "The number of series currently suppressed because their value is null.",
            ),
        }
    }
}

impl Collector for SinkCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = Vec::with_capacity(6);
        descs.extend(self.frontier_gauge.desc());
        descs.extend(self.errors_gauge.desc());
        descs.extend(self.skipped_gauge.desc());
        descs.extend(self.conflicts_gauge.desc());
        descs.extend(self.collisions_gauge.desc());
        descs.extend(self.null_values_gauge.desc());
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = {
            let state = self.state.lock().expect("sink state mutex poisoned");
            self.frontier_gauge.set(f64::cast_lossy(state.frontier_ms));
            self.errors_gauge.set(f64::cast_lossy(state.errors));
            self.skipped_gauge.set(f64::cast_lossy(state.skipped));
            self.conflicts_gauge.set(f64::cast_lossy(state.conflicts));
            self.collisions_gauge.set(f64::cast_lossy(state.collisions));
            self.null_values_gauge
                .set(f64::cast_lossy(state.null_values));
            build_families(&state.published)
        };

        families.extend(self.frontier_gauge.collect());
        families.extend(self.errors_gauge.collect());
        families.extend(self.skipped_gauge.collect());
        families.extend(self.conflicts_gauge.collect());
        families.extend(self.collisions_gauge.collect());
        families.extend(self.null_values_gauge.collect());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frontier that has closed every timestamp strictly below `bound`.
    fn frontier(bound: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::from(bound))
    }

    fn label_a() -> Vec<(String, String)> {
        vec![("a".into(), "1".into())]
    }

    /// `label_a()`, borrowed: what `stage_ok` now takes (see `extract_row`).
    const LABEL_A: &[(&str, &str)] = &[("a", "1")];

    fn key_m() -> PublishedKey {
        ("m".into(), label_a())
    }

    /// Stages one gauge update for the `(m, {a:1})` series.
    fn stage_m(st: &mut SinkState, value: f64, time: u64, diff: i64) {
        st.stage_ok(
            "m",
            Some(MetricKind::Gauge),
            true,
            LABEL_A,
            Some(value),
            "h",
            Timestamp::from(time),
            diff,
        );
    }

    /// Stages one gauge update with a null `value` for the `(m, {a:1})` series.
    fn stage_m_null(st: &mut SinkState, time: u64, diff: i64) {
        st.stage_ok(
            "m",
            Some(MetricKind::Gauge),
            true,
            LABEL_A,
            None,
            "h",
            Timestamp::from(time),
            diff,
        );
    }

    #[mz_ore::test]
    fn fold_and_publish() {
        let mut st = SinkState::default();
        stage_m(&mut st, 2.0, 0, 1);
        st.integrate(&frontier(1));
        st.publish_if_healthy();
        assert_eq!(st.published.len(), 1);
        assert_eq!(st.published[&key_m()].0, 2.0);

        // Unsupported type (metric_kind = None) is skipped and counted.
        st.stage_ok("h1", None, true, &[], Some(1.0), "h", Timestamp::from(1), 1);
        st.integrate(&frontier(2));
        assert_eq!(st.skipped, 1);

        // Error freezes publication. The update at time 2 retracts the old value and inserts the
        // new one, both fold into `working` while frozen.
        st.errors = 1;
        stage_m(&mut st, 2.0, 2, -1);
        stage_m(&mut st, 9.0, 2, 1);
        st.integrate(&frontier(3));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 2.0);

        // Recovery republishes the integrated value.
        st.errors = 0;
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 9.0);
        assert_eq!(st.collisions, 0);
    }

    #[mz_ore::test]
    fn value_update_split_across_activations_no_collision() {
        let mut st = SinkState::default();
        // Establish the series at value 5.
        stage_m(&mut st, 5.0, 0, 1);
        st.integrate(&frontier(1));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 5.0);

        // A value update 5 -> 9 at time 1 arrives insert-first, split across two activations. The
        // timestamp stays open until both diffs are buffered.
        stage_m(&mut st, 9.0, 1, 1);
        st.integrate(&frontier(1));
        st.publish_if_healthy();
        assert_eq!(st.collisions, 0);
        stage_m(&mut st, 5.0, 1, -1);

        // Close the timestamp: the series is present at value 9 with no collision.
        st.integrate(&frontier(2));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 9.0);
        assert_eq!(st.collisions, 0);
    }

    #[mz_ore::test]
    fn duplicate_multiplicity_consolidates() {
        let mut st = SinkState::default();
        // The same identity at multiplicity 2 consolidates to a single live row.
        stage_m(&mut st, 5.0, 0, 1);
        stage_m(&mut st, 5.0, 0, 1);
        st.integrate(&frontier(1));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 5.0);
        assert_eq!(st.collisions, 0);

        // A second, distinct live value for the same series is a genuine collision.
        stage_m(&mut st, 7.0, 1, 1);
        st.integrate(&frontier(2));
        st.publish_if_healthy();
        assert_eq!(st.collisions, 1);
        // The smallest value wins deterministically.
        assert_eq!(st.published[&key_m()].0, 5.0);
    }

    #[mz_ore::test]
    fn no_publish_before_time_closed() {
        let mut st = SinkState::default();
        // An update at time 5 must not appear while the frontier still allows data at time 5.
        stage_m(&mut st, 2.0, 5, 1);
        st.integrate(&frontier(5));
        st.publish_if_healthy();
        assert!(st.published.is_empty());

        // Once the frontier advances past time 5, the update publishes.
        st.integrate(&frontier(6));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 2.0);
    }

    #[mz_ore::test]
    fn null_value_gaps_series() {
        let mut st = SinkState::default();
        // A null-valued row for (m,{a}) at a closed time: no series, counted in null_values.
        stage_m_null(&mut st, 1, 1);
        st.integrate(&frontier(2));
        st.publish_if_healthy();
        assert!(!st.published.contains_key(&key_m()));
        assert_eq!(st.null_values, 1);

        // A later non-null value republishes the series (gap closes) and clears the count, even
        // though the null-valued row is still live alongside it.
        stage_m(&mut st, 5.0, 3, 1);
        st.integrate(&frontier(4));
        st.publish_if_healthy();
        assert_eq!(st.published[&key_m()].0, 5.0);
        assert_eq!(st.null_values, 0);
    }

    #[mz_ore::test]
    fn null_labels_become_empty() {
        let mut st = SinkState::default();
        // An empty label vector (the shaped relation's `{}` for a source row with no labels)
        // keys and publishes correctly.
        st.stage_ok(
            "m",
            Some(MetricKind::Gauge),
            true,
            &[],
            Some(1.0),
            "h",
            Timestamp::from(1),
            1,
        );
        st.integrate(&frontier(2));
        st.publish_if_healthy();
        assert_eq!(st.published[&("m".into(), vec![])].0, 1.0);
    }

    #[mz_ore::test]
    fn extract_row_normalizes_null_datums() {
        use mz_repr::SqlScalarType;

        // Mirrors the shaped relation `shape_metric_sink_source` builds: `labels`/`help` are
        // non-null by construction, `metric_name`/`value` stay nullable, and `metric_kind`/
        // `name_valid` are the planner's computed classification columns.
        let desc = RelationDesc::builder()
            .with_column("metric_name", SqlScalarType::String.nullable(true))
            .with_column(
                "labels",
                SqlScalarType::Map {
                    value_type: Box::new(SqlScalarType::String),
                    custom_id: None,
                }
                .nullable(false),
            )
            .with_column("value", SqlScalarType::Float64.nullable(true))
            .with_column("help", SqlScalarType::String.nullable(false))
            .with_column("metric_kind", SqlScalarType::Int32.nullable(true))
            .with_column("name_valid", SqlScalarType::Bool.nullable(true))
            .finish();
        let cols = ColumnIndices::resolve(&desc);

        let mut row = Row::default();
        {
            let mut packer = row.packer();
            packer.push(Datum::Null); // metric_name
            packer.push_dict_with(|_| {}); // labels: always non-null by construction
            packer.push(Datum::Null); // value
            packer.push(Datum::String("")); // help: always non-null by construction
            packer.push(Datum::Null); // metric_kind: defensively treated as unsupported
            packer.push(Datum::Null); // name_valid: defensively treated as invalid
        }

        let datums: Vec<Datum> = row.iter().collect();
        let (name, metric_kind, name_valid, labels, value, help) = extract_row(&cols, &datums);
        assert_eq!(name, "");
        assert_eq!(metric_kind, None);
        assert!(!name_valid);
        assert_eq!(labels, Vec::<(&str, &str)>::new());
        assert_eq!(value, None);
        assert_eq!(help, "");
    }
}
