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

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use differential_dataflow::{Hashable, VecCollection};
use mz_compute_types::sinks::{ComputeSinkDesc, MetricSinkConnection};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_repr::{ColumnName, Diff, GlobalId, RelationDesc, Row, Timestamp};
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
            move |frontiers| {
                if worker_id != active_worker_id {
                    // Drain so the operator isn't rescheduled forever; there is no state to
                    // fold into on this worker.
                    ok_input.for_each(|_, _| {});
                    err_input.for_each(|_, _| {});
                    return;
                }

                let mut ok_updates = Vec::new();
                ok_input.for_each(|_, data| {
                    for (row, _time, diff) in data.drain(..) {
                        ok_updates.push((row, diff));
                    }
                });
                let mut err_diff_total: i64 = 0;
                err_input.for_each(|_, data| {
                    for (_err, _time, diff) in data.drain(..) {
                        err_diff_total += diff.into_inner();
                    }
                });

                let mut st = state.lock().expect("sink state mutex poisoned");

                // Apply retractions before insertions. A row's value changing at a single
                // timestamp arrives as a retraction of the old row followed by an insertion of
                // the new one, both keyed by the same (metric_name, labels). Clearing the slot
                // first means this legitimate update isn't mistaken by `apply_row` for two rows
                // racing for the same series.
                ok_updates.sort_by_key(|(_, diff)| diff.into_inner() > 0);
                for (row, diff) in &ok_updates {
                    let (name, kind, labels, value, help) = extract_row(&cols, row);
                    st.apply_row(&name, &kind, &labels, value, &help, diff.into_inner());
                }

                st.errors += err_diff_total;

                let mut frontier = Antichain::new();
                for f in frontiers {
                    frontier.extend(f.frontier().iter().copied());
                }
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
/// `metric_name`, `metric_type`, `labels`, `value`, and `help` columns, each `NOT NULL` and of
/// the required type, but their position within the row is unconstrained.
struct ColumnIndices {
    metric_name: usize,
    metric_type: usize,
    labels: usize,
    value: usize,
    help: usize,
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
            metric_type: idx("metric_type"),
            labels: idx("labels"),
            value: idx("value"),
            help: idx("help"),
        }
    }
}

/// Extracts `(metric_name, metric_type, sorted labels, value, help)` from one source row.
fn extract_row(
    cols: &ColumnIndices,
    row: &Row,
) -> (String, String, Vec<(String, String)>, f64, String) {
    let datums: Vec<_> = row.iter().collect();
    let metric_name = datums[cols.metric_name].unwrap_str().to_string();
    let metric_type = datums[cols.metric_type].unwrap_str().to_string();
    let mut labels: Vec<(String, String)> = datums[cols.labels]
        .unwrap_map()
        .iter()
        .map(|(k, v)| (k.to_string(), v.unwrap_str().to_string()))
        .collect();
    labels.sort();
    let value = datums[cols.value].unwrap_float64();
    let help = datums[cols.help].unwrap_str().to_string();
    (metric_name, metric_type, labels, value, help)
}

/// Key into [`SinkState::working`] / [`SinkState::published`]: a metric name paired with its
/// sorted label vector.
type PublishedKey = (String, Vec<(String, String)>);
/// Value in [`SinkState::working`] / [`SinkState::published`]: the series' current value, kind,
/// and help string.
type PublishedValue = (f64, MetricKind, String);

/// Working and published metric state for one metric sink.
///
/// The working map always integrates ok-collection diffs. `published` is what the collector
/// exposes and is refreshed from `working` only while `errors` is zero, so an errored dataflow
/// freezes the last healthy values.
#[derive(Default)]
struct SinkState {
    working: BTreeMap<PublishedKey, PublishedValue>,
    published: BTreeMap<PublishedKey, PublishedValue>,
    /// Net count of live errors on the sink's input (can rise and fall as errors are
    /// retracted); publication is frozen while this is nonzero.
    errors: i64,
    frontier_ms: u64,
    skipped: u64,
    conflicts: u64,
    collisions: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum MetricKind {
    Gauge,
    Counter,
}

impl MetricKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "gauge" => Some(MetricKind::Gauge),
            "counter" => Some(MetricKind::Counter),
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

/// Matches Prometheus's metric name grammar: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
        }
        _ => false,
    }
}

/// Matches Prometheus's label name grammar: `[a-zA-Z_][a-zA-Z0-9_]*`.
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
    /// Folds one row-level update into the working state.
    ///
    /// `diff` follows differential dataflow sign conventions: positive inserts or overwrites the
    /// `(metric_name, labels)` entry, non-positive retracts it. Rows with an unsupported
    /// `metric_type` (anything but `gauge`/`counter`) or an invalid Prometheus metric or label
    /// name are dropped and counted in `skipped`.
    ///
    /// Overwriting an entry that is already present, i.e. an insertion for a key that was never
    /// retracted, counts as a collision: two source rows are live for the same series at once.
    /// Callers are expected to apply retractions before insertions within a timestamp so that
    /// ordinary value updates don't trigger a false positive here.
    fn apply_row(
        &mut self,
        metric_name: &str,
        metric_type: &str,
        labels: &[(String, String)],
        value: f64,
        help: &str,
        diff: i64,
    ) {
        let Some(kind) = MetricKind::parse(metric_type) else {
            self.skipped += 1;
            return;
        };
        let valid_name = is_valid_metric_name(metric_name);
        let valid_labels = labels.iter().all(|(k, _)| is_valid_label_name(k));
        if !valid_name || !valid_labels {
            self.skipped += 1;
            return;
        }

        let key = (metric_name.to_string(), labels.to_vec());
        if diff > 0 {
            if self
                .working
                .insert(key, (value, kind, help.to_string()))
                .is_some()
            {
                self.collisions += 1;
            }
        } else if diff < 0 {
            self.working.remove(&key);
        }
    }

    /// Refreshes `published` (and the `conflicts` count derived from it) from `working`, but only
    /// while the dataflow is free of live errors. While `errors > 0`, publication stays frozen at
    /// the last healthy snapshot; `working` keeps integrating updates in the meantime, so the
    /// next healthy publish reflects everything that happened during the freeze.
    fn publish_if_healthy(&mut self) {
        if self.errors == 0 {
            self.published = self.working.clone();
            self.conflicts = count_conflicts(&self.published);
        }
    }
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
                "The number of input rows whose metric name and labels match another live row.",
            ),
        }
    }
}

impl Collector for SinkCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = Vec::with_capacity(5);
        descs.extend(self.frontier_gauge.desc());
        descs.extend(self.errors_gauge.desc());
        descs.extend(self.skipped_gauge.desc());
        descs.extend(self.conflicts_gauge.desc());
        descs.extend(self.collisions_gauge.desc());
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
            build_families(&state.published)
        };

        families.extend(self.frontier_gauge.collect());
        families.extend(self.errors_gauge.collect());
        families.extend(self.skipped_gauge.collect());
        families.extend(self.conflicts_gauge.collect());
        families.extend(self.collisions_gauge.collect());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn fold_and_publish() {
        let mut st = SinkState::default();
        st.apply_row("m", "gauge", &[("a".into(), "1".into())], 2.0, "h", 1);
        st.publish_if_healthy();
        assert_eq!(st.published.len(), 1);

        // Unsupported type is skipped and counted.
        st.apply_row("h1", "histogram", &[], 1.0, "h", 1);
        assert_eq!(st.skipped, 1);

        // Error freezes publication.
        st.errors = 1;
        st.apply_row("m", "gauge", &[("a".into(), "1".into())], 9.0, "h", 1);
        st.publish_if_healthy();
        assert_eq!(
            st.published[&("m".into(), vec![("a".into(), "1".into())])].0,
            2.0
        );

        // Recovery republishes the integrated value.
        st.errors = 0;
        st.publish_if_healthy();
        assert_eq!(
            st.published[&("m".into(), vec![("a".into(), "1".into())])].0,
            9.0
        );
    }
}
