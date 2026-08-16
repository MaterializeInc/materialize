// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflow for Prometheus metrics gathered from the metrics registry.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::dyncfgs::COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::soft_panic_or_log;
use mz_repr::{Datum, Diff, Timestamp};
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, columnar_exchange};
use prometheus::proto::MetricType;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::{ComputeLog, LogCollection, LogVariant, PermutedRowPacker};
use crate::typedefs::RowRowSpine;
use mz_row_spine::RowRowBuilder;

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

/// Key type for the snapshot: (metric_name, sorted_label_pairs).
///
/// The metric name is `Arc<str>` because it is shared across all rows of a
/// metric family and across scrapes that don't change it, so re-scraping
/// doesn't re-allocate it.
type SnapshotKey = (Arc<str>, Vec<(String, String)>);
/// Value type for the snapshot: (value, metric_type, help).
///
/// The help text is `Arc<str>` because it is identical for every row of a
/// metric family, so it is shared rather than cloned per row.
type SnapshotValue = (f64, &'static str, Arc<str>);
/// A snapshot of all gathered metrics, keyed for equality-only diffing.
///
/// Uses `HashMap` rather than `BTreeMap` because the diff performs only
/// key-equality lookups and the output is re-sorted downstream by
/// `mz_arrange_core`, so ordered iteration is not needed. A `BTreeMap` paid an
/// O(label-length) full-key comparison at every node on every insert and
/// lookup, which dominated per-scrape CPU. A `HashMap` hashes the key once per
/// access and compares only on a bucket match. The key is the full
/// `SnapshotKey`, not a hash fingerprint, so collisions cannot corrupt output.
///
/// This is the stdlib `HashMap`, not the deterministic `mz_ore` wrapper, which
/// hides iteration that [`emit_diff`] needs. The unstable iteration order is
/// sound because the emitted retract/insert set is an order-independent
/// multiset that `mz_arrange_core` re-sorts.
#[allow(clippy::disallowed_types)]
type Snapshot = std::collections::HashMap<SnapshotKey, SnapshotValue>;

/// Constructs the logging dataflow fragment for Prometheus metrics.
pub(super) fn construct(
    scope: Scope<'_, Timestamp>,
    config: &mz_compute_client::logging::LoggingConfig,
    metrics_registry: MetricsRegistry,
    now: Instant,
    start_offset: Duration,
    worker_config: Rc<ConfigSet>,
    workers_per_process: usize,
) -> Return {
    let variant = LogVariant::Compute(ComputeLog::PrometheusMetrics);
    let mut collections = BTreeMap::new();
    let interval = config.interval;
    let interval_ms = std::cmp::max(1, interval.as_millis());

    if !config.index_logs.contains_key(&variant) {
        return Return { collections };
    }

    let process_id = scope.index() / workers_per_process;
    let enable = scope.index() % workers_per_process == 0;

    // Build a source operator that periodically gathers Prometheus metrics
    // and packs them directly into Row pairs.
    let mut builder = OperatorBuilder::new("PrometheusMetrics".to_string(), scope.clone());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::<_, ColumnBuilder<_>>::from(output);

    let operator_info = builder.operator_info();
    builder.build(move |capabilities| {
        // Metrics are per-process, so only one worker per process needs to
        // scrape. Drop the capability for disabled workers so the frontier
        // can advance without this operator holding it back.
        let mut cap = enable.then_some(capabilities.into_element());
        let activator = scope.activator_for(operator_info.address);

        let mut prev_snapshot: Snapshot = Snapshot::new();
        let mut next_scrape = Instant::now();
        let mut packer = PermutedRowPacker::new(ComputeLog::PrometheusMetrics);

        move |_frontiers| {
            let Some(cap) = &mut cap else { return };

            // Advance the capability to the next logging interval boundary.
            // This keeps the output frontier progressing at the logging
            // rate, even when scrapes happen less frequently. Note that
            // advancing the frontier implies the data is up-to-date, but
            // the metrics snapshot may be stale by up to the scrape
            // interval when it exceeds the logging interval.
            let elapsed = now.elapsed().as_millis();
            let time_ms: u128 =
                ((elapsed + start_offset.as_millis()) / interval_ms + 1) * interval_ms;
            let ts: Timestamp = time_ms.try_into().expect("must fit");
            cap.downgrade(&ts);

            // Schedule the next activation at the interval boundary
            // to avoid drift from wall-clock elapsed time.
            let next_boundary_ms = time_ms - start_offset.as_millis();
            let next_activation =
                now + Duration::from_millis(next_boundary_ms.try_into().expect("must fit"));
            activator.activate_after(next_activation.saturating_duration_since(Instant::now()));

            // Only scrape when the scrape interval has elapsed.
            // The operator wakes every logging interval to advance the
            // capability, but scrapes less frequently if configured.
            if Instant::now() < next_scrape {
                return;
            }

            let prom_interval =
                COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL.get(&worker_config);
            let effective_interval = prom_interval.max(interval);
            next_scrape = Instant::now() + effective_interval;

            // Gather current metrics and build new snapshot, or an empty
            // snapshot when disabled (which retracts any existing data).
            //
            // `gather()` is O(registry size): it builds and sorts a proto for
            // every registered metric, independent of whether the metric
            // changed since the last scrape. With the snapshot diffing and
            // string allocation costs addressed, this is the remaining floor on
            // per-scrape CPU. Reducing it requires lowering metric cardinality
            // (which scales with operator/collection count), not changes here.
            let new_snapshot = if !prom_interval.is_zero() {
                let metric_families = metrics_registry.gather();
                flatten_metrics(metric_families)
            } else {
                Snapshot::new()
            };

            // Diff against previous snapshot and emit packed Row pairs.
            let mut output = output.activate();
            let mut session = output.session_with_builder(&cap);

            emit_diff(&prev_snapshot, &new_snapshot, |key, val, diff| {
                let (row_key, row_val) = pack_row(
                    &mut packer,
                    &key.0,
                    val.1,
                    &key.1,
                    val.0,
                    &val.2,
                    process_id,
                );
                session.give(((row_key, row_val), ts, diff));
            });

            prev_snapshot = new_snapshot;
        }
    });

    // Arrange into a trace.
    let exchange = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
        columnar_exchange::<mz_repr::Row, mz_repr::Row, Timestamp, mz_repr::Diff>,
    );
    let trace = stream
        .mz_arrange_core::<
            _,
            batcher::Chunker<_>,
            Col2ValBatcher<_, _, _, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >(exchange, "Arrange PrometheusMetrics")
        .trace;
    let token: Rc<dyn std::any::Any> = Rc::new(());
    let collection = LogCollection { trace, token };
    collections.insert(variant, collection);

    Return { collections }
}

/// Compute the diff between two metric snapshots, invoking `emit` once per row
/// that must be retracted (`Diff::MINUS_ONE`) or inserted (`Diff::ONE`).
///
/// Entries present in both snapshots with an unchanged value are suppressed.
/// Changed entries produce a retraction of the old value and an insertion of
/// the new one. The caller is responsible for packing and emitting each row.
///
/// Only key-equality lookups are used, so a `HashMap` snapshot is sufficient.
fn emit_diff(
    prev: &Snapshot,
    new: &Snapshot,
    mut emit: impl FnMut(&SnapshotKey, &SnapshotValue, Diff),
) {
    // Retract entries that were removed or changed.
    for (key, old_val) in prev {
        match new.get(key) {
            Some(new_val) if new_val == old_val => {}
            _ => emit(key, old_val, Diff::MINUS_ONE),
        }
    }

    // Insert entries that are new or changed.
    for (key, new_val) in new {
        match prev.get(key) {
            Some(old_val) if old_val == new_val => {}
            _ => emit(key, new_val, Diff::ONE),
        }
    }
}

/// Flatten metric families into a snapshot map.
///
/// Strings are moved out of the gathered proto (via `take_*`) rather than
/// cloned. The per-family `name`/`help` are shared across all of a family's
/// rows via `Arc<str>`, and derived histogram/summary names
/// (`_bucket`/`_sum`/`_count`) are formatted once per metric and shared across
/// their rows. This keeps re-scraping unchanged metrics from re-allocating
/// these strings on every scrape.
fn flatten_metrics(families: Vec<prometheus::proto::MetricFamily>) -> Snapshot {
    let mut snapshot = Snapshot::new();

    for mut family in families {
        let metric_type = family.get_field_type();
        let type_str = match metric_type {
            MetricType::COUNTER => "counter",
            MetricType::GAUGE => "gauge",
            MetricType::HISTOGRAM => "histogram",
            MetricType::SUMMARY => "summary",
            MetricType::UNTYPED => "untyped",
        };
        // `name`/`help` are shared by every row this family produces. Take
        // ownership of the proto's strings and share them via `Arc<str>`.
        let help: Arc<str> = Arc::from(family.take_help());
        let base_name: Arc<str> = Arc::from(family.take_name());

        for mut metric in family.take_metric() {
            // Move the label strings out of the proto rather than cloning.
            let base_labels: Vec<(String, String)> = metric
                .take_label()
                .into_iter()
                .map(|mut l| (l.take_name(), l.take_value()))
                .collect();

            match metric_type {
                MetricType::COUNTER => {
                    let value = metric.get_counter().value();
                    insert_row(
                        &mut snapshot,
                        Arc::clone(&base_name),
                        base_labels,
                        value,
                        type_str,
                        Arc::clone(&help),
                    );
                }
                MetricType::GAUGE => {
                    let value = metric.get_gauge().value();
                    insert_row(
                        &mut snapshot,
                        Arc::clone(&base_name),
                        base_labels,
                        value,
                        type_str,
                        Arc::clone(&help),
                    );
                }
                MetricType::HISTOGRAM => {
                    let histogram = metric.get_histogram();

                    // Derived names are formatted once and shared across the
                    // rows below via `Arc<str>`.
                    let bucket_name: Arc<str> = Arc::from(format!("{base_name}_bucket"));
                    let sum_name: Arc<str> = Arc::from(format!("{base_name}_sum"));
                    let count_name: Arc<str> = Arc::from(format!("{base_name}_count"));

                    // One row per bucket with `le` label.
                    for bucket in histogram.get_bucket() {
                        let mut labels = base_labels.clone();
                        labels.push(("le".to_string(), format_f64(bucket.upper_bound())));
                        insert_row(
                            &mut snapshot,
                            Arc::clone(&bucket_name),
                            labels,
                            f64::cast_lossy(bucket.cumulative_count()),
                            type_str,
                            Arc::clone(&help),
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        sum_name,
                        base_labels.clone(),
                        histogram.get_sample_sum(),
                        type_str,
                        Arc::clone(&help),
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        count_name,
                        base_labels,
                        f64::cast_lossy(histogram.get_sample_count()),
                        type_str,
                        Arc::clone(&help),
                    );
                }
                MetricType::SUMMARY => {
                    let summary = metric.get_summary();

                    // Derived names are formatted once and shared via `Arc<str>`.
                    let sum_name: Arc<str> = Arc::from(format!("{base_name}_sum"));
                    let count_name: Arc<str> = Arc::from(format!("{base_name}_count"));

                    // One row per quantile.
                    for quantile in summary.get_quantile() {
                        let mut labels = base_labels.clone();
                        labels.push(("quantile".to_string(), format_f64(quantile.quantile())));
                        insert_row(
                            &mut snapshot,
                            Arc::clone(&base_name),
                            labels,
                            quantile.value(),
                            type_str,
                            Arc::clone(&help),
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        sum_name,
                        base_labels.clone(),
                        summary.sample_sum(),
                        type_str,
                        Arc::clone(&help),
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        count_name,
                        base_labels,
                        f64::cast_lossy(summary.sample_count()),
                        type_str,
                        Arc::clone(&help),
                    );
                }
                MetricType::UNTYPED => {
                    soft_panic_or_log!("unexpected untyped metric: {base_name}");
                }
            }
        }
    }

    snapshot
}

/// Format an f64 for use as a label value, matching Prometheus conventions.
fn format_f64(v: f64) -> String {
    if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        v.to_string()
    }
}

/// Insert a single metric row into the snapshot.
fn insert_row(
    snapshot: &mut Snapshot,
    name: Arc<str>,
    mut labels: Vec<(String, String)>,
    value: f64,
    metric_type: &'static str,
    help: Arc<str>,
) {
    labels.sort();

    snapshot.insert((name, labels), (value, metric_type, help));
}

/// Pack a metric row into key/value row pairs.
fn pack_row<'a>(
    packer: &'a mut PermutedRowPacker,
    metric_name: &str,
    metric_type: &str,
    labels: &[(String, String)],
    value: f64,
    help: &str,
    process_id: usize,
) -> (&'a mz_repr::RowRef, &'a mz_repr::RowRef) {
    packer.pack_by_index(|row_packer, index| match index {
        // process_id
        0 => row_packer.push(Datum::UInt64(u64::cast_from(process_id))),
        // metric_name
        1 => row_packer.push(Datum::String(metric_name)),
        // metric_type
        2 => row_packer.push(Datum::String(metric_type)),
        // labels (Map)
        3 => {
            row_packer.push_dict(labels.iter().map(|(k, v)| (k.as_str(), Datum::String(v))));
        }
        // value
        4 => row_packer.push(Datum::Float64(value.into())),
        // help
        5 => row_packer.push(Datum::String(help)),
        _ => unreachable!("unexpected column index {index}"),
    })
}

#[cfg(test)]
mod tests {
    use prometheus::proto::{Bucket, Counter, Histogram, LabelPair, Metric, MetricFamily};

    use super::*;

    /// An owned, comparable representation of a single emitted diff row:
    /// (name, sorted_labels, value, metric_type, help, diff).
    type DiffRow = (
        String,
        Vec<(String, String)>,
        f64,
        &'static str,
        String,
        Diff,
    );

    fn label(name: &str, value: &str) -> LabelPair {
        let mut l = LabelPair::new();
        l.set_name(name.to_string());
        l.set_value(value.to_string());
        l
    }

    fn metric(labels: &[(&str, &str)]) -> Metric {
        let mut m = Metric::new();
        m.set_label(labels.iter().map(|(k, v)| label(k, v)).collect());
        m
    }

    fn counter_family(name: &str, help: &str, metrics: &[(&[(&str, &str)], f64)]) -> MetricFamily {
        let mut fam = MetricFamily::new();
        fam.set_name(name.to_string());
        fam.set_help(help.to_string());
        fam.set_field_type(MetricType::COUNTER);
        let ms = metrics
            .iter()
            .map(|(labels, value)| {
                let mut m = metric(labels);
                let mut counter = Counter::new();
                counter.set_value(*value);
                m.set_counter(counter);
                m
            })
            .collect();
        fam.set_metric(ms);
        fam
    }

    /// Collect the emitted diff into a deterministic (sorted) owned vector.
    fn collect_diff(prev: &Snapshot, new: &Snapshot) -> Vec<DiffRow> {
        let mut out = Vec::new();
        emit_diff(prev, new, |key, val, diff| {
            out.push((
                key.0.to_string(),
                key.1.clone(),
                val.0,
                val.1,
                val.2.to_string(),
                diff,
            ));
        });
        // Sort retractions before insertions, then by name and labels, so the
        // assertions don't depend on `HashMap` iteration order.
        out.sort_by(|a, b| {
            let order = |d: &DiffRow| if d.5 == Diff::MINUS_ONE { 0 } else { 1 };
            (order(a), &a.0, &a.1).cmp(&(order(b), &b.0, &b.1))
        });
        out
    }

    #[mz_ore::test]
    fn test_flatten_counter_labels_sorted() {
        // Labels are provided out of order. The snapshot key must be sorted.
        let families = vec![counter_family(
            "requests",
            "total requests",
            &[(&[("worker", "1"), ("region", "us")], 7.0)],
        )];
        let snapshot = flatten_metrics(families);

        let expected_labels = vec![
            ("region".to_string(), "us".to_string()),
            ("worker".to_string(), "1".to_string()),
        ];
        let key = (Arc::from("requests"), expected_labels);
        let value = snapshot.get(&key).expect("counter row present");
        assert_eq!(value.0, 7.0);
        assert_eq!(value.1, "counter");
        assert_eq!(&*value.2, "total requests");
        assert_eq!(snapshot.len(), 1);
    }

    #[mz_ore::test]
    fn test_diff_retract_insert_and_suppression() {
        // Previous scrape: three metrics (a, b, d).
        let prev = flatten_metrics(vec![counter_family(
            "m",
            "h",
            &[
                (&[("k", "a")], 1.0),
                (&[("k", "b")], 2.0),
                (&[("k", "d")], 5.0),
            ],
        )]);
        // Next scrape: `a` unchanged, `b` changed, `d` removed, `c` new.
        let new = flatten_metrics(vec![counter_family(
            "m",
            "h",
            &[
                (&[("k", "a")], 1.0),
                (&[("k", "b")], 3.0),
                (&[("k", "c")], 9.0),
            ],
        )]);

        let diff = collect_diff(&prev, &new);

        let labels = |v: &str| vec![("k".to_string(), v.to_string())];
        // Unchanged `a` is suppressed. `b` retracts old and inserts new. `d`
        // retracts. `c` inserts.
        assert_eq!(
            diff,
            vec![
                (
                    "m".to_string(),
                    labels("b"),
                    2.0,
                    "counter",
                    "h".to_string(),
                    Diff::MINUS_ONE
                ),
                (
                    "m".to_string(),
                    labels("d"),
                    5.0,
                    "counter",
                    "h".to_string(),
                    Diff::MINUS_ONE
                ),
                (
                    "m".to_string(),
                    labels("b"),
                    3.0,
                    "counter",
                    "h".to_string(),
                    Diff::ONE
                ),
                (
                    "m".to_string(),
                    labels("c"),
                    9.0,
                    "counter",
                    "h".to_string(),
                    Diff::ONE
                ),
            ]
        );
    }

    #[mz_ore::test]
    fn test_identical_snapshots_emit_nothing() {
        let snapshot = flatten_metrics(vec![counter_family("m", "h", &[(&[("k", "a")], 1.0)])]);
        // Re-scraping unchanged metrics must produce no diff.
        assert!(collect_diff(&snapshot, &snapshot).is_empty());
    }

    #[mz_ore::test]
    fn test_flatten_histogram_rows() {
        let mut fam = MetricFamily::new();
        fam.set_name("lat".to_string());
        fam.set_help("latency".to_string());
        fam.set_field_type(MetricType::HISTOGRAM);

        let mut hist = Histogram::new();
        hist.set_sample_sum(12.5);
        hist.set_sample_count(7);
        let buckets = [(0.5_f64, 3_u64), (1.0_f64, 7_u64)]
            .into_iter()
            .map(|(ub, count)| {
                let mut b = Bucket::new();
                b.set_upper_bound(ub);
                b.set_cumulative_count(count);
                b
            })
            .collect();
        hist.set_bucket(buckets);

        // Base labels deliberately out of order to exercise sorting.
        let mut m = metric(&[("z", "1"), ("a", "2")]);
        m.set_histogram(hist);
        fam.set_metric(vec![m]);

        let snapshot = flatten_metrics(vec![fam]);

        // Expect: two `_bucket` rows (each with sorted labels including `le`),
        // one `_sum`, one `_count`.
        assert_eq!(snapshot.len(), 4);

        let base = || {
            vec![
                ("a".to_string(), "2".to_string()),
                ("z".to_string(), "1".to_string()),
            ]
        };
        let with_le = |le: &str| {
            let mut l = base();
            l.push(("le".to_string(), le.to_string()));
            l.sort();
            l
        };

        let bucket_05 = snapshot
            .get(&(Arc::from("lat_bucket"), with_le("0.5")))
            .expect("le=0.5 bucket present");
        assert_eq!(bucket_05.0, 3.0);
        assert_eq!(bucket_05.1, "histogram");

        let bucket_1 = snapshot
            .get(&(Arc::from("lat_bucket"), with_le("1")))
            .expect("le=1 bucket present");
        assert_eq!(bucket_1.0, 7.0);

        let sum = snapshot
            .get(&(Arc::from("lat_sum"), base()))
            .expect("_sum row present");
        assert_eq!(sum.0, 12.5);

        let count = snapshot
            .get(&(Arc::from("lat_count"), base()))
            .expect("_count row present");
        assert_eq!(count.0, 7.0);
        assert_eq!(&*count.2, "latency");
    }
}
