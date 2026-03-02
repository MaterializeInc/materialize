// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflow for Prometheus metrics gathered from the metrics registry.

use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::{Datum, Diff, Timestamp};
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, Column, columnar_exchange};
use prometheus::proto::MetricType;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};
use timely::container::DrainContainer;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::{ComputeLog, LogCollection, LogVariant, PermutedRowPacker};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::RowRowSpine;

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

/// Key type for the snapshot: (metric_name, sorted_labels_key).
/// The labels_key is a string of sorted `k=v` pairs for dedup/diffing.
type SnapshotKey = (String, String);
/// Value type for the snapshot: (value, metric_type, help).
type SnapshotValue = (f64, String, String);

/// Constructs the logging dataflow fragment for Prometheus metrics.
pub(super) fn construct<G: Scope<Timestamp = Timestamp>>(
    scope: G,
    config: &mz_compute_client::logging::LoggingConfig,
    metrics_registry: MetricsRegistry,
    now: Instant,
    start_offset: Duration,
) -> Return {
    let variant = LogVariant::Compute(ComputeLog::PrometheusMetrics);
    let mut collections = BTreeMap::new();
    let interval = config.interval;
    let interval_ms = std::cmp::max(1, interval.as_millis());

    if !config.index_logs.contains_key(&variant) {
        return Return { collections };
    }

    let worker_id = scope.index();
    let enable = scope.index() % scope.peers() == 0;

    // Build a source operator that periodically gathers Prometheus metrics.
    let mut builder = OperatorBuilder::new("PrometheusMetrics".to_string(), scope.clone());
    type SourceData = ((SnapshotKey, SnapshotValue), Timestamp, Diff);
    let (output, stream) = builder.new_output::<Column<SourceData>>();
    let mut output = OutputBuilder::<_, ColumnBuilder<SourceData>>::from(output);

    let operator_info = builder.operator_info();
    builder.build(move |capabilities| {
        // Metrics are per-process, so only one worker per process needs to
        // scrape. Drop the capability for disabled workers so the frontier
        // can advance without this operator holding it back.
        let mut cap = enable.then_some(capabilities.into_element());
        let activator = scope.activator_for(operator_info.address);

        let mut prev_snapshot: BTreeMap<SnapshotKey, SnapshotValue> = BTreeMap::new();
        let mut next_scrape = Instant::now();

        move |_frontiers| {
            let Some(cap) = &mut cap else { return };

            let elapsed = now.elapsed();
            let elapsed_ms = elapsed.as_millis();
            let time_ms: u128 =
                ((elapsed_ms + start_offset.as_millis()) / interval_ms + 1) * interval_ms;
            let ts: Timestamp = time_ms.try_into().expect("must fit");

            // Downgrade capability to the current timestamp.
            cap.downgrade(&ts);
            if Instant::now() < next_scrape {
                return;
            }
            next_scrape = Instant::now() + interval;

            // Gather current metrics and build new snapshot.
            let metric_families = metrics_registry.gather();
            let new_snapshot = flatten_metrics(&metric_families);

            // Diff against previous snapshot and emit updates.
            let mut output = output.activate();
            let mut session = output.session_with_builder(&cap);

            // Retract entries that were removed or changed.
            for (key, old_val) in &prev_snapshot {
                match new_snapshot.get(key) {
                    Some(new_val) if new_val == old_val => {}
                    _ => {
                        let key = (&key.0, &key.1);
                        let val = (&old_val.0, &old_val.1, &old_val.2);
                        session.give(((key, val), ts, Diff::MINUS_ONE));
                    }
                }
            }

            // Insert entries that are new or changed.
            for (key, new_val) in &new_snapshot {
                match prev_snapshot.get(key) {
                    Some(old_val) if old_val == new_val => {}
                    _ => {
                        let key = (&key.0, &key.1);
                        let val = (&new_val.0, &new_val.1, &new_val.2);
                        session.give(((key, val), ts, Diff::ONE));
                    }
                }
            }

            prev_snapshot = new_snapshot;

            // Reschedule after the current scrape interval.
            activator.activate_after(interval);
        }
    });

    // Convert the raw tuples into packed Row pairs.
    let packed =
        stream.unary::<ColumnBuilder<_>, _, _, _>(Pipeline, "ToRow PrometheusMetrics", |_, _| {
            let mut packer = PermutedRowPacker::new(ComputeLog::PrometheusMetrics);
            move |input, output| {
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for (((metric_name, labels_key), (value, metric_type, help)), ts, diff) in
                        data.drain()
                    {
                        let labels = parse_labels(labels_key);
                        let (key, val) = pack_row(
                            &mut packer,
                            metric_name,
                            metric_type,
                            &labels,
                            *value,
                            help,
                            worker_id,
                        );
                        session.give(((key, val), ts, diff));
                    }
                });
            }
        });

    // Arrange into a trace.
    let exchange = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
        columnar_exchange::<mz_repr::Row, mz_repr::Row, Timestamp, mz_repr::Diff>,
    );
    let trace = packed
        .mz_arrange_core::<_, Col2ValBatcher<_, _, _, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
            exchange,
            "Arrange PrometheusMetrics",
        )
        .trace;
    let token: Rc<dyn std::any::Any> = Rc::new(());
    let collection = LogCollection { trace, token };
    collections.insert(variant, collection);

    Return { collections }
}

/// Flatten metric families into a snapshot map.
fn flatten_metrics(
    families: &[prometheus::proto::MetricFamily],
) -> BTreeMap<SnapshotKey, SnapshotValue> {
    let mut snapshot = BTreeMap::new();

    for family in families {
        let base_name = family.name();
        let help = family.help();
        let metric_type = family.get_field_type();
        let type_str = match metric_type {
            MetricType::COUNTER => "counter",
            MetricType::GAUGE => "gauge",
            MetricType::HISTOGRAM => "histogram",
            MetricType::SUMMARY => "summary",
            MetricType::UNTYPED => "untyped",
        };

        for metric in family.get_metric() {
            let base_labels: Vec<(&str, &str)> = metric
                .get_label()
                .iter()
                .map(|l| (l.name(), l.value()))
                .collect();

            match metric_type {
                MetricType::COUNTER => {
                    let value = metric.get_counter().get_value();
                    insert_row(
                        &mut snapshot,
                        base_name,
                        &base_labels,
                        value,
                        type_str,
                        help,
                    );
                }
                MetricType::GAUGE => {
                    let value = metric.get_gauge().get_value();
                    insert_row(
                        &mut snapshot,
                        base_name,
                        &base_labels,
                        value,
                        type_str,
                        help,
                    );
                }
                MetricType::HISTOGRAM => {
                    let histogram = metric.get_histogram();

                    // One row per bucket with `le` label.
                    for bucket in histogram.get_bucket() {
                        let mut labels = base_labels.clone();
                        let le_str = format_f64(bucket.upper_bound());
                        labels.push(("le", &le_str));
                        labels.sort_by_key(|(k, _)| *k);
                        insert_row(
                            &mut snapshot,
                            &format!("{base_name}_bucket"),
                            &labels,
                            f64::cast_lossy(bucket.cumulative_count()),
                            type_str,
                            help,
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        &format!("{base_name}_sum"),
                        &base_labels,
                        histogram.get_sample_sum(),
                        type_str,
                        help,
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        &format!("{base_name}_count"),
                        &base_labels,
                        f64::cast_lossy(histogram.get_sample_count()),
                        type_str,
                        help,
                    );
                }
                MetricType::SUMMARY => {
                    let summary = metric.get_summary();

                    // One row per quantile.
                    for quantile in summary.get_quantile() {
                        let mut labels = base_labels.clone();
                        let q_str = format_f64(quantile.quantile());
                        labels.push(("quantile", &q_str));
                        labels.sort_by_key(|(k, _)| *k);
                        insert_row(
                            &mut snapshot,
                            base_name,
                            &labels,
                            quantile.value(),
                            type_str,
                            help,
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        &format!("{base_name}_sum"),
                        &base_labels,
                        summary.sample_sum(),
                        type_str,
                        help,
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        &format!("{base_name}_count"),
                        &base_labels,
                        f64::cast_lossy(summary.sample_count()),
                        type_str,
                        help,
                    );
                }
                MetricType::UNTYPED => {
                    #[allow(deprecated)]
                    let value = metric.get_untyped().get_value();
                    insert_row(
                        &mut snapshot,
                        base_name,
                        &base_labels,
                        value,
                        type_str,
                        help,
                    );
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
    snapshot: &mut BTreeMap<SnapshotKey, SnapshotValue>,
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
    metric_type: &str,
    help: &str,
) {
    let mut sorted_labels: Vec<(&str, &str)> = labels.to_vec();
    sorted_labels.sort_by_key(|(k, _)| *k);
    let labels_key = sorted_labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    snapshot.insert(
        (name.to_string(), labels_key),
        (value, metric_type.to_string(), help.to_string()),
    );
}

/// Parse a labels key string back into sorted label pairs.
fn parse_labels(labels_key: &str) -> Vec<(&str, &str)> {
    if labels_key.is_empty() {
        return Vec::new();
    }
    labels_key
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .collect()
}

/// Pack a metric row into key/value row pairs.
fn pack_row<'a>(
    packer: &'a mut PermutedRowPacker,
    metric_name: &str,
    metric_type: &str,
    labels: &[(&str, &str)],
    value: f64,
    help: &str,
    worker_id: usize,
) -> (&'a mz_repr::RowRef, &'a mz_repr::RowRef) {
    packer.pack_by_index(|row_packer, index| match index {
        // metric_name
        0 => row_packer.push(Datum::String(metric_name)),
        // metric_type
        1 => row_packer.push(Datum::String(metric_type)),
        // labels (Map)
        2 => {
            row_packer.push_dict(labels.iter().map(|(k, v)| (*k, Datum::String(v))));
        }
        // value
        3 => row_packer.push(Datum::Float64(value.into())),
        // help
        4 => row_packer.push(Datum::String(help)),
        // worker_id
        5 => row_packer.push(Datum::UInt64(u64::cast_from(worker_id))),
        _ => unreachable!("unexpected column index {index}"),
    })
}
