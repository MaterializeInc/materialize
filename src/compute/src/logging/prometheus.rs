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
use std::time::{Duration, Instant};

use mz_compute_types::dyncfgs::COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::soft_panic_or_log;
use mz_repr::{Datum, Timestamp};
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, columnar_exchange};
use prometheus::proto::MetricType;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::{
    ComputeLog, LogCollection, LogVariant, PermutedRowPacker, downgrade_to_interval_boundary,
    emit_snapshot_diff,
};
use crate::typedefs::RowRowSpine;
use mz_row_spine::RowRowBuilder;

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

/// Key type for the snapshot: (metric_name, sorted_label_pairs).
type SnapshotKey = (String, Vec<(String, String)>);
/// Value type for the snapshot: (value, metric_type, help).
type SnapshotValue = (f64, &'static str, String);

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

        let mut prev_snapshot: BTreeMap<SnapshotKey, SnapshotValue> = BTreeMap::new();
        let mut next_scrape = Instant::now();
        let mut packer = PermutedRowPacker::new(ComputeLog::PrometheusMetrics);

        move |_frontiers| {
            let Some(cap) = &mut cap else { return };

            let ts =
                downgrade_to_interval_boundary(cap, &activator, now, start_offset, interval_ms);

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
            let new_snapshot = if !prom_interval.is_zero() {
                let metric_families = metrics_registry.gather();
                flatten_metrics(metric_families)
            } else {
                BTreeMap::new()
            };

            // Diff against previous snapshot and emit packed Row pairs.
            let mut output = output.activate();
            let mut session = output.session_with_builder(&cap);
            emit_snapshot_diff(
                &mut session,
                &mut packer,
                &prev_snapshot,
                &new_snapshot,
                ts,
                |packer, key, value| {
                    pack_row(
                        packer, &key.0, value.1, &key.1, value.0, &value.2, process_id,
                    )
                },
            );

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

/// Flatten metric families into a snapshot map.
fn flatten_metrics(
    families: Vec<prometheus::proto::MetricFamily>,
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
            let base_labels: Vec<(String, String)> = metric
                .get_label()
                .iter()
                .map(|l| (l.name().to_string(), l.value().to_string()))
                .collect();

            match metric_type {
                MetricType::COUNTER => {
                    let value = metric.get_counter().value();
                    insert_row(
                        &mut snapshot,
                        base_name.to_string(),
                        base_labels,
                        value,
                        type_str,
                        help,
                    );
                }
                MetricType::GAUGE => {
                    let value = metric.get_gauge().value();
                    insert_row(
                        &mut snapshot,
                        base_name.to_string(),
                        base_labels,
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
                        labels.push(("le".to_string(), format_f64(bucket.upper_bound())));
                        insert_row(
                            &mut snapshot,
                            format!("{base_name}_bucket"),
                            labels,
                            f64::cast_lossy(bucket.cumulative_count()),
                            type_str,
                            help,
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        format!("{base_name}_sum"),
                        base_labels.clone(),
                        histogram.get_sample_sum(),
                        type_str,
                        help,
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        format!("{base_name}_count"),
                        base_labels,
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
                        labels.push(("quantile".to_string(), format_f64(quantile.quantile())));
                        insert_row(
                            &mut snapshot,
                            base_name.to_string(),
                            labels,
                            quantile.value(),
                            type_str,
                            help,
                        );
                    }

                    // _sum row
                    insert_row(
                        &mut snapshot,
                        format!("{base_name}_sum"),
                        base_labels.clone(),
                        summary.sample_sum(),
                        type_str,
                        help,
                    );

                    // _count row
                    insert_row(
                        &mut snapshot,
                        format!("{base_name}_count"),
                        base_labels,
                        f64::cast_lossy(summary.sample_count()),
                        type_str,
                        help,
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
    snapshot: &mut BTreeMap<SnapshotKey, SnapshotValue>,
    name: String,
    mut labels: Vec<(String, String)>,
    value: f64,
    metric_type: &'static str,
    help: &str,
) {
    labels.sort();

    snapshot.insert((name, labels), (value, metric_type, help.to_string()));
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
