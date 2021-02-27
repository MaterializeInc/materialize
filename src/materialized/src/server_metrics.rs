// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tools for interacting with Prometheus metrics

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::time::Instant;

use crate::BUILD_INFO;
use lazy_static::lazy_static;
use prometheus::{
    register_gauge_vec, register_int_gauge_vec, Gauge, GaugeVec, IntGauge, IntGaugeVec,
};
use sysinfo::{ProcessorExt, SystemExt};

pub const METRIC_SERVER_METADATA: &str = "mz_server_metadata_seconds";
pub const METRIC_WORKER_COUNT: &str = "mz_server_metadata_timely_worker_threads";

lazy_static! {
    static ref SERVER_METADATA_RAW: GaugeVec = register_gauge_vec!(
        METRIC_SERVER_METADATA,
        "server metadata, value is uptime",
        &[
            "build_time",
            "build_version",
            "build_sha",
            "os",
            "ncpus_logical",
            "ncpus_physical",
            "cpu0",
            "memory_total",
        ]
    )
    .expect("can build mz_server_metadata");
    static ref SERVER_METADATA: Gauge = {
        let mut system = sysinfo::System::new();
        system.refresh_system();

        SERVER_METADATA_RAW.with_label_values(&[
            BUILD_INFO.time,
            BUILD_INFO.version,
            BUILD_INFO.sha,
            &os_info::get().to_string(),
            &num_cpus::get().to_string(),
            &num_cpus::get_physical().to_string(),
            &{
                let cpu0 = &system.get_processors()[0];
                format!("{} {}MHz", cpu0.get_brand(), cpu0.get_frequency())
            },
            &system.get_total_memory().to_string(),
        ])
    };
    pub static ref WORKER_COUNT: IntGaugeVec = register_int_gauge_vec!(
        METRIC_WORKER_COUNT,
        "number of timely workers materialized is running with",
        &["count"]
    )
    .unwrap();
    static ref REQUEST_TIMES: IntGaugeVec = register_int_gauge_vec!(
        "mz_server_scrape_metrics_times",
        "how long it took to gather metrics, used for very low frequency high accuracy measures",
        &["action"]
    )
    .unwrap();
    static ref REQUEST_METRICS_GATHER: IntGauge = REQUEST_TIMES.with_label_values(&["gather"]);
    pub static ref REQUEST_METRICS_ENCODE: IntGauge = REQUEST_TIMES.with_label_values(&["encode"]);
}

/// Call [`prometheus::gather`], ensuring that all our metrics are up to date
pub fn load_prom_metrics(start_time: Instant) -> Vec<prometheus::proto::MetricFamily> {
    let before_gather = Instant::now();
    let uptime = before_gather - start_time;
    let (secs, milli_part) = (uptime.as_secs() as f64, uptime.subsec_millis() as f64);
    SERVER_METADATA.set(secs + milli_part / 1_000.0);
    let result = prometheus::gather();

    REQUEST_METRICS_GATHER.set(Instant::now().duration_since(before_gather).as_micros() as i64);
    result
}

pub fn filter_metrics<'a>(
    metrics: &'a [prometheus::proto::MetricFamily],
    filter: &HashSet<&str>,
) -> BTreeMap<&'a str, Vec<PromMetric<'a>>> {
    metrics
        .iter()
        .filter(|m| filter.contains(m.get_name()))
        .filter_map(|m| PromMetric::from_metric_family(m).ok())
        .filter_map(|ms| {
            if let Some(m) = ms.get(0) {
                Some((m.name(), ms))
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug)]
pub enum PromMetric<'a> {
    Counter {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Gauge {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Histogram {
        name: &'a str,
        count: u64,
        labels: BTreeMap<&'a str, &'a str>,
    },
}

impl fmt::Display for PromMetric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn fmt(
            f: &mut fmt::Formatter,
            name: &str,
            value: impl fmt::Display,
            labels: &BTreeMap<&str, &str>,
        ) -> fmt::Result {
            write!(f, "{} ", name)?;
            for (n, v) in labels.iter() {
                write!(f, "{}={} ", n, v)?;
            }
            writeln!(f, "{}", value)
        }
        match self {
            PromMetric::Counter {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Gauge {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Histogram {
                name,
                count,
                labels,
            } => fmt(f, name, count, labels),
        }
    }
}

impl<'p> PromMetric<'p> {
    pub fn from_metric_family<'a>(
        m: &'a prometheus::proto::MetricFamily,
    ) -> Result<Vec<PromMetric<'a>>, ()> {
        use prometheus::proto::MetricType;
        fn l2m(metric: &prometheus::proto::Metric) -> BTreeMap<&str, &str> {
            metric
                .get_label()
                .iter()
                .map(|lp| (lp.get_name(), lp.get_value()))
                .collect()
        }
        m.get_metric()
            .iter()
            .map(|metric| {
                Ok(match m.get_field_type() {
                    MetricType::COUNTER => PromMetric::Counter {
                        name: m.get_name(),
                        value: metric.get_counter().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::GAUGE => PromMetric::Gauge {
                        name: m.get_name(),
                        value: metric.get_gauge().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::HISTOGRAM => PromMetric::Histogram {
                        name: m.get_name(),
                        count: metric.get_histogram().get_sample_count(),
                        labels: l2m(metric),
                    },
                    _ => return Err(()),
                })
            })
            .collect()
    }

    pub fn name(&self) -> &'p str {
        match self {
            PromMetric::Counter { name, .. } => name,
            PromMetric::Gauge { name, .. } => name,
            PromMetric::Histogram { name, .. } => name,
        }
    }

    pub fn label(&self, key: &str) -> Option<&str> {
        match self {
            PromMetric::Counter { labels, .. } => labels.get(key).copied(),
            PromMetric::Gauge { labels, .. } => labels.get(key).copied(),
            PromMetric::Histogram { labels, .. } => labels.get(key).copied(),
        }
    }

    /// Get the value of this metric as an i64
    pub fn value(&self) -> f64 {
        match self {
            PromMetric::Counter { value, .. } => *value,
            PromMetric::Gauge { value, .. } => *value,
            PromMetric::Histogram { count, .. } => *count as f64,
        }
    }
}
