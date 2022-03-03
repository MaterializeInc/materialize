// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A helper that scrapes materialized's prometheus metrics and produces
//! updates for the mz_metrics table.

use std::collections::HashMap;
use std::pin::Pin;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream, StreamExt};
use prometheus::proto::MetricType;
use tokio::time::{self, Duration};
use tokio_stream::wrappers::IntervalStream;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{self, SYSTEM_TIME};
use mz_repr::{Datum, Diff, Row};

use crate::catalog::builtin::{
    BuiltinTable, MZ_PROMETHEUS_HISTOGRAMS, MZ_PROMETHEUS_METRICS, MZ_PROMETHEUS_READINGS,
};
use crate::catalog::BuiltinTableUpdate;
use crate::coord::{LoggingConfig, Message, TimestampedUpdate};

/// Scrapes the prometheus registry when asked and produces a batch of metric
/// data that can be inserted into the built-in `mz_metrics` table.
pub struct Scraper {
    interval: Option<Duration>,
    retain_for: u64,
    registry: MetricsRegistry,
    metadata: HashMap<Row, u64>,
}

fn convert_metrics_to_value_rows<
    'a,
    M: IntoIterator<Item = &'a prometheus::proto::MetricFamily>,
>(
    timestamp: DateTime<Utc>,
    families: M,
) -> (Vec<Row>, Vec<Row>) {
    let mut row_buf = Row::default();
    let mut rows: Vec<Row> = vec![];
    let mut metadata: Vec<Row> = vec![];

    for fam in families {
        let kind = fam.get_field_type();
        if kind != MetricType::COUNTER && kind != MetricType::GAUGE {
            continue;
        }

        metadata.push(metric_family_metadata(&fam));
        for metric in fam.get_metric() {
            let labels: Vec<_> = metric
                .get_label()
                .into_iter()
                .map(|pair| (pair.get_name(), Datum::from(pair.get_value())))
                .collect();
            let mut row_packer = row_buf.packer();
            row_packer.push(Datum::from(fam.get_name()));
            row_packer.push(Datum::from(timestamp));
            row_packer.push_dict(labels.iter().copied());
            row_packer.push(Datum::from(match kind {
                MetricType::COUNTER => metric.get_counter().get_value(),
                MetricType::GAUGE => metric.get_gauge().get_value(),
                _ => unreachable!("never hit for anything other than gauges & counters"),
            }));
            rows.push(row_buf.clone());
        }
    }
    (rows, metadata)
}

fn convert_metrics_to_histogram_rows<
    'a,
    M: IntoIterator<Item = &'a prometheus::proto::MetricFamily>,
>(
    timestamp: DateTime<Utc>,
    families: M,
) -> (Vec<Row>, Vec<Row>) {
    let mut row_buf = Row::default();
    let mut rows: Vec<Row> = vec![];
    let mut metadata: Vec<Row> = vec![];

    for fam in families {
        let name = fam.get_name();
        if fam.get_field_type() == MetricType::HISTOGRAM {
            metadata.push(metric_family_metadata(&fam));
            for metric in fam.get_metric() {
                let labels: Vec<_> = metric
                    .get_label()
                    .into_iter()
                    .map(|pair| (pair.get_name(), Datum::from(pair.get_value())))
                    .collect();
                for bucket in metric.get_histogram().get_bucket() {
                    let mut row_packer = row_buf.packer();
                    row_packer.push(Datum::from(name));
                    row_packer.push(Datum::from(timestamp));
                    row_packer.push_dict(labels.iter().copied());
                    row_packer.push(Datum::from(bucket.get_upper_bound()));
                    row_packer.push(Datum::from(bucket.get_cumulative_count() as i64));
                    rows.push(row_buf.clone());
                }
            }
        }
    }
    (rows, metadata)
}

fn metric_family_metadata(family: &prometheus::proto::MetricFamily) -> Row {
    Row::pack(&[
        Datum::from(family.get_name()),
        Datum::from(match family.get_field_type() {
            MetricType::COUNTER => "counter",
            MetricType::GAUGE => "gauge",
            MetricType::HISTOGRAM => "histogram",
            MetricType::SUMMARY => "summary",
            MetricType::UNTYPED => "untyped",
        }),
        Datum::from(family.get_help()),
    ])
}

fn add_expiring_update(
    table: &BuiltinTable,
    updates: Vec<Row>,
    retain_for: u64,
    out: &mut Vec<TimestampedUpdate>,
) {
    // NB: This makes sure to send both records in the same message so we can
    // persist them atomically. Otherwise, we may end up with permanent orphans
    // if a restart/crash happens at the wrong time.
    let id = table.id;
    out.push(TimestampedUpdate {
        updates: updates
            .iter()
            .cloned()
            .map(|row| BuiltinTableUpdate { id, row, diff: 1 })
            .collect(),
        timestamp_offset: 0,
    });
    out.push(TimestampedUpdate {
        updates: updates
            .iter()
            .cloned()
            .map(|row| BuiltinTableUpdate { id, row, diff: -1 })
            .collect(),
        timestamp_offset: retain_for,
    });
}

fn add_metadata_update<I: IntoIterator<Item = Row>>(
    updates: I,
    diff: Diff,
    out: &mut Vec<TimestampedUpdate>,
) {
    let id = MZ_PROMETHEUS_METRICS.id;
    out.push(TimestampedUpdate {
        updates: updates
            .into_iter()
            .map(|row| BuiltinTableUpdate { id, row, diff })
            .collect(),
        timestamp_offset: 0,
    });
}

impl Scraper {
    /// Constructs a new metrics scraper for the specified registry.
    ///
    /// The logging configuration specifies what metrics to scrape and how long
    /// to retain them for. If the logging configuration is none, scraping is
    /// disabled.
    pub fn new(
        logging_config: Option<&LoggingConfig>,
        registry: MetricsRegistry,
    ) -> Result<Scraper, anyhow::Error> {
        let (interval, retain_for) = match logging_config {
            Some(config) => {
                let interval = config.metrics_scraping_interval;
                let retain_for = u64::try_from(config.retain_readings_for.as_millis())
                    .map_err(|_| anyhow!("scraper retention duration does not fit in an i64"))?;
                (interval, retain_for)
            }
            None => (None, 0),
        };
        Ok(Scraper {
            interval,
            retain_for,
            registry,
            metadata: HashMap::new(),
        })
    }

    /// Produces a stream that yields a `Message::ScrapeMetrics` at the desired
    /// scrape frequency.
    ///
    /// If the scraper is disabled, this stream will yield no items.
    pub fn tick_stream(&self) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        match self.interval {
            None => stream::empty().boxed(),
            Some(interval) => IntervalStream::new(time::interval(interval))
                .map(|_| Message::ScrapeMetrics)
                .boxed(),
        }
    }

    /// Scrapes the metrics once, producing a batch of updates that should be
    /// inserted into the `mz_metrics` table.
    pub fn scrape_once(&mut self) -> Vec<TimestampedUpdate> {
        let now = SYSTEM_TIME();
        let timestamp = now::to_datetime(now);

        let metric_fams = self.registry.gather();

        let mut out = vec![];

        let (value_readings, meta_value) =
            convert_metrics_to_value_rows(timestamp, metric_fams.iter());
        add_expiring_update(
            &MZ_PROMETHEUS_READINGS,
            value_readings,
            self.retain_for,
            &mut out,
        );

        let (histo_readings, meta_histo) =
            convert_metrics_to_histogram_rows(timestamp, metric_fams.iter());
        add_expiring_update(
            &MZ_PROMETHEUS_HISTOGRAMS,
            histo_readings,
            self.retain_for,
            &mut out,
        );

        // Find any metric metadata we need to add:
        let retain_for = self.retain_for;
        let missing = meta_value
            .into_iter()
            .chain(meta_histo.into_iter())
            .filter(|metric| {
                self.metadata
                    .insert(metric.clone(), now + retain_for)
                    .is_none()
            });
        add_metadata_update(missing, 1, &mut out);

        // Expire any that can now go (I would love HashMap.drain_filter here):
        add_metadata_update(
            self.metadata
                .iter()
                .filter(|(_, &retention)| retention <= now)
                .map(|(row, _)| row)
                .cloned(),
            -1,
            &mut out,
        );
        self.metadata.retain(|_, &mut retention| retention > now);

        out
    }
}
