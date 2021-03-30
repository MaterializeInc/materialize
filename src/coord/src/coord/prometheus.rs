// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A task that scrapes materialized's prometheus metrics and sends them to our logging tables.

use std::{
    collections::HashMap,
    convert::TryInto,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use chrono::NaiveDateTime;
use prometheus::{proto::MetricType, Registry};
use repr::{Datum, Row, Timestamp};
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::{builtin::BuiltinTable, BuiltinTableUpdate};

use super::Message;
use super::{
    catalog::builtin::{MZ_PROMETHEUS_HISTOGRAMS, MZ_PROMETHEUS_METRICS, MZ_PROMETHEUS_READINGS},
    TimestampedUpdate,
};

/// Scrapes the prometheus registry in a regular interval and submits a batch of metric data to a
/// logging worker, to be inserted into a table.
pub struct Scraper<'a> {
    interval: Duration,
    retain_for: u64,
    registry: &'a Registry,
    command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
    internal_tx: UnboundedSender<super::Message>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ScraperMessage {
    Shutdown,
}

fn convert_metrics_to_value_rows<
    'a,
    M: IntoIterator<Item = &'a prometheus::proto::MetricFamily>,
>(
    timestamp: NaiveDateTime,
    families: M,
) -> (Vec<Row>, Vec<Row>) {
    let mut row_packer = Row::default();
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
            row_packer.push(Datum::from(fam.get_name()));
            row_packer.push(Datum::from(timestamp));
            row_packer.push_dict(labels.iter().copied());
            row_packer.push(Datum::from(match kind {
                MetricType::COUNTER => metric.get_counter().get_value(),
                MetricType::GAUGE => metric.get_gauge().get_value(),
                _ => unreachable!("never hit for anything other than gauges & counters"),
            }));
            rows.push(row_packer.finish_and_reuse());
        }
    }
    (rows, metadata)
}

fn convert_metrics_to_histogram_rows<
    'a,
    M: IntoIterator<Item = &'a prometheus::proto::MetricFamily>,
>(
    timestamp: NaiveDateTime,
    families: M,
) -> (Vec<Row>, Vec<Row>) {
    let mut row_packer = Row::default();
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
                    row_packer.push(Datum::from(name));
                    row_packer.push(Datum::from(timestamp));
                    row_packer.push_dict(labels.iter().copied());
                    row_packer.push(Datum::from(bucket.get_upper_bound()));
                    row_packer.push(Datum::from(bucket.get_cumulative_count() as i64));
                    rows.push(row_packer.finish_and_reuse());
                }
            }
        }
    }
    (rows, metadata)
}

fn metric_family_metadata(family: &prometheus::proto::MetricFamily) -> Row {
    Row::pack(&[
        Datum::from(family.get_name()),
        Datum::from(family.get_help()),
        Datum::from(match family.get_field_type() {
            MetricType::COUNTER => "counter",
            MetricType::GAUGE => "gauge",
            MetricType::HISTOGRAM => "histogram",
            MetricType::SUMMARY => "summary",
            MetricType::UNTYPED => "untyped",
        }),
    ])
}

impl<'a> Scraper<'a> {
    pub fn new(
        interval: Duration,
        retain_for: Duration,
        registry: &'a Registry,
        command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
        internal_tx: UnboundedSender<super::Message>,
    ) -> Self {
        let retain_for = retain_for.as_millis() as u64;
        Scraper {
            interval,
            retain_for,
            registry,
            command_rx,
            internal_tx,
        }
    }

    /// Run forever: Scrape the metrics registry once per interval, telling the coordinator to
    /// insert the values and meta-info in internal tables.
    pub fn run(&mut self) {
        let mut metadata: HashMap<Row, u64> = HashMap::new();
        loop {
            thread::sleep(self.interval);
            let now: Timestamp = UNIX_EPOCH
                .elapsed()
                .expect("system clock is recent enough")
                .as_millis()
                .try_into()
                .expect("materialized is younger than 550M years.");
            if let Ok(cmd) = self.command_rx.try_recv() {
                match cmd {
                    ScraperMessage::Shutdown => return,
                }
            }

            let timestamp = NaiveDateTime::from_timestamp(0, 0)
                + chrono::Duration::from_std(Duration::from_millis(now))
                    .expect("Couldn't convert timestamps");
            let metric_fams = self.registry.gather();

            let (value_readings, meta_value) =
                convert_metrics_to_value_rows(timestamp, metric_fams.iter());
            self.send_expiring_update(&MZ_PROMETHEUS_READINGS, value_readings);

            let (histo_readings, meta_histo) =
                convert_metrics_to_histogram_rows(timestamp, metric_fams.iter());
            self.send_expiring_update(&MZ_PROMETHEUS_HISTOGRAMS, histo_readings);

            // Find any metric metadata we need to add:
            let missing = meta_value
                .into_iter()
                .chain(meta_histo.into_iter())
                .filter(|metric| {
                    metadata
                        .insert(metric.clone(), now + self.retain_for)
                        .is_none()
                });
            self.send_metadata_update(missing, 1);

            // Expire any that can now go (I would love HashMap.drain_filter here):
            self.send_metadata_update(
                metadata
                    .iter()
                    .filter(|(_, &retention)| retention <= now)
                    .map(|(row, _)| row)
                    .cloned(),
                -1,
            );
            metadata.retain(|_, &mut retention| retention > now);
        }
    }

    fn send_expiring_update(&self, table: &BuiltinTable, updates: Vec<Row>) {
        let id = table.id;
        self.internal_tx
            .send(Message::InsertBuiltinTableUpdates(TimestampedUpdate {
                updates: updates
                    .iter()
                    .cloned()
                    .map(|row| BuiltinTableUpdate { id, row, diff: 1 })
                    .collect(),
                timestamp_offset: 0,
            }))
            .expect("Sending positive metric reading messages");
        self.internal_tx
            .send(Message::InsertBuiltinTableUpdates(TimestampedUpdate {
                updates: updates
                    .iter()
                    .cloned()
                    .map(|row| BuiltinTableUpdate { id, row, diff: -1 })
                    .collect(),
                timestamp_offset: self.retain_for,
            }))
            .expect("Sending metric reading retraction messages");
    }

    fn send_metadata_update<I: IntoIterator<Item = Row>>(&self, updates: I, diff: isize) {
        let id = MZ_PROMETHEUS_METRICS.id;
        self.internal_tx
            .send(Message::InsertBuiltinTableUpdates(TimestampedUpdate {
                updates: updates
                    .into_iter()
                    .map(|row| BuiltinTableUpdate { id, row, diff })
                    .collect(),
                timestamp_offset: self.retain_for,
            }))
            .expect("Sending metric metadata message");
    }
}
