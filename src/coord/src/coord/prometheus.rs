// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    convert::TryInto,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use dataflow::{
    logging::materialized::{MaterializedEvent, Metric, MetricReading},
    SequencedCommand,
};
use prometheus::{proto::MetricType, Registry};
use repr::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use super::Message;

/// Scrapes the prometheus registry in a regular interval and sends back a [`Batch`] of metric data
/// that can be inserted into a table.
pub struct Scraper<'a> {
    interval: Duration,
    retain_for: Duration,
    registry: &'a Registry,
    command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
    internal_tx: UnboundedSender<super::Message>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ScraperMessage {
    Shutdown,
}

fn convert_metrics_to_rows<'a, M: IntoIterator<Item = &'a prometheus::proto::Metric>>(
    kind: MetricType,
    metrics: M,
) -> Vec<MetricReading> {
    metrics
        .into_iter()
        .flat_map(|m| {
            use MetricType::*;
            let labels = m
                .get_label()
                .into_iter()
                .map(|pair| (pair.get_name().to_owned(), pair.get_value().to_owned()))
                .collect::<Vec<(String, String)>>();
            match kind {
                COUNTER => Some(MetricReading::new(labels, m.get_counter().get_value())),
                GAUGE => Some(MetricReading::new(labels, m.get_gauge().get_value())),
                // TODO: destructure histograms & summaries in a meaningful way.
                _ => None,
            }
        })
        .collect()
}

impl<'a> Scraper<'a> {
    pub fn new(
        interval: Duration,
        retain_for: Duration,
        registry: &'a Registry,
        command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
        internal_tx: UnboundedSender<super::Message>,
    ) -> Self {
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
        let retain_for = self.retain_for.as_millis() as u64;
        loop {
            thread::sleep(self.interval);
            let now: Timestamp = UNIX_EPOCH
                .elapsed()
                .expect("system clock before 1970")
                .as_millis()
                .try_into()
                .expect("materialize has existed for >500M years");
            if let Ok(cmd) = self.command_rx.try_recv() {
                match cmd {
                    ScraperMessage::Shutdown => return,
                }
            }

            let metric_fams = self.registry.gather();
            let metrics = metric_fams
                .into_iter()
                .map(|family| {
                    Metric::new(
                        family.get_name().to_string(),
                        family.get_field_type().into(),
                        family.get_help().to_string(),
                        convert_metrics_to_rows(
                            family.get_field_type(),
                            family.get_metric().into_iter(),
                        ),
                    )
                })
                .collect();
            self.internal_tx
                .send(Message::Broadcast(SequencedCommand::ReportMaterializedLog(
                    MaterializedEvent::PrometheusMetrics {
                        timestamp: now,
                        retain_for,
                        metrics,
                    },
                )))
                .expect("Couldn't send");
        }
    }
}
