use std::{
    convert::TryInto,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use dataflow::SequencedCommand;
use prometheus::{
    proto::{Metric, MetricFamily, MetricType},
    Registry,
};
use repr::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::builtin::MZ_PROMETHEUS_READINGS;

use super::Message;

/// Scrapes the prometheus registry in a regular interval and sends back a [`Batch`] of metric data
/// that can be inserted into a table.
pub struct Scraper<'a> {
    interval: Duration,
    registry: &'a Registry,
    command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
    internal_tx: UnboundedSender<super::Message>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ScraperMessage {
    Shutdown,
}

struct Reading {
    name: String,
    kind: MetricType,
    labels: Vec<(String, String)>,
    value: f64,
}

fn convert_metrics_to_rows<'a, M: IntoIterator<Item = &'a Metric>>(
    name: &str,
    kind: MetricType,
    metrics: M,
) -> Vec<Reading> {
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
                COUNTER => vec![Reading {
                    name: name.to_string(),
                    kind,
                    labels,
                    value: m.get_counter().get_value(),
                }],
                // TODO: gauges, histograms, summaries and more
                _ => vec![],
            }
        })
        .collect()
}

impl<'a> Scraper<'a> {
    pub fn new(
        interval: Duration,
        registry: &'a Registry,
        command_rx: std::sync::mpsc::Receiver<ScraperMessage>,
        internal_tx: UnboundedSender<super::Message>,
    ) -> Self {
        Scraper {
            interval,
            registry,
            command_rx,
            internal_tx,
        }
    }

    /// Run forever: Scrape the metrics registry once per interval, telling the coordinator to
    /// insert the values and meta-info in internal tables.
    pub fn run(&mut self) {
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
            let _rows = metric_fams.into_iter().flat_map(|family| {
                convert_metrics_to_rows(
                    family.get_name(),
                    family.get_field_type(),
                    family.get_metric().into_iter(),
                )
            });
            self.internal_tx
                .send(Message::Broadcast(SequencedCommand::Insert {
                    id: MZ_PROMETHEUS_READINGS.id,
                    updates: vec![],
                }))
                .expect("Couldn't send");
        }
    }
}
