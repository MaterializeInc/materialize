// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::HashMap;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::bail;
use ore::metric;
use ore::metrics::{GaugeVecExt, IntGaugeVec, MetricsRegistry};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use dataflow_types::sources::{
    persistence::TimestampSourceUpdate, ExternalSourceConnector, FileSourceConnector,
    KafkaSourceConnector, KinesisSourceConnector, S3SourceConnector, SourceConnector,
};
use expr::{GlobalId, PartitionId};
use kafka_util::client::MzClientContext;
use ore::collections::CollectionExt;

use crate::coord;

const CONF_DENYLIST: &'static [&'static str] = &["statistics.interval.ms"];

#[derive(Debug)]
pub enum TimestampMessage {
    Add(GlobalId, SourceConnector),
    Drop(GlobalId),
}

/// Timestamp consumer: wrapper around source consumers that stores necessary information
/// about topics and offset for real-time consistency
struct RtTimestampConsumer {
    connector: RtTimestampConnector,
}

enum RtTimestampConnector {
    Kafka(RtKafkaConnector),
    File(RtFileConnector),
    Ocf(RtFileConnector),
    Kinesis(RtKinesisConnector),
    S3(RtS3Connector),
}

/// Data consumer for Kafka source with RT consistency
#[derive(Clone)]
struct RtKafkaConnector {
    coordination_state: Arc<TimestampingState>,
    id: GlobalId,
    topic: String,
}

/// Coordination structure for individual timestamping threads
struct TimestampingState {
    /// Flag is set when timestamping for this source has been dropped
    stop: AtomicBool,
    /// Channel through which messages can be sent to the coordinator
    coordinator_channel: mpsc::UnboundedSender<coord::Message>,
}

/// Data consumer stub for Kinesis source with RT consistency
struct RtKinesisConnector {}

/// Data consumer stub for File source with RT consistency
struct RtFileConnector {}

/// Data consumer stub for S3 source with RT consistency
struct RtS3Connector {}

/// Return the list of partition ids associated with a specific topic
fn get_kafka_partitions(
    consumer: &BaseConsumer<MzClientContext>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = consumer.fetch_metadata(Some(&topic), timeout)?;
    if meta.topics().len() == 0 {
        bail!("topic {} does not exist", topic);
    } else if meta.topics().len() > 1 {
        bail!("topic metadata had more than one result");
    }
    let meta_topic = meta.topics().into_element();
    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }
    Ok(meta_topic.partitions().iter().map(|x| x.id()).collect())
}

pub struct Timestamper {
    /// Current list of up to date sources that use a real time consistency model
    rt_sources: HashMap<GlobalId, RtTimestampConsumer>,

    /// Channel through which timestamp data updates are communicated through the coordinator
    tx: mpsc::UnboundedSender<coord::Message>,
    /// Channel through which to timestamp metadata updates are received from the coordinator
    /// (to add or remove the timestamping of a source)
    rx: std::sync::mpsc::Receiver<TimestampMessage>,

    /// Frequency at which thread should run
    timestamp_frequency: Duration,

    /// Metrics that the timestamper reports.
    metrics: Metrics,
}

#[derive(Clone)]
struct Metrics {
    max_available_offset: IntGaugeVec,
}

impl Metrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            max_available_offset: registry.register(metric!(
                name: "mz_kafka_partition_offset_max",
                help: "The high watermark for a partition, the maximum offset that we could hope to ingest",
                var_labels: ["topic", "source_id", "partition_id"],
            ))
        }
    }
}

impl Timestamper {
    pub fn new(
        frequency: Duration,
        tx: mpsc::UnboundedSender<coord::Message>,
        rx: std::sync::mpsc::Receiver<TimestampMessage>,
        registry: &MetricsRegistry,
    ) -> Self {
        info!(
            "Starting Timestamping Thread. Frequency: {} ms.",
            frequency.as_millis()
        );

        Self {
            rt_sources: HashMap::new(),
            tx,
            rx,
            timestamp_frequency: frequency,
            metrics: Metrics::register_with(registry),
        }
    }

    /// Run the update function in a loop at the specified frequency. Acquires timestamps using
    /// either the Kafka topic ground truth
    pub fn run(&mut self) {
        loop {
            thread::sleep(self.timestamp_frequency);
            let shutdown = self.update_sources();
            if shutdown {
                return;
            }
        }
    }

    /// Updates list of timestamp sources based on coordinator information. If using
    /// using the real-time timestamping logic, then maintain a list of Kafka consumers
    /// that poll topics to check how much data has been generated. If using the Kafka
    /// source timestamping logic, then keep a mapping of (name,id) to translate user-
    /// defined timestamps to GlobalIds
    fn update_sources(&mut self) -> bool {
        // First check if there are some new source that we should
        // start checking
        loop {
            match self.rx.try_recv() {
                Ok(TimestampMessage::Add(source_id, sc)) => {
                    let sc = match sc {
                        SourceConnector::External { connector, .. } => connector,
                        _ => {
                            tracing::debug!(
                                "Local source {} cannot be timestamped. Ignoring",
                                source_id
                            );
                            continue;
                        }
                    };

                    if !self.rt_sources.contains_key(&source_id) {
                        info!(
                            "Timestamping Source {} with Real Time Consistency.",
                            source_id
                        );
                        let consumer = self.create_rt_connector(source_id, sc);
                        if let Some(consumer) = consumer {
                            self.rt_sources.insert(source_id, consumer);
                        }
                    }
                }
                Ok(TimestampMessage::Drop(id)) => {
                    self.drop_source(id);
                }
                Err(TryRecvError::Empty) => return false,
                Err(TryRecvError::Disconnected) => {
                    // First, let's remove all of the threads consuming metadata
                    // from realtime Kafka sources
                    for (_, src) in self.rt_sources.iter_mut() {
                        if let RtTimestampConnector::Kafka(RtKafkaConnector {
                            coordination_state,
                            ..
                        }) = &src.connector
                        {
                            coordination_state.stop.store(true, Ordering::SeqCst);
                        }
                    }
                    return true;
                }
            }
        }
    }

    fn drop_source(&mut self, id: GlobalId) {
        info!("Dropping Timestamping for Source {}.", id);
        if let Some(RtTimestampConsumer {
            connector:
                RtTimestampConnector::Kafka(RtKafkaConnector {
                    coordination_state, ..
                }),
            ..
        }) = self.rt_sources.remove(&id)
        {
            coordination_state.stop.store(true, Ordering::SeqCst);
        }
    }

    /// Creates a RT connector
    /// TODO(rkhaitan): this function burns my eyes
    fn create_rt_connector(
        &self,
        id: GlobalId,
        sc: ExternalSourceConnector,
    ) -> Option<RtTimestampConsumer> {
        match sc {
            ExternalSourceConnector::Kafka(kc) => {
                self.create_rt_kafka_connector(id, kc)
                    .map(|connector| RtTimestampConsumer {
                        connector: RtTimestampConnector::Kafka(connector),
                    })
            }
            ExternalSourceConnector::File(fc) => {
                self.create_rt_file_connector(id, fc)
                    .map(|connector| RtTimestampConsumer {
                        connector: RtTimestampConnector::File(connector),
                    })
            }
            ExternalSourceConnector::AvroOcf(fc) => {
                self.create_rt_ocf_connector(id, fc)
                    .map(|connector| RtTimestampConsumer {
                        connector: RtTimestampConnector::Ocf(connector),
                    })
            }
            ExternalSourceConnector::Kinesis(kinc) => self
                .create_rt_kinesis_connector(id, kinc)
                .map(|connector| RtTimestampConsumer {
                    connector: RtTimestampConnector::Kinesis(connector),
                }),
            ExternalSourceConnector::S3(s3c) => {
                self.create_rt_s3_connector(id, s3c)
                    .map(|connector| RtTimestampConsumer {
                        connector: RtTimestampConnector::S3(connector),
                    })
            }
            ExternalSourceConnector::Postgres(_) => None,
            ExternalSourceConnector::PubNub(_) => None,
        }
    }

    fn create_rt_kinesis_connector(
        &self,
        _id: GlobalId,
        _kinc: KinesisSourceConnector,
    ) -> Option<RtKinesisConnector> {
        Some(RtKinesisConnector {})
    }

    fn create_rt_kafka_connector(
        &self,
        id: GlobalId,
        kc: KafkaSourceConnector,
    ) -> Option<RtKafkaConnector> {
        // These keys do not make sense for the timestamping connector, and will
        // be filtered out (fixes #6313)
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &kc.addrs.to_string());

        // TODO(guswynn): replace this when https://github.com/tokio-rs/tracing/pull/1821 is merged
        if log::log_enabled!(target: "librdkafka", log::Level::Debug) {
            debug!("Enabling 'debug' for rdkafka");
            config.set("debug", "all");
        }

        for (k, v) in &kc.config_options {
            if !CONF_DENYLIST.contains(&k.as_str()) {
                config.set(k, v);
            }
        }

        let consumer =
            match config.create_with_context::<_, BaseConsumer<MzClientContext>>(MzClientContext) {
                Ok(consumer) => consumer,
                Err(e) => {
                    error!("Failed to create Kafka Consumer {}", e);
                    return None;
                }
            };

        let connector = RtKafkaConnector {
            coordination_state: Arc::new(TimestampingState {
                stop: AtomicBool::new(false),
                coordinator_channel: self.tx.clone(),
            }),
            id,
            topic: kc.topic,
        };

        // Start metadata refresh thread
        // Default value obtained from https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        let metadata_refresh_frequency = Duration::from_millis(
            kc.config_options
                .get("topic.metadata.refresh.interval.ms")
                // Safe conversion: statement::extract_config enforces that option is a value
                // between 0 and 3600000
                .unwrap_or(&"30000".to_owned())
                .parse()
                .unwrap(),
        );

        thread::Builder::new()
            .name("rt_kafka_meta".to_string())
            .spawn({
                let connector = connector.clone();
                let metrics = self.metrics.clone();
                move || {
                    rt_kafka_metadata_fetch_loop(
                        connector,
                        consumer,
                        metadata_refresh_frequency,
                        &metrics,
                    )
                }
            })
            .unwrap();

        Some(connector)
    }

    fn create_rt_ocf_connector(
        &self,
        _id: GlobalId,
        _fc: FileSourceConnector,
    ) -> Option<RtFileConnector> {
        Some(RtFileConnector {})
    }

    fn create_rt_file_connector(
        &self,
        _id: GlobalId,
        _fc: FileSourceConnector,
    ) -> Option<RtFileConnector> {
        Some(RtFileConnector {})
    }

    fn create_rt_s3_connector(
        &self,
        _id: GlobalId,
        _fc: S3SourceConnector,
    ) -> Option<RtS3Connector> {
        Some(RtS3Connector {})
    }
}

fn rt_kafka_metadata_fetch_loop(
    c: RtKafkaConnector,
    consumer: BaseConsumer<MzClientContext>,
    wait: Duration,
    metrics: &Metrics,
) {
    debug!(
        "Starting realtime Kafka thread for {} (source {})",
        &c.topic, &c.id
    );

    let mut current_partition_count = 0;
    let mut max_available_offsets_metrics = vec![];

    while !c.coordination_state.stop.load(Ordering::SeqCst) {
        match get_kafka_partitions(&consumer, &c.topic, Duration::from_secs(30)) {
            Ok(partitions) => {
                // There cannot be more than i32 partitions
                let new_partition_count: i32 = partitions.len().try_into().unwrap();
                match new_partition_count.cmp(&current_partition_count) {
                    cmp::Ordering::Greater => {
                        let diff = new_partition_count - current_partition_count;
                        info!(
                            "Discovered {} new ({} total) kafka partitions for topic {} (source {})",
                            diff, new_partition_count, c.topic, c.id,
                        );

                        for partition in current_partition_count..new_partition_count {
                            c.coordination_state
                                .coordinator_channel
                                .send(coord::Message::AdvanceSourceTimestamp(
                                    coord::AdvanceSourceTimestamp {
                                        id: c.id,
                                        update: TimestampSourceUpdate::RealTime(
                                            PartitionId::Kafka(partition),
                                        ),
                                    },
                                ))
                                .expect(
                                    "Failed to send update to coordinator. This should not happen",
                                );
                        }
                        current_partition_count = new_partition_count;
                    }
                    cmp::Ordering::Less => {
                        info!(
                            "Ignoring decrease in partitions (from {} to {}) for topic {} (source {})",
                            current_partition_count, new_partition_count, c.topic, c.id,
                        );
                    }
                    cmp::Ordering::Equal => (),
                }
            }
            Err(e) => {
                error!(
                    "Unable to fetch kafka metadata for topic {} (source {}): {}",
                    c.topic, e, c.id
                );
            }
        }

        // Fetch the latest offset for each partition.
        //
        // TODO(benesch): Kafka supports fetching these in bulk, but
        // rust-rdkafka does not. That would save us a lot of requests on
        // large topics.
        for pid in 0..current_partition_count {
            match consumer.fetch_watermarks(&c.topic, pid, Duration::from_secs(30)) {
                Ok((_low, high)) => {
                    while max_available_offsets_metrics.len() <= usize::try_from(pid).unwrap() {
                        max_available_offsets_metrics.push(
                            metrics.max_available_offset.get_delete_on_drop_gauge(vec![
                                c.topic.clone(),
                                c.id.to_string(),
                                pid.to_string(),
                            ]),
                        );
                    }
                    max_available_offsets_metrics[usize::try_from(pid).unwrap()].set(high);
                }
                Err(e) => {
                    error!(
                        "Unable to fetch Kafka watermarks for topic {} [{}] ({}): {}",
                        c.topic, pid, c.id, e
                    );
                }
            }
        }

        // Poll once to clear any extraneous messages on this queue.
        consumer.poll(Duration::from_secs(0));

        if current_partition_count > 0 {
            thread::sleep(wait);
        } else {
            // If no partitions have been detected yet, sleep for a second rather than
            // the specified "wait" period of time, as we know that there should at least be one
            // partition
            thread::sleep(Duration::from_secs(1));
        }
    }

    debug!("Terminating realtime Kafka thread for {}", &c.topic);
}
