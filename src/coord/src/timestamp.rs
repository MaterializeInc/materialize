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
use std::ops::Deref;
use std::panic;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context};
use itertools::Itertools;
use lazy_static::lazy_static;
use mz_avro::schema::Schema;
use mz_avro::types::Value;
use ore::metric;
use ore::metrics::{GaugeVecExt, IntGaugeVec, MetricsRegistry};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use dataflow_types::sources::{
    encoding::DataEncoding,
    persistence::{Consistency, TimestampSourceUpdate},
    DebeziumMode, ExternalSourceConnector, FileSourceConnector, KafkaSourceConnector,
    KinesisSourceConnector, MzOffset, S3SourceConnector, SourceConnector, SourceEnvelope,
};
use expr::{GlobalId, PartitionId};
use kafka_util::client::MzClientContext;
use ore::collections::CollectionExt;

use crate::coord;

const CONF_DENYLIST: &'static [&'static str] = &["statistics.interval.ms"];

lazy_static! {
    /// Key schema for Debezium consistency sources.
    static ref DEBEZIUM_TRX_SCHEMA_KEY: Schema = {
        r#"{
          "name": "io.debezium.connector.common.TransactionMetadataKey",
          "type": "record",
          "fields": [{"name": "id", "type": "string"}]
        }"#.parse().unwrap()
    };

    /// Value schema for Debezium consistency sources.
    static ref DEBEZIUM_TRX_SCHEMA_VALUE: Schema = {
        r#"{
          "type": "record",
          "name": "TransactionMetadataValue",
          "namespace": "io.debezium.connector.common",
          "fields": [
            {"name": "status", "type": "string"},
            {"name": "id", "type": "string"},
            {"name": "event_count", "type": ["null", "long"], "default": null},
            {
              "name": "data_collections",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "ConnectDefault",
                    "namespace": "io.confluent.connect.Avro",
                    "fields": [
                      {"name": "data_collection", "type": "string"},
                      {"name": "event_count", "type": "long"}
                    ]
                  }
                }
              ],
              "default": null
            }
          ],
          "connect.name": "io.debezium.connector.common.TransactionMetadataValue"
        }"#.parse().unwrap()
    };
}

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

enum ByoTimestampConnector {
    Kafka(ByoKafkaConnector),
}

// List of possible encoding types
enum ValueEncoding {
    Bytes(Vec<u8>),
}

/// Timestamp consumer: wrapper around source consumers that stores necessary information
/// about topics and offset for byo consistency
struct ByoTimestampConsumer {
    /// Source Connector
    connector: ByoTimestampConnector,
    /// The name of the source with which this connector is associated
    ///
    /// * For kafka this is the topic
    /// * For file types this is the file name
    source_name: String,
    /// The format of the connector
    envelope: ConsistencyFormatting,
    /// The max assigned timestamp.
    last_ts: u64,
    /// The max offset for which a timestamp has been assigned
    last_offset: MzOffset,
}

/// Supported format/envelope pairs for consistency topic decoding
enum ConsistencyFormatting {
    /// The formatting of this consistency source follows the
    /// Debezium Avro format
    DebeziumAvro,
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

/// Data consumer for Kafka source with BYO consistency
struct ByoKafkaConnector {
    consumer: BaseConsumer<MzClientContext>,

    /// Offset of the last consistency record we received and processed. We use
    /// this to filter out messages that we have seen already.
    ///
    /// Session timeouts, together with consumer-group re-arrangement, combined
    /// with the fact that we don't commit read offsets for the BYO consumer
    /// can lead to cases where an existing consumer starts reading the
    /// consistency topic from the beginning, after a connection outage.
    last_offset: Option<i64>,
}

impl ByoKafkaConnector {
    fn new(consumer: BaseConsumer<MzClientContext>) -> ByoKafkaConnector {
        ByoKafkaConnector {
            consumer,
            last_offset: None,
        }
    }
}

/// Data consumer stub for Kinesis source with RT consistency
struct RtKinesisConnector {}

/// Data consumer stub for File source with RT consistency
struct RtFileConnector {}

/// Data consumer stub for S3 source with RT consistency
struct RtS3Connector {}

fn byo_query_source(
    consumer: &mut ByoTimestampConsumer,
) -> Result<Vec<ValueEncoding>, anyhow::Error> {
    let mut messages = vec![];
    match &mut consumer.connector {
        ByoTimestampConnector::Kafka(kafka_connector) => {
            while let Some(message) = kafka_get_next_message(&mut kafka_connector.consumer)? {
                if let Some(last_offset) = kafka_connector.last_offset {
                    if message.offset() <= last_offset {
                        // it would probably be nicer to print the decoded
                        // message here. But we don't know the message format
                        // and I don't necessarily want to pipe Kafka specifics
                        // (offsets) to the decoding part.
                        debug!(
                            "Received BYO consistency record that we have received before: {:?}",
                            message
                        );
                        continue;
                    }
                }
                kafka_connector.last_offset = Some(message.offset());

                match message.payload() {
                    Some(payload) => {
                        messages.push(ValueEncoding::Bytes(payload.to_vec()));
                    }
                    None => {
                        bail!("unexpected null payload");
                    }
                }
            }
        }
    }
    Ok(messages)
}

/// Polls a message from a Kafka Source
fn kafka_get_next_message(
    consumer: &mut BaseConsumer<MzClientContext>,
) -> Result<Option<BorrowedMessage>, anyhow::Error> {
    if let Some(result) = consumer.poll(Duration::from_millis(60)) {
        match result {
            Ok(message) => Ok(Some(message)),

            Err(err) => {
                bail!("Failed to process message {}", err);
            }
        }
    } else {
        Ok(None)
    }
}

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

    /// Current list of up to date sources that use a BYO consistency model
    byo_sources: HashMap<GlobalId, ByoTimestampConsumer>,

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

/// Implements the byo timestamping logic
///
/// If the partition count remains the same:
/// A new timestamp should be
/// 1) strictly greater than the last timestamp in this partition
/// 2) greater or equal to all the timestamps that have been assigned so far across all partitions
/// If the partition count increases:
/// A new timestamp should be:
/// 1) strictly greater than the last timestamp
/// This is necessary to guarantee that this timestamp *could not have been closed yet*
///
/// Supports two envelopes: None and Debezium. Currentlye compatible with Debezium format 1.1
fn update_source_timestamps(
    id: &GlobalId,
    tx: &mpsc::UnboundedSender<coord::Message>,
    byo_consumer: &mut ByoTimestampConsumer,
) -> Result<(), anyhow::Error> {
    let messages = byo_query_source(byo_consumer)?;
    match byo_consumer.envelope {
        ConsistencyFormatting::DebeziumAvro => {
            for msg in messages {
                let ValueEncoding::Bytes(msg) = msg;
                // The first 5 bytes are reserved for the schema id/schema registry information
                let mut bytes = &msg[5..];
                let res = mz_avro::from_avro_datum(&DEBEZIUM_TRX_SCHEMA_VALUE, &mut bytes);
                match res {
                    Err(_) => {
                        // This was a key message, can safely ignore it
                        // TODO (#6671): raise error on failure to parse
                        continue;
                    }
                    Ok(record) => {
                        generate_ts_updates_from_debezium(&id, tx, byo_consumer, record)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Extracts Materialize timestamp updates from a Debezium consistency record.
fn generate_ts_updates_from_debezium(
    id: &GlobalId,
    tx: &mpsc::UnboundedSender<coord::Message>,
    byo_consumer: &mut ByoTimestampConsumer,
    value: Value,
) -> Result<(), anyhow::Error> {
    if let Value::Record(record) = value {
        let results =
            parse_debezium(record).with_context(|| format!("Failed to parse debezium record"))?;

        // Results are only returned when the record's status type is END
        if let Some(results) = results {
            byo_consumer.last_ts += 1;
            for (topic, count) in results {
                // Debezium topics are formatted as "server_name.database.topic", but
                // entries in data_collection do not contain server_name
                // so we discard it before doing the comparison.
                // We check for both here
                // TODO(): possible performance issue here?
                let parsed_source_name = byo_consumer.source_name.split('.').skip(1).join(".");
                if byo_consumer.source_name == topic.trim() || parsed_source_name == topic.trim() {
                    byo_consumer.last_offset.offset += count;
                    // Debezium consistency topic should only work for single-partition
                    // topics
                    tx.send(coord::Message::AdvanceSourceTimestamp(
                        coord::AdvanceSourceTimestamp {
                            id: *id,
                            update: TimestampSourceUpdate::BringYourOwn(
                                match byo_consumer.connector {
                                    ByoTimestampConnector::Kafka(_) => PartitionId::Kafka(0),
                                },
                                byo_consumer.last_ts,
                                byo_consumer.last_offset,
                            ),
                        },
                    ))
                    .expect("Failed to send update to coordinator");
                }
            }
        }
    } else {
        bail!("Expected record type for value, got {:?}", value);
    }

    Ok(())
}

/// A debezium record contains a set of update counts for each topic that the transaction
/// updated. This function extracts the set of (topic, update_count) as a vector if
/// processing an END message. It returns NONE otherwise.
fn parse_debezium(
    record: Vec<(String, Value)>,
) -> Result<Option<Vec<(String, i64)>>, anyhow::Error> {
    let mut collections = vec![];
    let mut event_count = 0;
    for (key, value) in record {
        match (key.as_str(), value) {
            ("data_collections", Value::Union { inner: value, .. }) => match *value {
                Value::Array(items) => {
                    for v in items {
                        match v {
                            Value::Record(item) => {
                                let mut collection: Option<String> = None;
                                let mut event_count: Option<i64> = None;
                                for (k, v) in item {
                                    match (k.as_str(), v) {
                                        ("data_collection", Value::String(data)) => {
                                            collection = Some(data)
                                        }
                                        ("data_collection", v) => {
                                            bail!(
                                                "Expected string for field 'data_collection', got {:?}",
                                                v
                                            );
                                        }
                                        ("event_count", Value::Long(e)) => event_count = Some(e),
                                        ("event_count", v) => {
                                            bail!(
                                                "Expected long for field 'event_count', got {:?}",
                                                v
                                            );
                                        }
                                        (_, _) => (),
                                    }
                                }
                                match (collection, event_count) {
                                    (Some(c), Some(e)) => collections.push((c, e)),
                                    (c, w) => {
                                        bail!(
                                        "Missing count or collection name. Parsed collection={:?}, event_count={:?}",
                                        c, w);
                                    }
                                }
                            }
                            _ => {
                                bail!("Record expected, got {:?}", v);
                            }
                        }
                    }
                }
                _ => {
                    bail!(
                        "Expected Array for field 'data_collections', got {:?}",
                        value
                    );
                }
            },
            ("data_collections", v) => {
                bail!("Expection Union for field 'data_collections', got {:?}", v);
            }
            ("event_count", Value::Union { inner: value, .. }) => match *value {
                Value::Long(e) => event_count = e,
                _ => {
                    bail!("Expected Long for field 'event_count', got {:?}", value);
                }
            },
            ("event_count", v) => {
                bail!("Expected Union for field 'event_count', got {:?}", v);
            }
            ("status", Value::String(status)) => match status.as_str() {
                "BEGIN" => return Ok(None),
                "END" => (),
                _ => bail!(
                    "Expect 'BEGIN' or 'END' for field 'status', got {:?}",
                    status
                ),
            },
            ("status", v) => {
                bail!("Expected String for field 'status', got {:?}", v);
            }
            (_, _) => (),
        }
    }
    if event_count < 0 {
        bail!("Expected non-negative event count, got '{}'", event_count);
    }
    if collections.iter().map(|(_, c)| c).sum::<i64>() != event_count {
        bail!(
            "Event count '{:?}' does not match parsed collections: {:?}",
            event_count,
            collections
        );
    }
    Ok(Some(collections))
}

/// This function determines the expected format of the consistency metadata as a function
/// of the encoding and the envelope of the source.
/// Specifically:
/// 1) an OCF file source with a Debezium envelope will expect an OCF Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
/// 2) any other file source with a Debezium envelope will expect an Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
fn identify_consistency_format(_enc: DataEncoding, env: SourceEnvelope) -> ConsistencyFormatting {
    if let SourceEnvelope::Debezium(_, DebeziumMode::Plain) = env {
        ConsistencyFormatting::DebeziumAvro
    } else {
        panic!("BYO timestamping for non-Debezium sources not supported!");
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
            byo_sources: HashMap::new(),
            tx,
            rx,
            timestamp_frequency: frequency,
            metrics: Metrics::register_with(registry),
        }
    }

    /// Run the update function in a loop at the specified frequency. Acquires timestamps using
    /// either the Kafka topic ground truth
    /// TODO(ncrooks): move to thread local BYO implementation
    pub fn run(&mut self) {
        loop {
            thread::sleep(self.timestamp_frequency);
            let shutdown = self.update_sources();
            if shutdown {
                return;
            } else {
                self.update_byo_timestamp();
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
                    let (sc, enc, env, cons) = if let SourceConnector::External {
                        connector,
                        encoding,
                        envelope,
                        consistency,
                        ts_frequency: _,
                        timeline: _,
                        persist: _,
                    } = sc
                    {
                        (connector, encoding, envelope, consistency)
                    } else {
                        tracing::debug!(
                            "Local source {} cannot be timestamped. Ignoring",
                            source_id
                        );
                        continue;
                    };

                    if !self.rt_sources.contains_key(&source_id)
                        && !self.byo_sources.contains_key(&source_id)
                    {
                        // Did not know about source, must update
                        match cons {
                            Consistency::RealTime => {
                                info!(
                                    "Timestamping Source {} with Real Time Consistency.",
                                    source_id
                                );
                                let consumer = self.create_rt_connector(source_id, sc);
                                if let Some(consumer) = consumer {
                                    self.rt_sources.insert(source_id, consumer);
                                }
                            }
                            Consistency::BringYourOwn(consistency_topic) => {
                                info!("Timestamping Source {} with BYO Consistency. Consistency Source: {:?}.",
                                      source_id, consistency_topic);
                                let consumer = self.create_byo_connector(
                                    source_id,
                                    sc,
                                    enc.value(),
                                    env,
                                    consistency_topic.topic,
                                );
                                if let Some(consumer) = consumer {
                                    self.byo_sources.insert(source_id, consumer);
                                }
                            }
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
        self.byo_sources.remove(&id);
    }

    /// Iterate over each source, updating BYO timestamps
    ///
    /// If an error is encountered while reading the sources, log an error message
    /// and remove that source of BYO timestamps
    fn update_byo_timestamp(&mut self) {
        let mut invalid_byo_sources: Vec<GlobalId> = vec![];
        for (id, byo_consumer) in self.byo_sources.iter_mut() {
            // Get the next set of messages from the Consistency topic
            if let Err(err) = update_source_timestamps(id, &self.tx, byo_consumer) {
                error!(
                    "Failed to correctly parse messages from source {}; {}",
                    id, err
                );
                invalid_byo_sources.push(*id);
            }
        }

        for id in invalid_byo_sources {
            self.drop_source(id);
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

    /// Creates a BYO connector
    fn create_byo_connector(
        &self,
        id: GlobalId,
        sc: ExternalSourceConnector,
        enc: DataEncoding,
        env: SourceEnvelope,
        timestamp_topic: String,
    ) -> Option<ByoTimestampConsumer> {
        match sc {
            ExternalSourceConnector::Kafka(kc) => {
                let topic = kc.topic.clone();
                match self.create_byo_kafka_connector(id, &kc, timestamp_topic) {
                    Some(connector) => Some(ByoTimestampConsumer {
                        source_name: topic,
                        connector: ByoTimestampConnector::Kafka(connector),
                        envelope: identify_consistency_format(enc, env),
                        last_ts: 0,
                        last_offset: MzOffset { offset: 0 },
                    }),
                    None => None,
                }
            }
            ExternalSourceConnector::AvroOcf(_) => None, // BYO is not supported for OCF sources
            ExternalSourceConnector::File(_) => None, // BYO is not supported for plain file sources
            ExternalSourceConnector::Kinesis(_) => None, // BYO is not supported for Kinesis sources
            ExternalSourceConnector::S3(_) => None,   // BYO is not supported for s3 sources
            ExternalSourceConnector::Postgres(_) => None, // BYO is not supported for postgres sources
            ExternalSourceConnector::PubNub(_) => None,   // BYO is not supported for pubnub sources
        }
    }

    fn create_byo_kafka_connector(
        &self,
        id: GlobalId,
        kc: &KafkaSourceConnector,
        timestamp_topic: String,
    ) -> Option<ByoKafkaConnector> {
        let mut config = ClientConfig::new();
        config
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &kc.addrs.to_string());

        let group_id_prefix = kc.group_id_prefix.clone().unwrap_or_else(String::new);
        config.set(
            "group.id",
            &format!(
                "{}materialize-byo-{}-{}",
                group_id_prefix, &timestamp_topic, id
            ),
        );

        for (k, v) in &kc.config_options {
            if !CONF_DENYLIST.contains(&k.as_str()) {
                config.set(k, v);
            }
        }

        match config.create_with_context(MzClientContext) {
            Ok(consumer) => {
                let consumer = ByoKafkaConnector::new(consumer);
                consumer.consumer.subscribe(&[&timestamp_topic]).unwrap();

                match get_kafka_partitions(
                    &consumer.consumer,
                    &timestamp_topic,
                    Duration::from_secs(5),
                )
                .as_ref()
                .map(Deref::deref)
                {
                    Ok([]) => {
                        warn!(
                            "Consistency topic {} does not exist; assuming it will exist soon",
                            timestamp_topic
                        );
                        Some(consumer)
                    }
                    Ok([_]) => Some(consumer),
                    Ok(partitions) => {
                        error!(
                            "Consistency topic should contain a single partition. Contains {}",
                            partitions.len(),
                        );
                        None
                    }
                    Err(e) => {
                        warn!(
                            "Unable to fetch metadata about consistency topic {}; \
                             assuming it exists with one partition (error: {})",
                            timestamp_topic, e
                        );
                        Some(consumer)
                    }
                }
            }
            Err(e) => {
                error!("Could not create a Kafka consumer. Error: {}", e);
                None
            }
        }
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
