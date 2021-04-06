// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;
use std::panic;
use std::path::PathBuf;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::bail;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{debug, error, info, log_enabled, warn};
use mz_avro::schema::Schema;
use mz_avro::types::Value;
use prometheus::{register_int_gauge_vec, IntGaugeVec};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio::sync::mpsc;

use dataflow::source::read_file_task;
use dataflow::source::FileReadStyle;
use dataflow_types::{
    AvroOcfEncoding, Consistency, DataEncoding, DebeziumMode, ExternalSourceConnector,
    FileSourceConnector, KafkaSourceConnector, KinesisSourceConnector, MzOffset, S3SourceConnector,
    SourceConnector, SourceEnvelope, TimestampSourceUpdate,
};
use expr::{GlobalId, PartitionId};
use ore::collections::CollectionExt;

use crate::coord;

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

    static ref MAX_AVAILABLE_OFFSET: IntGaugeVec = register_int_gauge_vec!(
        "mz_kafka_partition_offset_max",
        "The high watermark for a partition, the maximum offset that we could hope to ingest",
        &["topic", "source_id", "partition_id"]
    ).unwrap();
}

#[derive(Debug)]
pub enum TimestampMessage {
    Add(GlobalId, SourceConnector),
    Drop(GlobalId),
    Shutdown,
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
    Ocf(ByoFileConnector<Value, anyhow::Error>),
    // Kinesis and S3 are not supported
}

// List of possible encoding types
enum ValueEncoding {
    Bytes(Vec<u8>),
    Avro(Value),
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
    /// The formatting of this consistency source follows the
    /// Debezium AvroOCF format
    DebeziumOcf,
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

use std::fmt::Display;

/// Data consumer for Kafka source with BYO consistency
struct ByoKafkaConnector {
    consumer: BaseConsumer,
}

impl ByoKafkaConnector {
    fn new(consumer: BaseConsumer) -> ByoKafkaConnector {
        ByoKafkaConnector { consumer }
    }
}

/// Data consumer stub for Kinesis source with RT consistency
struct RtKinesisConnector {}

/// Data consumer stub for File source with RT consistency
struct RtFileConnector {}

/// Data consumer stub for S3 source with RT consistency
struct RtS3Connector {}

/// Data consumer stub for File source with BYO consistency
struct ByoFileConnector<Out, Err> {
    stream: Receiver<Result<Out, Err>>,
}

fn byo_query_source(consumer: &mut ByoTimestampConsumer) -> Vec<ValueEncoding> {
    let mut messages = vec![];
    match &mut consumer.connector {
        ByoTimestampConnector::Kafka(kafka_connector) => {
            while let Some(payload) = kafka_get_next_message(&mut kafka_connector.consumer) {
                messages.push(ValueEncoding::Bytes(payload));
            }
        }
        ByoTimestampConnector::Ocf(file_consumer) => {
            while let Some(payload) = file_get_next_message(file_consumer) {
                messages.push(ValueEncoding::Avro(payload));
            }
        }
    }
    messages
}

/// Returns the next message of a stream, or None if no such message exists
fn file_get_next_message<Out, Err>(file_consumer: &mut ByoFileConnector<Out, Err>) -> Option<Out>
where
    Err: Display,
{
    match file_consumer.stream.try_recv() {
        Ok(Ok(record)) => Some(record),
        Ok(Err(e)) => {
            error!("Failed to read file for timestamping: {}", e);
            None
        }
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => None,
    }
}

/// Polls a message from a Kafka Source
fn kafka_get_next_message(consumer: &mut BaseConsumer) -> Option<Vec<u8>> {
    if let Some(result) = consumer.poll(Duration::from_millis(60)) {
        match result {
            Ok(message) => match message.payload() {
                Some(p) => Some(p.to_vec()),
                None => {
                    error!("unexpected null payload");
                    None
                }
            },
            Err(err) => {
                error!("Failed to process message {}", err);
                None
            }
        }
    } else {
        None
    }
}

/// Return the list of partition ids associated with a specific topic
fn get_kafka_partitions(
    consumer: &BaseConsumer,
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
}

/// Extracts Materialize timestamp updates from a Debezium consistency record.
fn generate_ts_updates_from_debezium(
    id: &GlobalId,
    tx: &mpsc::UnboundedSender<coord::Message>,
    byo_consumer: &mut ByoTimestampConsumer,
    value: Value,
) {
    if let Value::Record(record) = value {
        // All entries in the transaction should have the same timestamp
        let results = parse_debezium(record);
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
                                    ByoTimestampConnector::Ocf(_) => PartitionId::File,
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
    }
}

/// A debezium record contains a set of update counts for each topic that the transaction
/// updated. This function extracts the set of (topic, update_count) as a vector if
/// processing an END message. It returns NONE otherwise.
fn parse_debezium(record: Vec<(String, Value)>) -> Option<Vec<(String, i64)>> {
    let mut result = vec![];
    for (key, value) in record {
        if key == "status" {
            if let Value::String(status) = value {
                if status == "BEGIN" {
                    return None;
                }
            }
        } else if key == "data_collections" {
            if let Value::Union { inner: value, .. } = value {
                if let Value::Array(items) = *value {
                    for v in items {
                        if let Value::Record(item) = v {
                            let mut value: String = String::new();
                            let mut write_count = 0;
                            for (k, v) in item {
                                if k == "data_collection" {
                                    if let Value::String(data) = v {
                                        value = data;
                                    } else {
                                        panic!("Incorrect AVRO format. String expected");
                                    }
                                } else if k == "event_count" {
                                    if let Value::Long(e) = v {
                                        write_count = e;
                                    } else {
                                        panic!("Incorrect AVRO format. Long expected");
                                    }
                                }
                            }
                            if !value.is_empty() {
                                result.push((value, write_count));
                            }
                        } else {
                            error!("Incorrect AVRO format. Record expected");
                        }
                    }
                }
            } else {
                error!(
                    "Incorrect AVRO format. Union of Null/Array expected {:?}",
                    value
                );
            }
        }
    }
    Some(result)
}

/// This function determines the expected format of the consistency metadata as a function
/// of the encoding and the envelope of the source.
/// Specifically:
/// 1) an OCF file source with a Debezium envelope will expect an OCF Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
/// 2) any other file source with a Debezium envelope will expect an Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
fn identify_consistency_format(enc: DataEncoding, env: SourceEnvelope) -> ConsistencyFormatting {
    if let SourceEnvelope::Debezium(_, DebeziumMode::Plain) = env {
        if let DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema: _ }) = enc {
            ConsistencyFormatting::DebeziumOcf
        } else {
            ConsistencyFormatting::DebeziumAvro
        }
    } else {
        panic!("BYO timestamping for non-Debezium sources not supported!");
    }
}

impl Timestamper {
    pub fn new(
        frequency: Duration,
        tx: mpsc::UnboundedSender<coord::Message>,
        rx: std::sync::mpsc::Receiver<TimestampMessage>,
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
        }
    }

    /// Run the update function in a loop at the specified frequency. Acquires timestamps using
    /// either the Kafka topic ground truth
    /// TODO(ncrooks): move to thread local BYO implementation
    pub fn update(&mut self) {
        loop {
            thread::sleep(self.timestamp_frequency);
            let shutdown = self.update_sources();
            if shutdown {
                break;
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
        while let Ok(update) = self.rx.try_recv() {
            match update {
                TimestampMessage::Add(source_id, sc) => {
                    let (sc, enc, env, cons) = if let SourceConnector::External {
                        connector,
                        encoding,
                        envelope,
                        consistency,
                        ts_frequency: _,
                    } = sc
                    {
                        (connector, encoding, envelope, consistency)
                    } else {
                        log::debug!("Local source {} cannot be timestamped. Ignoring", source_id);
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
                                info!("Timestamping Source {} with BYO Consistency. Consistency Source: {}.", source_id, consistency_topic);
                                let consumer = self.create_byo_connector(
                                    source_id,
                                    sc,
                                    enc,
                                    env,
                                    consistency_topic,
                                );
                                if let Some(consumer) = consumer {
                                    self.byo_sources.insert(source_id, consumer);
                                }
                            }
                        }
                    }
                }
                TimestampMessage::Drop(id) => {
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
                TimestampMessage::Shutdown => {
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
        false
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
    fn update_byo_timestamp(&mut self) {
        for (id, byo_consumer) in &mut self.byo_sources {
            // Get the next set of messages from the Consistency topic
            let messages = byo_query_source(byo_consumer);
            match byo_consumer.envelope {
                ConsistencyFormatting::DebeziumAvro => {
                    for msg in messages {
                        let msg = if let ValueEncoding::Bytes(msg) = msg {
                            msg
                        } else {
                            panic!("Kafka Debezium consistency should only encode byte messages");
                        };
                        // The first 5 bytes are reserved for the schema id/schema registry information
                        let mut bytes = &msg[5..];
                        let res = mz_avro::from_avro_datum(&DEBEZIUM_TRX_SCHEMA_VALUE, &mut bytes);
                        match res {
                            Err(_) => {
                                // This was a key message, can safely ignore it
                                continue;
                            }
                            Ok(record) => {
                                generate_ts_updates_from_debezium(
                                    &id,
                                    &self.tx,
                                    byo_consumer,
                                    record,
                                );
                            }
                        }
                    }
                }
                ConsistencyFormatting::DebeziumOcf => {
                    for msg in messages {
                        let value = if let ValueEncoding::Avro(value) = msg {
                            value
                        } else {
                            panic!("Debezium OCF consistency should only encode Value messages");
                        };
                        generate_ts_updates_from_debezium(&id, &self.tx, byo_consumer, value);
                    }
                }
            }
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
        const CONF_DENYLIST: &'static [&'static str] = &["statistics.interval.ms"];
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &kc.addrs.to_string());

        if log_enabled!(target: "librdkafka", log::Level::Debug) {
            config.set("debug", "all");
        }

        for (k, v) in &kc.config_options {
            if !CONF_DENYLIST.contains(&k.as_str()) {
                config.set(k, v);
            }
        }

        let consumer = match config.create::<BaseConsumer>() {
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
                .get("topic_metadata_refresh_interval_ms")
                // Safe conversion: statement::extract_config enforces that option is a value
                // between 0 and 3600000
                .unwrap_or(&"30000".to_owned())
                .parse()
                .unwrap(),
        );

        thread::spawn({
            let connector = connector.clone();
            move || rt_kafka_metadata_fetch_loop(connector, consumer, metadata_refresh_frequency)
        });

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

    fn create_byo_ocf_connector(
        &self,
        _id: GlobalId,
        fc: &FileSourceConnector,
        timestamp_topic: String,
    ) -> Option<ByoFileConnector<mz_avro::types::Value, anyhow::Error>> {
        let ctor = move |file| mz_avro::Reader::new(file);
        let tail = if fc.tail {
            FileReadStyle::TailFollowFd
        } else {
            FileReadStyle::ReadOnce
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(10000_usize);
        let compression = fc.compression.clone();
        std::thread::spawn(move || {
            read_file_task(
                PathBuf::from(timestamp_topic),
                tx,
                None,
                tail,
                compression,
                ctor,
            );
        });

        Some(ByoFileConnector { stream: rx })
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
            ExternalSourceConnector::AvroOcf(fc) => {
                match self.create_byo_ocf_connector(id, &fc, timestamp_topic) {
                    Some(consumer) => Some(ByoTimestampConsumer {
                        source_name: fc.path.to_string_lossy().into_owned(),
                        connector: ByoTimestampConnector::Ocf(consumer),
                        envelope: identify_consistency_format(enc, env),
                        last_ts: 0,
                        last_offset: MzOffset { offset: 0 },
                    }),
                    None => None,
                }
            }
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
            // should match the isolation level of the source; for now that's always read_committed
            .set("isolation.level", "read_committed")
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
            config.set(k, v);
        }

        match config.create() {
            Ok(consumer) => {
                let consumer = ByoKafkaConnector::new(consumer);
                consumer.consumer.subscribe(&[&timestamp_topic]).unwrap();

                match get_kafka_partitions(
                    &consumer.consumer,
                    &timestamp_topic,
                    Duration::from_secs(1),
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

fn rt_kafka_metadata_fetch_loop(c: RtKafkaConnector, consumer: BaseConsumer, wait: Duration) {
    debug!(
        "Starting realtime Kafka thread for {} (source {})",
        &c.topic, &c.id
    );

    let mut current_partition_count = 0;

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
                        error!(
                            "Ignoring decrease in partitions (from {} to {}) for topic {} (source {})",
                             new_partition_count, current_partition_count, c.topic, c.id,
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
                    let max_offset = MAX_AVAILABLE_OFFSET.with_label_values(&[
                        &c.topic,
                        &c.id.to_string(),
                        &pid.to_string(),
                    ]);
                    max_offset.set(high);
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
