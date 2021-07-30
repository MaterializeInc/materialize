// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::ops::Deref;
use std::panic;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use dataflow_types::KafkaOffset;
use dataflow_types::PartitionOffset;
use dataflow_types::SourceFrontierDiscoverer;
use dataflow_types::SourcePartitionDiscoverer;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{debug, error, info, log_enabled, warn};
use mz_avro::schema::Schema;
use mz_avro::types::Value;
use ore::now::system_time;
use prometheus::{register_int_gauge_vec, IntGaugeVec};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use repr::Timestamp;
use timely::progress::Antichain;
use tokio::sync::mpsc;

use dataflow_types::{
    Consistency, DataEncoding, DebeziumMode, ExternalSourceConnector, FileSourceConnector,
    KafkaSourceConnector, KinesisSourceConnector, MzOffset, S3SourceConnector, SourceConnector,
    SourceEnvelope, TimestampSourceUpdate,
};
use expr::{GlobalId, PartitionId};
use ore::collections::CollectionExt;

use crate::catalog::storage;
use crate::catalog::Error;
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
struct RtKafkaConnector {
    coordination_state: Arc<TimestampingState>,
}

/// Source "Discoverer" for Kafka sources.
struct RtKafkaDiscoverer {
    id: GlobalId,
    topic: String,
    consumer: BaseConsumer,
    update_interval: Duration,
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

fn byo_query_source(
    consumer: &mut ByoTimestampConsumer,
) -> Result<Vec<ValueEncoding>, anyhow::Error> {
    let mut messages = vec![];
    match &mut consumer.connector {
        ByoTimestampConnector::Kafka(kafka_connector) => {
            while let Some(payload) = kafka_get_next_message(&mut kafka_connector.consumer)? {
                messages.push(ValueEncoding::Bytes(payload));
            }
        }
    }
    Ok(messages)
}

/// Polls a message from a Kafka Source
fn kafka_get_next_message(consumer: &mut BaseConsumer) -> Result<Option<Vec<u8>>, anyhow::Error> {
    if let Some(result) = consumer.poll(Duration::from_millis(60)) {
        match result {
            Ok(message) => match message.payload() {
                Some(p) => Ok(Some(p.to_vec())),
                None => {
                    bail!("unexpected null payload");
                }
            },
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

    storage: Arc<Mutex<storage::Connection>>,

    /// Frequency at which thread should run
    timestamp_frequency: Duration,
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
                    let frontier = Antichain::from_elem(PartitionOffset::new(
                        PartitionId::Kafka(0),
                        byo_consumer.last_offset,
                    ));
                    // Debezium consistency topic should only work for single-partition
                    // topics
                    tx.send(coord::Message::AdvanceSourceTimestamp(
                        coord::SourceUpdate {
                            id: *id,
                            partitions: vec![PartitionId::Kafka(0)],
                            timestamp_bindings: vec![TimestampSourceUpdate {
                                timestamp: byo_consumer.last_ts,
                                frontier: frontier,
                            }],
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
        storage: Arc<Mutex<storage::Connection>>,
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
            storage,
            timestamp_frequency: frequency,
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
        while let Ok(update) = self.rx.try_recv() {
            match update {
                TimestampMessage::Add(source_id, sc) => {
                    let (sc, enc, env, cons) = if let SourceConnector::External {
                        connector,
                        encoding,
                        envelope,
                        consistency,
                        key_envelope: _,
                        ts_frequency: _,
                        timeline: _,
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
                TimestampMessage::Drop(id) => {
                    self.drop_source(id);
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
                // TODO(aljoscha): get rid of expect()
                let discoverer = self
                    .create_kafka_discoverer(id, kc)
                    .expect("could not create discoverer");
                let connector = self
                    .create_rt_connector_loop(discoverer)
                    .expect("could not create real-time metadata connector");

                Some(RtTimestampConsumer {
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

    fn create_kafka_discoverer(
        &self,
        id: GlobalId,
        kc: KafkaSourceConnector,
    ) -> Result<RtKafkaDiscoverer, anyhow::Error> {
        // These keys do not make sense for the timestamping connector, and will
        // be filtered out (fixes #6313)
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

        let consumer = config
            .create::<BaseConsumer>()
            .context("Failed to create Kafka Consumer for metadata updates")?;

        Ok(RtKafkaDiscoverer {
            id,
            topic: kc.topic.clone(),
            consumer,
            update_interval: metadata_refresh_frequency,
        })
    }

    fn create_rt_connector_loop<D>(&self, discoverer: D) -> Result<RtKafkaConnector, anyhow::Error>
    where
        D: Send
            + 'static
            + SourceFrontierDiscoverer<SourceTimestamp = PartitionOffset>
            + SourcePartitionDiscoverer<Partition = PartitionId>
            + std::fmt::Display,
    {
        let storage = self.storage.clone();

        let coordination_state = Arc::new(TimestampingState {
            stop: AtomicBool::new(false),
            coordinator_channel: self.tx.clone(),
        });

        thread::Builder::new()
            .name("rt_metadata_thread".to_string())
            .spawn({
                let coordination_state = Arc::clone(&coordination_state);
                let update_interval = discoverer.update_interval();
                move || {
                    rt_metadata_fetch_loop(storage, coordination_state, discoverer, update_interval)
                }
            })
            .unwrap();

        let connector = RtKafkaConnector { coordination_state };

        Ok(connector)
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

        match config.create() {
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

impl SourcePartitionDiscoverer for RtKafkaDiscoverer {
    type Partition = PartitionId;

    fn get_available_partitions(&self) -> Result<Vec<Self::Partition>, anyhow::Error> {
        // Poll once to clear any extraneous messages on this queue.
        self.consumer.poll(Duration::from_secs(0));

        match get_kafka_partitions(&self.consumer, &self.topic, Duration::from_secs(30)) {
            Ok(partitions) => Ok(partitions
                .into_iter()
                .map(|pid| PartitionId::Kafka(pid))
                .collect()),
            Err(e) => Err(anyhow!(
                "Unable to fetch kafka metadata for topic {} (source {}): {}",
                self.topic,
                e,
                self.id
            )),
        }
    }
}

impl SourceFrontierDiscoverer for RtKafkaDiscoverer {
    type SourceTimestamp = PartitionOffset;

    fn get_available_frontier(&self) -> Result<Antichain<Self::SourceTimestamp>, anyhow::Error> {
        // Poll once to clear any extraneous messages on this queue.
        self.consumer.poll(Duration::from_secs(0));

        match get_kafka_partitions(&self.consumer, &self.topic, Duration::from_secs(30)) {
            Ok(partitions) => {
                // TODO(benesch): Kafka supports fetching these in bulk, but
                // rust-rdkafka does not. That would save us a lot of requests on
                // large topics.
                let frontier: Result<Vec<_>, _> = partitions
                    .into_iter()
                    .map(|pid| {
                        match self.consumer.fetch_watermarks(
                            &self.topic,
                            pid,
                            Duration::from_secs(30),
                        ) {
                            Ok((_low, high)) => {
                                let max_offset = MAX_AVAILABLE_OFFSET.with_label_values(&[
                                    &self.topic,
                                    &self.id.to_string(),
                                    &pid.to_string(),
                                ]);
                                max_offset.set(high);
                                debug!("High offset: {}: {}", pid, high);
                                let pid = PartitionId::Kafka(pid);

                                // high is one past the highest offset, MzOffset is 1-based, and
                                // the MzOffset we send here should be one past the highest offset
                                // for which the binding is covering.
                                Ok(PartitionOffset::new(
                                    pid,
                                    KafkaOffset { offset: high }.into(),
                                ))
                            }
                            Err(e) => Err(anyhow!(
                                "Unable to fetch Kafka watermarks for topic {} [{}] ({}): {}",
                                self.topic,
                                pid,
                                self.id,
                                e
                            )),
                        }
                    })
                    .collect();

                let mut result = Antichain::new();
                result.extend(frontier?.into_iter());

                Ok(result)
            }
            Err(e) => Err(anyhow!(
                "Unable to fetch kafka metadata for topic {} (source {}): {}",
                self.topic,
                e,
                self.id
            )),
        }
    }

    fn id(&self) -> GlobalId {
        self.id
    }

    fn update_interval(&self) -> Duration {
        self.update_interval
    }
}

impl Display for RtKafkaDiscoverer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (source {})", self.topic, self.id)
    }
}

fn rt_metadata_fetch_loop<D>(
    storage: Arc<Mutex<storage::Connection>>,
    coordination_state: Arc<TimestampingState>,
    discoverer: D,
    wait: Duration,
) where
    D: SourceFrontierDiscoverer<SourceTimestamp = PartitionOffset>
        + SourcePartitionDiscoverer<Partition = PartitionId>
        + std::fmt::Display,
{
    debug!("Starting realtime metadata thread for {}", discoverer);

    let mut announced_offsets = HashMap::new();

    {
        let storage = storage.lock().expect("lock poisoned");

        let previous_bindings = load_timestamp_bindings(storage, discoverer.id())
            .expect("could not load timestamp bindings");

        announced_offsets.extend(
            previous_bindings
                .iter()
                .map(|(pid, _ts, offset)| (pid.clone(), offset.offset)),
        );

        let partitions = previous_bindings
            .iter()
            .map(|(pid, _timestamp, _offset)| pid.clone())
            .collect();

        let mut timestamp_bindings = Vec::new();

        for (timestamp, bindings) in &previous_bindings
            .into_iter()
            .group_by(|(_pid, timestamp, _offset)| timestamp.clone())
        {
            let bindings =
                bindings.map(|(pid, _timestamp, offset)| PartitionOffset::new(pid, offset));

            let mut frontier = Antichain::new();
            frontier.extend(bindings);

            timestamp_bindings.push(TimestampSourceUpdate {
                frontier,
                timestamp: timestamp.clone(),
            });
        }

        println!("Restored earlier ts bindings: {:?}", timestamp_bindings);

        coordination_state
            .coordinator_channel
            .send(coord::Message::AdvanceSourceTimestamp(
                coord::SourceUpdate {
                    id: discoverer.id(),
                    partitions,
                    timestamp_bindings,
                },
            ))
            .expect(
                "Failed to send previous timestamp bindings to coordinator. This should not happen",
            );
    }

    let update_interval = 1000;
    let mut last_update_time = Instant::now();
    let mut current_ts = system_time();

    while !coordination_state.stop.load(Ordering::SeqCst) {
        let new_ts = maybe_update_timestamp(&mut last_update_time, update_interval, current_ts);

        if new_ts == current_ts {
            thread::sleep(wait);
            continue;
        }

        current_ts = new_ts;

        let fallible = || -> Result<(), anyhow::Error> {
            let partitions = discoverer.get_available_partitions()?;
            let frontier = discoverer.get_available_frontier()?;

            // map from source frontier to old-school binding format
            let updates = frontier
                .iter()
                .map(|PartitionOffset { partition, offset }| {
                    (partition.clone(), current_ts.clone(), offset.clone())
                });

            let storage = storage.lock().expect("lock poisoned");

            insert_timestamp_bindings(storage, discoverer.id(), updates)
                .expect("could not store timestamp bindings");

            let timestamp_bindings = vec![TimestampSourceUpdate {
                frontier: frontier,
                timestamp: current_ts,
            }];

            coordination_state
                .coordinator_channel
                .send(coord::Message::AdvanceSourceTimestamp(
                    coord::SourceUpdate {
                        id: discoverer.id(),
                        partitions,
                        timestamp_bindings,
                    },
                ))
                .expect("Failed to send update to coordinator. This should not happen");

            coordination_state
                .coordinator_channel
                .send(coord::Message::CompactTimestampBindings(discoverer.id()))
                .expect("Failed to send compaction notice to coordinator. This should not happen");

            Ok(())
        };

        if let Err(e) = fallible() {
            error!("{}", e);
        }
    }

    debug!("Terminating metadata update thread for {}", discoverer);
}

fn maybe_update_timestamp(
    last_update_time: &mut Instant,
    update_interval: u64,
    current_ts: Timestamp,
) -> Timestamp {
    if last_update_time.elapsed().as_millis() < update_interval.into() {
        return current_ts;
    }

    *last_update_time = Instant::now();

    let mut new_ts = system_time();
    new_ts += update_interval - (new_ts % update_interval);

    new_ts
}

// These shouldn't really be here. But it seems specific source types will
// need to have their own data in persistent storage.

fn insert_timestamp_bindings(
    mut storage: MutexGuard<storage::Connection>,
    source_id: GlobalId,
    timestamps: impl IntoIterator<Item = (PartitionId, Timestamp, MzOffset)>,
) -> Result<(), Error> {
    let tx = storage.transaction()?;

    for (pid, ts, offset) in timestamps.into_iter() {
        tx.insert_timestamp_binding(&source_id, &pid.to_string(), ts, offset.offset)?;
    }
    tx.commit()?;

    Ok(())
}

fn load_timestamp_bindings(
    mut storage: MutexGuard<storage::Connection>,
    source_id: GlobalId,
) -> Result<Vec<(PartitionId, Timestamp, MzOffset)>, Error> {
    let tx = storage.transaction()?;

    let result = tx.load_timestamp_bindings(source_id)?;

    tx.commit()?;

    Ok(result)
}
