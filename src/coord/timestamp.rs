// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::panic;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use avro::schema::Schema;
use avro::types::Value;
use lazy_static::lazy_static;
use log::{error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use rusoto_core::HttpClient;
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{Kinesis, KinesisClient, ListShardsInput};
use rusqlite::{params, NO_PARAMS};

use catalog::sql::SqlVal;
use dataflow_types::{
    Consistency, DataEncoding, Envelope, ExternalSourceConnector, FileSourceConnector,
    KafkaSourceConnector, KinesisSourceConnector, SourceConnector,
};
use expr::{PartitionId, SourceInstanceId};

use crate::coord;

use futures::executor::block_on;
use itertools::Itertools;

lazy_static! {
    static ref DEBEZIUM_TRX_SCHEMA_KEY: Schema = {
        Schema::parse_str(
            r#"    {
    "name": "io.debezium.connector.common.TransactionMetadataKey",
    "type": "record",
    "fields": [
        {
            "name": "id",
            "type": "string"
        }
    ]
}"#,
        )
        .unwrap()
    };
    static ref DEBEZIUM_TRX_SCHEMA_VALUE: Schema = {
        Schema::parse_str(
            r#"    {
    "name": "io.debezium.connector.common.TransactionMetadataValue",
    "type": "record",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "status",
            "type": "string"
        },
        {
            "name": "event_count",
            "type": [
                "long",
                "null"
            ]
        },
        {
            "name": "data_collections",
            "type": [
                {
                    "type": "array",
                    "items": {
                        "name": "data",
                        "type": "record",
                        "fields": [
                            {
                                "name": "event_count",
                                "type": "long"
                            },
                            {
                                "name": "data_collection",
                                "type": "string"
                            }
                        ]
                    }
                },
                "null"
            ]
        }
    ]
}"#,
        )
        .unwrap()
    };
}

pub struct TimestampConfig {
    pub frequency: Duration,
    pub max_size: i64,
}

#[derive(Debug)]
pub enum TimestampMessage {
    Add(SourceInstanceId, SourceConnector),
    DropInstance(SourceInstanceId),
    Shutdown,
}

/// Timestamp consumer: wrapper around source consumers that stores necessary information
/// about topics and offset for real-time consistency
struct RtTimestampConsumer {
    connector: RtTimestampConnector,
    last_offset: i64,
}

enum RtTimestampConnector {
    Kafka(RtKafkaConnector),
    File(RtFileConnector),
    Kinesis(RtKinesisConnector),
}

/// Timestamp consumer: wrapper around source consumers that stores necessary information
/// about topics and offset for byo consistency
struct ByoTimestampConsumer {
    // Source Connector
    connector: ByoTimestampConnector,
    // The name of the source with which this connector is associated
    source_name: String,
    // The format of the connector
    envelope: ConsistencyFormatting,
    // The last timestamp assigned per partition
    last_partition_ts: HashMap<PartitionId, u64>,
    // The max assigned timestamp. Should be max(last_partition_ts)
    last_ts: u64,
    // The max offset for which a timestamp has been assigned
    last_offset: i64,
    // The total number of partitions for the data topic
    current_partition_count: i32,
}

/// Supported format/envelope pairs for consistency topic decoding
/// TODO(natacha): this should be removed
enum ConsistencyFormatting {
    // The formatting of this consistency source follows the
    // SourceName,PartitionCount,PartitionId,TS,Offset
    Raw,
    // The formatting of this consistency source follows the
    // Debezium Kafka format
    DebeziumKafka,
    // The formatting of this consistency source follows the
    // Debezium format (OCF + Avro)
    DebeziumOcf,
}
enum ByoTimestampConnector {
    Kafka(ByoKafkaConnector),
    File(ByoFileConnector),
    Kinesis(ByoKinesisConnector),
}

/// Data consumer for Kafka source with RT consistency
struct RtKafkaConnector {
    consumer: BaseConsumer,
    topic: String,
    //start_offset: u64,
}

/// Data consumer for Kafka source with BYO consistency
struct ByoKafkaConnector {
    consumer: BaseConsumer,
}

/// Data consumer for Kinesis source with RT consistency
#[allow(dead_code)]
struct RtKinesisConnector {
    stream_name: String,
    kinesis_client: KinesisClient,
}

/// Data consumer stub for Kinesis source with BYO consistency
struct ByoKinesisConnector {}

/// Data consumer stub for File source with RT consistency
struct RtFileConnector {}

/// Data consumer stub for File source with BYO consistency
struct ByoFileConnector {}

fn byo_query_source(consumer: &mut ByoTimestampConsumer, max_increment_size: i64) -> Vec<Vec<u8>> {
    let mut messages = vec![];
    let mut msg_count = 0;
    match &mut consumer.connector {
        ByoTimestampConnector::Kafka(kafka_consumer) => {
            while let Some(payload) = kafka_get_next_message(&mut kafka_consumer.consumer) {
                messages.push(payload);
                msg_count += 1;
                if msg_count == max_increment_size {
                    // Make sure to bound the number of timestamp updates we have at once,
                    // to avoid overflowing the system
                    break;
                }
            }
        }
        ByoTimestampConnector::Kinesis(_kinesis_consumer) => {
            error!("Timestamping for Kinesis sources is unimplemented");
        }
        ByoTimestampConnector::File(_file_consumer) => {
            error!("Timestamping for File sources is unimplemented");
        }
    }
    messages
}

fn byo_extract_update_from_bytes(
    consumer: &ByoTimestampConsumer,
    messages: Vec<Vec<u8>>,
) -> Vec<(i32, PartitionId, u64, i64)> {
    let mut updates = vec![];
    for payload in messages {
        let st = str::from_utf8(&payload);
        match st {
            Ok(timestamp) => {
                // Extract timestamp from payload
                let split: Vec<&str> = timestamp.split(',').collect();
                if split.len() != 5 {
                    error!("incorrect payload format. Expected: SourceName,PartitionCount,PartitionId,TS,Offset");
                    continue;
                }
                let topic_name = String::from(split[0]);
                let partition_count = match split[1].parse::<i32>() {
                    Ok(i) => i,
                    Err(err) => {
                        error!("incorrect timestamp format {}", err);
                        continue;
                    }
                };
                let partition = match &consumer.connector {
                    ByoTimestampConnector::Kinesis(_) => match split[2].parse::<String>() {
                        Ok(s) => PartitionId::Kinesis(s),
                        Err(err) => {
                            error!("incorrect timestamp format {}", err);
                            continue;
                        }
                    },
                    ByoTimestampConnector::Kafka(_) => match split[2].parse::<i32>() {
                        Ok(i) => PartitionId::Kafka(i),
                        Err(err) => {
                            error!("incorrect timestamp format {}", err);
                            continue;
                        }
                    },
                    ByoTimestampConnector::File(_) => unimplemented!(),
                };
                let ts = match split[3].parse::<u64>() {
                    Ok(i) => i,
                    Err(err) => {
                        error!("incorrect timestamp format {}", err);
                        continue;
                    }
                };
                let offset = match split[4].parse::<i64>() {
                    Ok(i) => i,
                    Err(err) => {
                        error!("incorrect timestamp format {}", err);
                        continue;
                    }
                };
                if topic_name == consumer.source_name {
                    updates.push((partition_count, partition, ts, offset))
                }
            }
            Err(err) => error!("incorrect payload format: {}", err),
        }
    }
    updates
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
fn get_kafka_partitions(consumer: &BaseConsumer, topic: &str) -> Vec<i32> {
    let mut partitions = vec![];
    while partitions.len() == 0 {
        let result = consumer.fetch_metadata(Some(&topic), Duration::from_secs(1));
        match &result {
            Ok(meta) => {
                if let Some(topic) = meta.topics().iter().find(|t| t.name() == topic) {
                    partitions = topic.partitions().iter().map(|x| x.id()).collect_vec();
                }
            }
            Err(e) => {
                error!("Failed to obtain partition information: {} {}", topic, e);
            }
        };
    }
    partitions
}

pub struct Timestamper {
    // Current list of up to date sources that use a real time consistency model
    rt_sources: HashMap<SourceInstanceId, RtTimestampConsumer>,

    // Current list of up to date sources that use a BYO consistency model
    byo_sources: HashMap<SourceInstanceId, ByoTimestampConsumer>,

    // Connection to the underlying SQL lite instance
    storage: Arc<Mutex<catalog::sql::Connection>>,

    tx: futures::channel::mpsc::UnboundedSender<coord::Message>,
    rx: std::sync::mpsc::Receiver<TimestampMessage>,

    // Last Timestamp (necessary because not necessarily increasing otherwise)
    current_timestamp: u64,

    // Frequency at which thread should run
    timestamp_frequency: Duration,

    // Max increment size
    max_increment_size: i64,
}

/// A debezium record contains a set of update counts for each topic that the transaction
/// updated. This function extracts the set of (topic, update_count) as a vector.
fn parse_debezium(record: Vec<(String, Value)>) -> Vec<(String, i64)> {
    let mut result = vec![];
    for (key, value) in record {
        if key == "data_collections" {
            if let Value::Union(_, value) = value {
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
                            result.push((value, write_count))
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
    result
}

/// This function determines the next maximum offset to timestamp.
/// This offset should be no greater than max_increment_size
fn determine_next_offset(last_offset: i64, available_offsets: i64, max_increment_size: i64) -> i64 {
    // Bound the next timestamp to be no more than max_increment_size in the future
    if (available_offsets - last_offset) > max_increment_size {
        last_offset + max_increment_size
    } else {
        available_offsets
    }
}

/// Determines whether the next proposed timestamp follows the timestamp
/// assigning rules
fn is_ts_valid(
    byo_consumer: &ByoTimestampConsumer,
    partition_count: i32,
    partition: &PartitionId,
    timestamp: u64,
) -> bool {
    let last_p_ts = match byo_consumer.last_partition_ts.get(&partition) {
        Some(ts) => *ts,
        None => 0,
    };

    if timestamp == 0
        || timestamp == std::u64::MAX
        || timestamp < byo_consumer.last_ts
        || timestamp <= last_p_ts
        || (partition_count > byo_consumer.current_partition_count
            && timestamp == byo_consumer.last_ts)
    {
        error!("The timestamp assignment rules have been violated. The rules are as follows:\n\
                     1) A timestamp should be greater than 0\n\
                     2) The timestamp should be strictly smaller than u64::MAX\n\
                     2) If no new partition is added, a new timestamp should be:\n \
                        - strictly greater than the last timestamp in this partition\n \
                        - greater or equal to all the timestamps that have been assigned across all partitions\n \
                        If a new partition is added, a new timestamp should be:\n  \
                        - strictly greater than the last timestamp\n");
        return false;
    }
    true
}

/// This function determines the expected format of the consistency metadata as a function
/// of the encoding and the envelope of the source.
/// Specifically:
/// 1) an OCF file source with a Debezium envelope will expect an OCF Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
/// 2) any other file source with a Debezium envelope will expect an Avro consistency source
/// that follows the TRX_METADATA_SCHEMA Avro spec outlined above
/// 3) any source that uses the Text/Regex/Csv/Byte format will expect a consistency source that
/// is formatted using the text
/// 4) any source that uses the Protobuf format currently expects a consistency source that is formatted
/// using the text format (SourceName,PartitionCount,Partition,Timestamp,Offset)
/// 5) any source that uses the Avro format currently expects a consistency source that is formatted
/// using the BYO_CONSISTENCY_SCHEMA Avro spec outlined above.
///
fn identify_consistency_format(enc: DataEncoding, env: Envelope) -> ConsistencyFormatting {
    if let Envelope::Debezium = env {
        if let DataEncoding::AvroOcf { reader_schema: _ } = enc {
            ConsistencyFormatting::DebeziumOcf
        } else {
            ConsistencyFormatting::DebeziumKafka
        }
    } else {
        ConsistencyFormatting::Raw
    }
}

impl Timestamper {
    pub fn new(
        config: &TimestampConfig,
        storage: Arc<Mutex<catalog::sql::Connection>>,
        tx: futures::channel::mpsc::UnboundedSender<coord::Message>,
        rx: std::sync::mpsc::Receiver<TimestampMessage>,
    ) -> Self {
        // Recover existing data by running max on the timestamp count. This will ensure that
        // there will never be two duplicate entries and that there is a continuous stream
        // of timestamp updates across reboots
        let max_ts = storage
            .lock()
            .expect("lock poisoned")
            .prepare("SELECT MAX(timestamp) FROM timestamps")
            .expect("Failed to prepare statement")
            .query_row(NO_PARAMS, |row| {
                let res: Result<SqlVal<u64>, _> = row.get(2);
                match res {
                    Ok(res) => Ok(res.0),
                    _ => Ok(0),
                }
            })
            .expect("Failure to parse timestamp");

        info!(
            "Starting Timestamping Thread. Frequency: {} ms.",
            config.frequency.as_millis()
        );

        Self {
            rt_sources: HashMap::new(),
            byo_sources: HashMap::new(),
            storage,
            tx,
            rx,
            current_timestamp: max_ts,
            timestamp_frequency: config.frequency,
            max_increment_size: config.max_size,
        }
    }

    fn storage(&self) -> MutexGuard<catalog::sql::Connection> {
        self.storage.lock().expect("lock poisoned")
    }

    /// Run the update function in a loop at the specified frequency. Acquires timestamps using
    /// either 1) the Kafka topic ground truth 2) real-time
    pub fn update(&mut self) {
        loop {
            thread::sleep(self.timestamp_frequency);
            let shutdown = self.update_sources();
            if shutdown {
                break;
            } else {
                self.update_rt_timestamp();
                self.update_byo_timestamp();
            }
        }
    }

    /// Implements the real-time timestamping logic
    fn update_rt_timestamp(&mut self) {
        let watermarks = self.rt_query_sources();
        self.rt_generate_next_timestamp();
        self.rt_persist_timestamp(&watermarks);
        for (id, partition_count, pid, offset) in watermarks {
            self.tx
                .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                    id,
                    partition_count,
                    pid,
                    timestamp: self.current_timestamp,
                    offset,
                })
                .expect("Failed to send timestamp update to coordinator");
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
                TimestampMessage::Add(id, sc) => {
                    let (sc, enc, env, cons) = if let SourceConnector::External {
                        connector,
                        encoding,
                        envelope,
                        consistency,
                    } = sc
                    {
                        (connector, encoding, envelope, consistency)
                    } else {
                        panic!("A Local Source should never be timestamped");
                    };
                    if !self.rt_sources.contains_key(&id) && !self.byo_sources.contains_key(&id) {
                        // Did not know about source, must update
                        match cons {
                            Consistency::RealTime => {
                                info!("Timestamping Source {} with Real Time Consistency.", id);
                                let start_offset = match sc {
                                    ExternalSourceConnector::Kafka(KafkaSourceConnector {
                                        start_offset,
                                        ..
                                    }) => start_offset,
                                    _ => 0,
                                };
                                let last_offset =
                                    std::cmp::max(start_offset, self.rt_recover_source(id));
                               let consumer = self.create_rt_connector(id, sc, last_offset);
                                if let Some(consumer) = consumer {
                                    self.rt_sources.insert(id, consumer);
                                }
                            }
                            Consistency::BringYourOwn(consistency_topic) => {
                                info!("Timestamping Source {} with BYO Consistency. Consistency Source: {}", id, consistency_topic);
                                let consumer =
                                    self.create_byo_connector(id, sc, enc, env, consistency_topic);
                                if let Some(consumer) = consumer {
                                    self.byo_sources.insert(id, consumer);
                                }
                            }
                        }
                    }
                }
                TimestampMessage::DropInstance(id) => {
                    info!("Dropping Timestamping for Source {}.", id);
                    self.storage()
                        .prepare_cached("DELETE FROM timestamps WHERE sid = ? AND vid = ?")
                        .expect("Failed to prepare delete statement")
                        .execute(params![SqlVal(&id.sid), SqlVal(&id.vid)])
                        .expect("Failed to execute delete statement");
                    self.rt_sources.remove(&id);
                    self.byo_sources.remove(&id);
                }
                TimestampMessage::Shutdown => return true,
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
    /// Supports two envelopes: None and Debezium. Currently compatible with Debezium format 1.1
    fn update_byo_timestamp(&mut self) {
        for (id, byo_consumer) in &mut self.byo_sources {
            // Get the next set of messages from the Consistency topic
            let messages = byo_query_source(byo_consumer, self.max_increment_size);
            match byo_consumer.envelope {
                ConsistencyFormatting::Raw => {
                    for (partition_count, partition, timestamp, offset) in
                        byo_extract_update_from_bytes(byo_consumer, messages)
                    {
                        if is_ts_valid(&byo_consumer, partition_count, &partition, timestamp) {
                            match byo_consumer.connector {
                                ByoTimestampConnector::Kafka(_) => {
                                    if byo_consumer.current_partition_count < partition_count {
                                        // A new partition has been added. Partitions always gets added with
                                        // newPartitionId = previousLastPartitionId + 1 and start from 0.
                                        // So this new partition will have ID "partition_count - 1"
                                        // We ensure that the first messages in this partition will always have
                                        // timestamps > the last closed timestamp. We need to explicitly close
                                        // out all prior timestamps. To achieve this, we send an additional
                                        // timestamp message to the coord/worker
                                        self.tx
                                            .unbounded_send(
                                                coord::Message::AdvanceSourceTimestamp {
                                                    id: *id,
                                                    partition_count, // The new partition count
                                                    pid: PartitionId::Kafka(partition_count - 1), // the ID of the new partition
                                                    timestamp: byo_consumer.last_ts,
                                                    offset: 0, // An offset of 0 will "fast-forward" the stream, it denotes
                                                               // the empty interval
                                                },
                                            )
                                            .expect("Failed to send update to coordinator");
                                    }
                                    byo_consumer.current_partition_count = partition_count;
                                    byo_consumer.last_ts = timestamp;
                                    byo_consumer
                                        .last_partition_ts
                                        .insert(partition.clone(), timestamp);
                                    self.tx
                                        .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                                            id: *id,
                                            partition_count,
                                            pid: partition,
                                            timestamp,
                                            offset,
                                        })
                                        .expect("Failed to send update to coordinator");
                                }
                                _ => {
                                    error!(
                                        "BYO consistency is not supported for this source type."
                                    );
                                    return;
                                }
                            }
                        }
                    }
                }
                ConsistencyFormatting::DebeziumKafka => {
                    for msg in messages {
                        // The first 5 bytes are reserved for the schema id/schema registry information
                        let mut bytes = &msg[5..];
                        let res = avro::from_avro_datum(&DEBEZIUM_TRX_SCHEMA_VALUE, &mut bytes);
                        let results = match res {
                            Ok(record) => {
                                if let Value::Record(record) = record {
                                    parse_debezium(record)
                                } else {
                                    error!("Incorrect Avro format. Expected Record");
                                    vec![]
                                }
                            }
                            Err(_) => {
                                // This message was a key message. We can safely ignore it
                                vec![]
                            }
                        };
                        //TODO(natacha): this is (potentially)
                        // inefficient every customer processes the same
                        // consistency topic.
                        for (topic, count) in results {
                            if byo_consumer.source_name == topic {
                                // TODO(natacha): consistency topic for Debezium currently supports only one partition
                                byo_consumer.last_offset += count;
                                byo_consumer.last_ts += 1;
                                self.tx
                                    .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                                        id: *id,
                                        partition_count: 1,
                                        pid: PartitionId::Kafka(0),
                                        timestamp: byo_consumer.last_ts,
                                        offset: byo_consumer.last_offset,
                                    })
                                    .expect("Failed to send update to coordinator");
                            }
                        }
                    }
                }
                ConsistencyFormatting::DebeziumOcf => {
                    error!("Avro OCF sources are not currently supported");
                }
            }
        }
    }

    /// Creates a RT connector
    fn create_rt_connector(
        &self,
        id: SourceInstanceId,
        sc: ExternalSourceConnector,
        last_offset: i64,
    ) -> Option<RtTimestampConsumer> {
        match sc {
            ExternalSourceConnector::Kafka(kc) => {
                let connector = self.create_rt_kafka_connector(id, kc);
                match connector {
                    Some(connector) => Some(RtTimestampConsumer {
                        connector: RtTimestampConnector::Kafka(connector),
                        last_offset,
                    }),
                    None => None,
                }
            }
            ExternalSourceConnector::File(fc) => {
                let connector = self.create_rt_file_connector(id, fc);
                match connector {
                    Some(connector) => Some(RtTimestampConsumer {
                        connector: RtTimestampConnector::File(connector),
                        last_offset,
                    }),
                    None => None,
                }
            }
            ExternalSourceConnector::AvroOcf(_) => unimplemented!(),
            ExternalSourceConnector::Kinesis(kinc) => {
                let connector = self.create_rt_kinesis_connector(id, kinc);
                match connector {
                    Some(connector) => Some(RtTimestampConsumer {
                        connector: RtTimestampConnector::Kinesis(connector),
                        last_offset,
                    }),
                    None => None,
                }
            }
        }
    }

    fn create_byo_file_connector(
        &self,
        _id: SourceInstanceId,
        _fc: &FileSourceConnector,
        _timestamp_topic: String,
    ) -> Option<ByoFileConnector> {
        None
    }

    fn create_rt_kinesis_connector(
        &self,
        _id: SourceInstanceId,
        kinc: KinesisSourceConnector,
    ) -> Option<RtKinesisConnector> {
        let request_dispatcher = HttpClient::new().unwrap();
        let provider = StaticProvider::new(
            kinc.access_key.clone(),
            kinc.secret_access_key.clone(),
            kinc.token.clone(),
            None,
        );
        let kinesis_client = KinesisClient::new_with(request_dispatcher, provider, kinc.region);

        Some(RtKinesisConnector {
            stream_name: kinc.stream_name.clone(),
            kinesis_client,
        })
    }

    fn create_rt_kafka_connector(
        &self,
        id: SourceInstanceId,
        kc: KafkaSourceConnector,
    ) -> Option<RtKafkaConnector> {
        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "earliest")
            .set("group.id", &format!("materialize-rt-{}-{}", &kc.topic, id))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &kc.url.to_string());

        for (k, v) in &kc.config_options {
            config.set(k, v);
        }

        match config.create() {
            Ok(consumer) => Some(RtKafkaConnector {
                consumer,
                topic: kc.topic,
            }),
            Err(e) => {
                error!("Failed to create Kafka Consumer {}", e);
                None
            }
        }
    }

    fn create_rt_file_connector(
        &self,
        _id: SourceInstanceId,
        _fc: FileSourceConnector,
    ) -> Option<RtFileConnector> {
        error!("Timestamping is unsupported for file sources");
        None
    }

    /// Creates a BYO connector
    fn create_byo_connector(
        &self,
        id: SourceInstanceId,
        sc: ExternalSourceConnector,
        enc: DataEncoding,
        env: Envelope,
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
                        last_partition_ts: HashMap::new(),
                        last_ts: 0,
                        current_partition_count: 1,
                        last_offset: 0,
                    }),
                    None => None,
                }
            }
            ExternalSourceConnector::File(fc) => {
                match self.create_byo_file_connector(id, &fc, timestamp_topic) {
                    Some(consumer) => Some(ByoTimestampConsumer {
                        source_name: fc.path.to_string_lossy().into_owned(),
                        connector: ByoTimestampConnector::File(consumer),
                        envelope: identify_consistency_format(enc, env),
                        last_partition_ts: HashMap::new(),
                        last_ts: 0,
                        current_partition_count: 1,
                        last_offset: 0,
                    }),
                    None => None,
                }
            }
            ExternalSourceConnector::AvroOcf(_) => unimplemented!(),
            ExternalSourceConnector::Kinesis(kinc) => {
                match self.create_byo_kinesis_connector(id, &kinc, timestamp_topic) {
                    Some(consumer) => Some(ByoTimestampConsumer {
                        source_name: kinc.stream_name,
                        connector: ByoTimestampConnector::Kinesis(consumer),
                        envelope: identify_consistency_format(enc, env),
                        last_partition_ts: HashMap::new(),
                        last_ts: 0,
                        current_partition_count: 1,
                        last_offset: 0,
                    }),
                    None => None,
                }
            }
        }
    }

    fn create_byo_kinesis_connector(
        &self,
        _id: SourceInstanceId,
        _kinc: &KinesisSourceConnector,
        _timestamp_topic: String,
    ) -> Option<ByoKinesisConnector> {
        unimplemented!();
    }

    fn create_byo_kafka_connector(
        &self,
        id: SourceInstanceId,
        kc: &KafkaSourceConnector,
        timestamp_topic: String,
    ) -> Option<ByoKafkaConnector> {
        let mut config = ClientConfig::new();
        config
            .set(
                "group.id",
                &format!("materialize-byo-{}-{}", &timestamp_topic, id),
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &kc.url.to_string());
        for (k, v) in &kc.config_options {
            config.set(k, v);
        }

        match config.create() {
            Ok(consumer) => {
                let consumer = ByoKafkaConnector { consumer };
                consumer.consumer.subscribe(&[&timestamp_topic]).unwrap();

                let partitions = get_kafka_partitions(&consumer.consumer, &timestamp_topic);
                if partitions.len() != 1 {
                    error!(
                        "Consistency topic should contain a single partition. Contains {}",
                        partitions.len()
                    );
                }
                Some(consumer)
            }
            Err(e) => {
                error!("Could not create a Kafka consumer. Error: {}", e);
                None
            }
        }
    }

    /// Recovers any existing timestamp updates for that (SourceId,ViewId) pair from the underlying
    /// SQL database. Notifies the coordinator of these updates
    fn rt_recover_source(&mut self, id: SourceInstanceId) -> i64 {
        let ts_updates: Vec<_> = self
            .storage()
            .prepare("SELECT pcount, pid, timestamp, offset FROM timestamps WHERE sid = ? AND vid = ? ORDER BY timestamp")
            .expect("Failed to execute select statement")
            .query_and_then(params![SqlVal(&id.sid), SqlVal(&id.vid)], |row| -> Result<_, failure::Error> {
                let pcount: SqlVal<i32> = row.get(0)?;
                let pid: SqlVal<PartitionId> = match row.get(1) {
                    Ok(val) => val,
                    Err(_err) => {
                        // Historically, pid was an i32 value. If the found value is not of type
                        // PartitionId, try to read an i32.
                        let pid: SqlVal<i32> = row.get(1)?;
                        SqlVal(PartitionId::Kafka(pid.0))
                    },
                };
                let timestamp: SqlVal<u64> = row.get(2)?;
                let offset: SqlVal<i64> = row.get(3)?;
                Ok((pcount.0, pid.0, timestamp.0, offset.0))
            })
            .expect("Failed to parse SQL result")
            .collect();

        let mut max_offset = 0;
        for row in ts_updates {
            let (partition_count, pid, timestamp, offset) =
                row.expect("Failed to parse SQL result");
            max_offset = if offset > max_offset {
                offset
            } else {
                max_offset
            };
            self.tx
                .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                    id,
                    partition_count,
                    pid,
                    timestamp,
                    offset,
                })
                .expect("Failed to send timestamp update to coordinator");
        }
        max_offset
    }

    /// Query real-time sources for the current max offset that has been generated for that source
    /// Set the new timestamped offset to min(max_offset, last_offset + increment_size): this ensures
    /// that we never create an overly large batch of messages for the same timestamp (which would
    /// prevent views from becoming visible in a timely fashion)
    fn rt_query_sources(&mut self) -> Vec<(SourceInstanceId, i32, PartitionId, i64)> {
        let mut result = vec![];
        for (id, cons) in self.rt_sources.iter_mut() {
            match &cons.connector {
                RtTimestampConnector::Kafka(kc) => {
                    let partitions = get_kafka_partitions(&kc.consumer, &kc.topic);
                    let partition_count = i32::try_from(partitions.len()).unwrap();
                    for p in partitions {
                        let watermark =
                            kc.consumer
                                .fetch_watermarks(&kc.topic, p, Duration::from_secs(1));
                        match watermark {
                            Ok((_low, high)) => {
                                let next_offset = determine_next_offset(
                                    cons.last_offset,
                                    high,
                                    self.max_increment_size,
                                );
                                cons.last_offset = next_offset;
                                result.push((
                                    *id,
                                    partition_count,
                                    PartitionId::Kafka(p),
                                    next_offset,
                                ))
                            }
                            Err(e) => {
                                error!(
                                    "Failed to obtain Kafka Watermark Information: {} {}",
                                    id, e
                                );
                            }
                        }
                    }
                }
                RtTimestampConnector::File(_cons) => {
                    error!("Timestamping for File sources is not supported");
                }
                RtTimestampConnector::Kinesis(kc) => {
                    match block_on(kc.kinesis_client.list_shards(ListShardsInput {
                        exclusive_start_shard_id: None,
                        max_results: None,
                        next_token: None,
                        stream_creation_timestamp: None,
                        stream_name: Some(kc.stream_name.clone()),
                    })) {
                        Ok(output) => match output.shards {
                            Some(shards) => {
                                // For now, always just push the current system timestamp.
                                // todo@jldlaughlin Github issue #2219
                                for shard in shards {
                                    result.push((*id, 0, PartitionId::Kinesis(shard.shard_id.clone()), self.current_timestamp as i64));
                                }
                            },
                            None => error!("Kinesis stream {} has no shards, cannot update watermark information", kc.stream_name),
                        },
                        Err(e) => error!("Failed to list shards for Kinesis stream {} and update watermark information: {}", kc.stream_name, e),
                    }
                }
            }
        }
        result
    }

    /// Persist timestamp updates to the underlying storage when using the
    /// real-time timestamping logic.
    fn rt_persist_timestamp(&self, ts_updates: &[(SourceInstanceId, i32, PartitionId, i64)]) {
        let storage = self.storage();
        for (id, pcount, pid, offset) in ts_updates {
            let mut stmt = storage
                .prepare_cached(
                    "INSERT INTO timestamps (sid, vid, pcount, pid, timestamp, offset) VALUES (?, ?, ?, ?, ?, ?)",
                )
                .expect(
                    "Failed to prepare insert statement into persistent store. \
                     Hint: increase the system file descriptor limit.",
                );
            while let Err(e) = stmt.execute(params![
                SqlVal(&id.sid),
                SqlVal(&id.vid),
                SqlVal(&pcount),
                SqlVal(&pid),
                SqlVal(&self.current_timestamp),
                SqlVal(&offset)
            ]) {
                error!(
                    "Failed to insert statement into persistent store: {}. \
                     Hint: increase the system file descriptor limit.",
                    e
                );
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    /// Generates a timestamp that is guaranteed to be monotonically increasing.
    /// This may require multiple calls to the underlying now() system method, which is not
    /// guaranteed to increase monotonically
    fn rt_generate_next_timestamp(&mut self) {
        // TODO[reliability] (brennan) - If someone does something silly like sets their
        // system clock backward by an hour while mz is running,
        // we will hang here for an hour.
        let mut new_ts = 0;
        while new_ts <= self.current_timestamp {
            let start = SystemTime::now();
            new_ts = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
        }
        assert!(new_ts > self.current_timestamp);
        self.current_timestamp = new_ts;
    }
}
