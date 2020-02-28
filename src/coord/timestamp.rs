// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use rusqlite::{params, NO_PARAMS};

use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::Consumer as KinesisConsumer;
use rusoto_kinesis::{
    Kinesis, KinesisClient, RegisterStreamConsumerInput, RegisterStreamConsumerOutput,
};

use catalog::sql::SqlVal;
use dataflow_types::{
    Consistency, ExternalSourceConnector, FileSourceConnector, KafkaSourceConnector,
    KinesisSourceConnector,
};
use expr::SourceInstanceId;

use chrono::Utc;
use log::{error, info};
use crate::coord;

pub struct TimestampConfig {
    pub frequency: Duration,
    pub max_size: i64,
}

#[derive(Debug)]
pub enum TimestampMessage {
    Add(SourceInstanceId, ExternalSourceConnector, Consistency),
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
    connector: ByoTimestampConnector,
    source_name: String,
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
}

/// Data consumer for Kafka source with BYO consistency
struct ByoKafkaConnector {
    consumer: BaseConsumer,
    timestamp_topic: String,
}

/// Data consumer for Kinesis source with RT consistency
#[allow(dead_code)]
struct RtKinesisConnector {
    consumer: KinesisConsumer,
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

fn byo_extract_ts_update(
    consumer: &ByoTimestampConsumer,
    messages: Vec<Vec<u8>>,
) -> Vec<(u64, i64)> {
    let mut updates = vec![];
    for payload in messages {
        let st = str::from_utf8(&payload);
        match st {
            Ok(timestamp) => {
                // Extract timestamp from payload
                let split: Vec<&str> = timestamp.split(',').collect();
                if split.len() != 3 {
                    error!("incorrect payload format. Expected: SourceName/TS/Offset");
                    continue;
                }
                let topic_name = String::from(split[0]);
                let ts = match split[1].parse::<u64>() {
                    Ok(i) => i,
                    Err(err) => {
                        error!("incorrect timestamp format {}", err);
                        continue;
                    }
                };
                let offset = match split[2].parse::<i64>() {
                    Ok(i) => i,
                    Err(err) => {
                        error!("incorrect timestamp format {}", err);
                        continue;
                    }
                };
                if topic_name == consumer.source_name {
                    updates.push((ts, offset))
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
        for (id, offset) in watermarks {
            self.tx
                .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                    id,
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
                TimestampMessage::Add(id, sc, consistency) => {
                    if !self.rt_sources.contains_key(&id) && !self.byo_sources.contains_key(&id) {
                        // Did not know about source, must update
                        match consistency {
                            Consistency::RealTime => {
                                info!("Timestamping Source {} with Real Time Consistency", id);
                                let last_offset = self.rt_recover_source(id);
                                let connector = self.create_rt_connector(id, sc, last_offset);
                                self.rt_sources.insert(id, connector);
                            }
                            Consistency::BringYourOwn(consistency_topic) => {
                                info!("Timestamping Source {} with BYO Consistency. Consistency Source: {}", id, consistency_topic);
                                let consumer = self.create_byo_connector(id, sc, consistency_topic);
                                self.byo_sources.insert(id, consumer);
                            }
                        }
                    }
                }
                TimestampMessage::DropInstance(id) => {
                    info!("Dropping Timestamping for Source {}", id);
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
    fn update_byo_timestamp(&mut self) {
        for (id, byo_consumer) in &mut self.byo_sources {
            // Get the next set of messages from the Consistency topic
            let messages = byo_query_source(byo_consumer, self.max_increment_size);
            // Notify coordinator of updates
            for (timestamp, offset) in byo_extract_ts_update(byo_consumer, messages) {
                self.tx
                    .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                        id: *id,
                        timestamp,
                        offset,
                    })
                    .expect("Failed to send update to coordinator");
            }
        }
    }

    /// Creates a RT connector
    fn create_rt_connector(
        &self,
        id: SourceInstanceId,
        sc: ExternalSourceConnector,
        last_offset: i64,
    ) -> RtTimestampConsumer {
        match sc {
            ExternalSourceConnector::Kafka(kc) => RtTimestampConsumer {
                connector: RtTimestampConnector::Kafka(self.create_rt_kafka_connector(id, kc)),
                last_offset,
            },
            ExternalSourceConnector::File(fc) => RtTimestampConsumer {
                connector: RtTimestampConnector::File(self.create_rt_file_connector(id, fc)),
                last_offset,
            },
            ExternalSourceConnector::Kinesis(kinc) => RtTimestampConsumer {
                connector: RtTimestampConnector::Kinesis(
                    self.create_rt_kinesis_connector(id, kinc),
                ),
                last_offset,
            },
        }
    }

    fn create_rt_kafka_connector(
        &self,
        id: SourceInstanceId,
        kc: KafkaSourceConnector,
    ) -> RtKafkaConnector {
        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-rt-{}-{}", &kc.topic, id))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "300000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &kc.url.to_string());

        if let Some(path) = kc.ssl_certificate_file {
            config.set("security.protocol", "ssl");
            config.set(
                "ssl.ca.location",
                path.to_str()
                    .expect("Converting ssl certificate file path failed"),
            );
        }

        let k_consumer: BaseConsumer = config.create().expect("Failed to create Kakfa consumer");
        RtKafkaConnector {
            consumer: k_consumer,
            topic: kc.topic,
        }
    }

    fn create_rt_file_connector(
        &self,
        _id: SourceInstanceId,
        _fc: FileSourceConnector,
    ) -> RtFileConnector {
        error!("Timestamping is unsupported for file sources");
        RtFileConnector {}
    }

    fn create_rt_kinesis_connector(
        &self,
        _id: SourceInstanceId,
        kinc: KinesisSourceConnector,
    ) -> RtKinesisConnector {
        let provider = StaticProvider::new(
            kinc.access_key.clone(),
            kinc.secret_access_key.clone(),
            None,
            None,
        );

        // todo@jldlaughlin: Use HttpClient::new_with_config() to support a TLS-enabled client
        let request_dispatcher = HttpClient::new().unwrap();
        let r: Region = kinc
            .region
            .parse()
            .unwrap_or_else(|_| panic!("Failed to parse AWS region: {}", kinc.region));
        let client = KinesisClient::new_with(request_dispatcher, provider, r);

        // Each consumer name must be unique within a stream.
        // todo@jldlaughlin: Add a random string at the end, too?
        let register_input = RegisterStreamConsumerInput {
            consumer_name: format!("materialize-consumer-{}", Utc::now().timestamp()),
            stream_arn: kinc.arn,
        };
        let consumer = match client.register_stream_consumer(register_input).sync() {
            Ok(RegisterStreamConsumerOutput { consumer }) => consumer,
            Err(e) => panic!(format!("Failed to register stream consumer: {:#?}", e)),
        };
        RtKinesisConnector { consumer }
    }

    /// Creates a BYO connector
    fn create_byo_connector(
        &self,
        id: SourceInstanceId,
        sc: ExternalSourceConnector,
        timestamp_topic: String,
    ) -> ByoTimestampConsumer {
        match sc {
            ExternalSourceConnector::Kafka(kc) => ByoTimestampConsumer {
                source_name: kc.topic.clone(),
                connector: ByoTimestampConnector::Kafka(self.create_byo_kafka_connector(
                    id,
                    kc,
                    timestamp_topic,
                )),
            },
            ExternalSourceConnector::File(fc) => {
                error!("File sources are unsupported for timestamping");
                ByoTimestampConsumer {
                    source_name: String::from(""),
                    connector: ByoTimestampConnector::File(self.create_byo_file_connector(
                        id,
                        fc,
                        timestamp_topic,
                    )),
                }
            }
            ExternalSourceConnector::Kinesis(kinc) => {
                error!("Kinesis sources are unsupported for timestamping");
                ByoTimestampConsumer {
                    source_name: String::from(""),
                    connector: ByoTimestampConnector::Kinesis(self.create_byo_kinesis_connector(
                        id,
                        kinc,
                        timestamp_topic,
                    )),
                }
            }
        }
    }

    fn create_byo_file_connector(
        &self,
        _id: SourceInstanceId,
        _fc: FileSourceConnector,
        _timestamp_topic: String,
    ) -> ByoFileConnector {
        ByoFileConnector {}
    }

    fn create_byo_kinesis_connector(
        &self,
        _id: SourceInstanceId,
        _kinc: KinesisSourceConnector,
        _timestamp_topic: String,
    ) -> ByoKinesisConnector {
        ByoKinesisConnector {}
    }

    fn create_byo_kafka_connector(
        &self,
        id: SourceInstanceId,
        kc: KafkaSourceConnector,
        timestamp_topic: String,
    ) -> ByoKafkaConnector {
        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set(
                "group.id",
                &format!("materialize-byo-{}-{}", &timestamp_topic, id),
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "300000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &kc.url.to_string());

        if let Some(path) = kc.ssl_certificate_file {
            config.set("security.protocol", "ssl");
            config.set(
                "ssl.ca.location",
                path.to_str()
                    .expect("Converting ssl certificate file path failed"),
            );
        }

        let k_consumer: BaseConsumer = config.create().expect("Failed to create Kakfa consumer");
        let consumer = ByoKafkaConnector {
            consumer: k_consumer,
            timestamp_topic,
        };
        consumer
            .consumer
            .subscribe(&[&consumer.timestamp_topic])
            .unwrap();
        consumer
    }

    /// Recovers any existing timestamp updates for that (SourceId,ViewId) pair from the underlying
    /// SQL database. Notifies the coordinator of these updates
    fn rt_recover_source(&mut self, id: SourceInstanceId) -> i64 {
        let ts_updates: Vec<_> = self
            .storage()
            .prepare("SELECT timestamp, offset FROM timestamps WHERE sid = ? AND vid = ? ORDER BY timestamp")
            .expect("Failed to execute select statement")
            .query_and_then(params![SqlVal(&id.sid), SqlVal(&id.vid)], |row| -> Result<_, failure::Error> {
                let timestamp: SqlVal<u64> = row.get(0)?;
                let offset: SqlVal<i64> = row.get(1)?;
                Ok((timestamp.0, offset.0))
            })
            .expect("Failed to parse SQL result")
            .collect();

        let mut max_offset = 0;
        for row in ts_updates {
            let (timestamp, offset) = row.expect("Failed to parse SQL result");
            max_offset = if offset > max_offset {
                offset
            } else {
                max_offset
            };
            self.tx
                .unbounded_send(coord::Message::AdvanceSourceTimestamp {
                    id,
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
    fn rt_query_sources(&mut self) -> Vec<(SourceInstanceId, i64)> {
        let mut result = vec![];
        for (id, cons) in self.rt_sources.iter_mut() {
            match &cons.connector {
                RtTimestampConnector::Kafka(kc) => {
                    let watermark =
                        kc.consumer
                            .fetch_watermarks(&kc.topic, 0, Duration::from_secs(1));
                    match watermark {
                        Ok(watermark) => {
                            let high = watermark.1 - 1;
                            // Bound the next timestamp to be no more than max_increment_size in the future
                            let next_ts = if (high - cons.last_offset) > self.max_increment_size {
                                cons.last_offset + self.max_increment_size
                            } else {
                                high
                            };
                            cons.last_offset = next_ts;
                            result.push((*id, next_ts))
                        }
                        Err(e) => {
                            error!("Failed to obtain Kafka Watermark Information: {} {}", id, e);
                        }
                    }
                }
                RtTimestampConnector::File(_cons) => {
                    error!("Timestamping for File sources is not supported");
                }
                RtTimestampConnector::Kinesis(_cons) => {
                    error!("Timestamping for Kinesis sources is not supported");
                }
            }
        }
        result
    }

    /// Persist timestamp updates to the underlying storage when using the
    /// real-time timestamping logic.
    fn rt_persist_timestamp(&self, ts_updates: &[(SourceInstanceId, i64)]) {
        let storage = self.storage();
        for (id, offset) in ts_updates {
            let mut stmt = storage
                .prepare_cached(
                    "INSERT INTO timestamps (sid, vid, timestamp, offset) VALUES (?, ?, ?, ?)",
                )
                .expect(
                    "Failed to prepare insert statement into persistent store. \
                     Hint: increase the system file descriptor limit.",
                );
            while let Err(e) = stmt.execute(params![
                SqlVal(&id.sid),
                SqlVal(&id.vid),
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
    /// This may require multiple calls to the underlying now() system method, which is not443Gk
    /// guaranteed to increase monotonically
    fn rt_generate_next_timestamp(&mut self) {
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
