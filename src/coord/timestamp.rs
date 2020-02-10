// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rusqlite::{params, NO_PARAMS};

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::str;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catalog::sql::SqlVal;
use expr::SourceInstanceId;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;

use dataflow_types::Consistency;

use log::{error, info};

#[derive(Debug)]
pub enum TimestampMessage {
    Add(SourceInstanceId, SocketAddr, String, Consistency),
    DropInstance(SourceInstanceId),
    BatchedUpdate(u64, Vec<(SourceInstanceId, i64)>),
    Update(SourceInstanceId, u64, i64),
    Shutdown,
}

pub struct TimestampChannel {
    pub sender: std::sync::mpsc::Sender<TimestampMessage>,
    pub receiver: std::sync::mpsc::Receiver<TimestampMessage>,
}

/// Timestamp consumer: wrapper around Kafka consumer that stores necessary information
/// about topics and offset for real-time consistency
struct RtTimestampConsumer {
    consumer: BaseConsumer,
    topic: String,
    last_offset: i64,
}

struct ByoTimestampConsumer {
    consumer: BaseConsumer,
    topic: String,
    timestamp_topic: String,
}

fn byo_query_source(consumer: &mut ByoTimestampConsumer, max_increment_size: i64) -> Vec<Vec<u8>> {
    let mut messages = vec![];
    let mut msg_count = 0;
    while let Some(payload) = get_next_message(consumer) {
        messages.push(payload);
        msg_count += 1;
        if msg_count == max_increment_size {
            // Make sure to bound the number of timestamp updates we have at once,
            // to avoid overflowing the system
            break;
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
                if topic_name == consumer.topic {
                    updates.push((ts, offset))
                }
            }
            Err(err) => error!("incorrect payload format: {}", err),
        }
    }
    updates
}

fn byo_notify_coordinator(
    id: SourceInstanceId,
    updates: Vec<(u64, i64)>,
    coord_channel: &TimestampChannel,
) {
    for (ts, offset) in updates {
        coord_channel
            .sender
            .send(TimestampMessage::Update(id, ts, offset))
            .expect("Failed to send update to coordinator");
    }
}

/// Polls a message from a Kafka Source
fn get_next_message(consumer: &mut ByoTimestampConsumer) -> Option<Vec<u8>> {
    if let Some(result) = consumer.consumer.poll(Duration::from_millis(60)) {
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
    sqllite: rusqlite::Connection,

    // Channel with coordinator
    coord_channel: TimestampChannel,

    // Last Timestamp (necessary because not necessarily increasing otherwise)
    current_timestamp: u64,

    // Frequency at which thread should run
    timestamp_frequency: Duration,

    // Max increment size
    max_increment_size: i64,
}

impl Timestamper {
    pub fn new(
        timestamp_frequency: Duration,
        max_ts_increment: i64,
        path: Option<&Path>,
        channel: TimestampChannel,
    ) -> Self {
        // open the underlying SQL lite connection
        let sqlite = match path {
            Some(path) => {
                fs::create_dir_all(path).expect("Failed to open SQL file");
                let full_path = path.join("catalog");
                rusqlite::Connection::open(full_path).expect("Could not connect")
            }
            None => rusqlite::Connection::open_in_memory().expect("Could not connect"),
        };

        sqlite
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS timestamps (
    sid blob NOT NULL,
    vid blob NOT NULL,
    timestamp integer NOT NULL,
    offset blob NOT NULL,
    PRIMARY KEY (sid, vid, timestamp)
)",
            )
            .expect("Failed to CREATE timestamp table");

        // Recover existing data by running max on the timestamp count. This will ensure that
        // there will never be two duplicate entries and that there is a continuous stream
        // of timestamp updates across reboots
        let max_ts = sqlite
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
            timestamp_frequency.as_millis()
        );

        Self {
            rt_sources: HashMap::new(),
            byo_sources: HashMap::new(),
            sqllite: sqlite,
            coord_channel: channel,
            current_timestamp: max_ts,
            timestamp_frequency,
            max_increment_size: max_ts_increment,
        }
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
        self.rt_notify_coordinator(watermarks);
    }

    /// Updates list of timestamp sources based on coordinator information. If using
    /// using the real-time timestamping logic, then maintain a list of Kafka consumers
    /// that poll topics to check how much data has been generated. If using the Kafka
    /// source timestamping logic, then keep a mapping of (name,id) to translate user-
    /// defined timestamps to GlobalIds
    fn update_sources(&mut self) -> bool {
        // First check if there are some new source that we should
        // start checking
        while let Ok(update) = self.coord_channel.receiver.try_recv() {
            match update {
                TimestampMessage::Add(id, addr, topic, consistency) => {
                    if !self.rt_sources.contains_key(&id) && !self.byo_sources.contains_key(&id) {
                        // Did not know about source, must update
                        match consistency {
                            Consistency::RealTime => {
                                info!("Timestamping Source {} with Real Time Consistency", id);
                                let last_offset = self.rt_recover_source(id);
                                let connector =
                                    self.create_rt_connector(id, addr, topic, last_offset);
                                self.rt_sources.insert(id, connector);
                            }
                            Consistency::BringYourOwn(consistency_topic) => {
                                info!("Timestamping Source {} with BYO Consistency. Topic: {}, Consistency Topic: {}", id, topic, consistency_topic);
                                let consumer =
                                    self.create_byo_connector(addr, topic, id, consistency_topic);
                                self.byo_sources.insert(id, consumer);
                            }
                        }
                    }
                }
                TimestampMessage::DropInstance(id) => {
                    info!("Dropping Timestamping for Source {}", id);
                    self.sqllite
                        .prepare_cached("DELETE FROM timestamps WHERE sid = ? AND vid = ?")
                        .expect("Failed to prepare delete statement")
                        .execute(params![SqlVal(&id.sid), SqlVal(&id.vid)])
                        .expect("Failed to execute delete statement");
                    self.rt_sources.remove(&id);
                    self.byo_sources.remove(&id);
                }
                TimestampMessage::Shutdown => return true,
                _ => {
                    // this should never happen
                }
            }
        }
        false
    }

    /// Implements the byo timestamping logic
    fn update_byo_timestamp(&mut self) {
        for (id, byo_consumer) in &mut self.byo_sources {
            // Get the next set of messages from the Consistency topic
            let messages = byo_query_source(byo_consumer, self.max_increment_size);
            // Extract the timestamp updates for this topic only
            let ts_updates = byo_extract_ts_update(byo_consumer, messages);
            // Notify coordinator of updates
            byo_notify_coordinator(id.clone(), ts_updates, &self.coord_channel);
        }
    }

    /// Creates a RT Kafka connector
    fn create_rt_connector(
        &self,
        id: SourceInstanceId,
        addr: SocketAddr,
        topic: String,
        last_offset: i64,
    ) -> RtTimestampConsumer {
        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-rt-{}-{}", &topic, id))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "300000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &addr.to_string());

        let k_consumer: BaseConsumer = config.create().expect("Failed to create Kakfa consumer");
        RtTimestampConsumer {
            consumer: k_consumer,
            topic,
            last_offset,
        }
    }

    /// Creates a RT Kafka connector
    fn create_byo_connector(
        &self,
        addr: SocketAddr,
        topic: String,
        id: SourceInstanceId,
        timestamp_topic: String,
    ) -> ByoTimestampConsumer {
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
            .set("bootstrap.servers", &addr.to_string());

        let k_consumer: BaseConsumer = config.create().expect("Failed to create Kakfa consumer");
        let consumer = ByoTimestampConsumer {
            consumer: k_consumer,
            topic,
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
            .sqllite
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
            let (ts, offset) = row.expect("Failed to parse SQL result");
            max_offset = if offset > max_offset {
                offset
            } else {
                max_offset
            };
            self.coord_channel
                .sender
                .send(TimestampMessage::Update(id, ts, offset))
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
            let watermark = cons
                .consumer
                .fetch_watermarks(&cons.topic, 0, Duration::from_secs(1));
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
        result
    }

    /// Persist timestamp updates to the underlying SQLlite store when using the real-time
    /// timestamping logic
    fn rt_persist_timestamp(&self, ts_updates: &[(SourceInstanceId, i64)]) {
        for (id, offset) in ts_updates {
            let mut stmt = self
                .sqllite
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

    /// Notify coordinator of a batch of timestamp updates, all with the same timestamp
    /// Used in real-time timestamping logic, where a set of sources get assigned the same
    /// timestamp
    fn rt_notify_coordinator(&self, ts_updates: Vec<(SourceInstanceId, i64)>) {
        self.coord_channel
            .sender
            .send(TimestampMessage::BatchedUpdate(
                self.current_timestamp,
                ts_updates,
            ))
            .expect("Failed to send timestamp update to coordinator");
    }

    /// Generates a timestamp that is guaranteed to be monotonically increasing.
    /// This may require multiple calls to the underlying now() system method, which is not
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
