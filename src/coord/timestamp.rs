// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use rusqlite::{params, NO_PARAMS};

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::str;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catalog::sql::SqlVal;
use expr::GlobalId;
use expr::SourceInstanceId;

use catalog::names::FullName;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;

use log::error;

#[derive(Debug)]
pub enum TimestampMessage {
    Add(FullName, FullName, SourceInstanceId, SocketAddr, String),
    DropView(FullName, GlobalId),
    DropInstance(FullName, FullName, SourceInstanceId),
    BatchedUpdate(u64, Vec<(SourceInstanceId, i64)>),
    Update(SourceInstanceId, u64, i64),
}

pub struct TimestampChannel {
    pub sender: std::sync::mpsc::Sender<TimestampMessage>,
    pub receiver: std::sync::mpsc::Receiver<TimestampMessage>,
}

struct TimestampConsumer {
    consumer: BaseConsumer,
    topic: String,
    last_offset: i64,
}

pub struct Timestamper {
    // Current list of up to date sources
    sources: HashMap<SourceInstanceId, TimestampConsumer>,

    // Current list of source names to instance ids
    source_names: HashMap<(String, String), SourceInstanceId>,

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

    // Optional consumer for timestamps
    ts_source: Option<TimestampConsumer>,

    // Buffered timestamp messages for which we do not know source mappings
    ts_buffered_updates: HashMap<(String, String), Vec<(u64, i64)>>,
}

/// Creates a Kafka connector
fn create_connector(addr: SocketAddr, topic: String, last_offset: i64) -> TimestampConsumer {
    let mut config = ClientConfig::new();
    config
        .set("auto.offset.reset", "smallest")
        .set("group.id", &format!("materialize-{}", &topic))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("max.poll.interval.ms", "300000") // 5 minutes
        .set("fetch.message.max.bytes", "134217728")
        .set("enable.sparse.connections", "true")
        .set("bootstrap.servers", &addr.to_string());

    let k_consumer: BaseConsumer = config.create().expect("Failed to create Kakfa consumer");
    TimestampConsumer {
        consumer: k_consumer,
        topic,
        last_offset,
    }
}

impl Timestamper {
    pub fn new(
        timestamp_frequency: Duration,
        max_ts_increment: i64,
        path: Option<&Path>,
        channel: TimestampChannel,
        ts_source: Option<(SocketAddr, String)>,
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
            .prepare(
                "CREATE TABLE IF NOT EXISTS timestamp (
            sid blob NOT NULL,
            vid blob NOT NULL,
            timestamp unsigned bigint NOT NULL,
            offset blob NOT NULL,
            PRIMARY KEY (sid,vid,timestamp));",
            )
            .expect("Failed to prepare CREATE statement")
            .execute(params![])
            .expect("Failed to CREATE timestamp table");

        // Recover existing data by running max on the timestamp count. This will ensure that
        // there will never be two duplicate entries and that there is a continuous stream
        // of timestamp updates across reboots
        let max_ts = sqlite
            .prepare("SELECT MAX(timestamp) from timestamp ")
            .expect("Failed to prepare statement")
            .query_row(NO_PARAMS, |row| {
                let res: Result<SqlVal<u64>, _> = row.get(2);
                match res {
                    Ok(res) => Ok(res.0),
                    _ => Ok(0),
                }
            })
            .expect("Failure to parse timestamp");

        Self {
            sources: HashMap::new(),
            sqllite: sqlite,
            coord_channel: channel,
            current_timestamp: max_ts,
            timestamp_frequency,
            max_increment_size: max_ts_increment,
            ts_source: ts_source.map(|(addr, topic)| {
                let consumer = create_connector(addr, topic.clone(), 0);
                consumer.consumer.subscribe(&[&topic]).unwrap();
                consumer
            }),
            source_names: HashMap::new(),
            ts_buffered_updates: HashMap::new(),
        }
    }

    /// Recovers any existing timestamp updates for that (SourceId,ViewId) pair from the underlying
    /// SQL database. Notifies the coordinator of these updates
    fn recover_source(&mut self, id: SourceInstanceId) -> i64 {
        let ts_updates: Vec<_> =  self
            .sqllite
            .prepare("SELECT timestamp, offset FROM timestamp where sid = ? and vid = ? ORDER BY timestamp")
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

    /// Run the update function in a loop at the specified frequency. Acquires timestamps using
    /// either 1) the Kafka topic ground truth 2) real-time
    pub fn update(&mut self) {
        loop {
            thread::sleep(self.timestamp_frequency);
            self.update_sources();
            if self.ts_source.is_none() {
                // Did not specify a Kafka Timestamp Source. Use wall-clock
                // time to determine timestamps
                self.update_ts_from_time();
            } else {
                // Specified a Kafka Timestamp Source. Use Kafka Stream to specify timestamp
                self.update_ts_from_source();
            }
        }
    }

    /// Implements the real-time timestamping logic
    fn update_ts_from_time(&mut self) {
        let watermarks = self.query_sources();
        self.generate_next_timestamp();
        self.persist_timestamp(&watermarks);
        self.notify_coordinator(watermarks);
    }

    fn get_next_message(&mut self) -> Option<Vec<u8>> {
        if let Some(result) = self
            .ts_source
            .as_ref()
            .expect("Cannot poll Kafka with null consumer")
            .consumer
            .poll(Duration::from_millis(0))
        {
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

    /// Implements the timestamping logic that uses a Connector as ground truth
    fn update_ts_from_source(&mut self) {
        // Expected format is SourceName-ViewName-Offset-Timestamp
        while let Some(payload) = self.get_next_message() {
            let st = str::from_utf8(&payload);
            match st {
                Ok(timestamp) => {
                    // Extract timestamp from payload
                    let update = self.extract_ts_update(timestamp);
                    if let Some(update) = update {
                        if let Some(id) =
                            self.source_names.get(&(update.0.clone(), update.1.clone()))
                        {
                            self.coord_channel
                                .sender
                                .send(TimestampMessage::Update(id.clone(), update.2, update.3))
                                .expect("Failed to send timestamp update to coordinator");
                        } else {
                            // No mapping to source id found, must buffer updates
                            let buffered = self
                                .ts_buffered_updates
                                .entry((update.0, update.1))
                                .or_insert_with(|| vec![]);
                            buffered.push((update.2, update.3));
                        }
                    } else {
                        error!("incorrect payload format. Expected: sourcename-viewname-offset-ts")
                    }
                }
                Err(err) => error!("incorrect payload format: {}", err),
            }
        }
    }

    /// Extract a (SourceName,ViewName,TS) tuple from a Kakfa or file payload.
    /// The expected format is SourceName-ViewName-Offset-TS
    fn extract_ts_update(&mut self, payload: &str) -> Option<(String, String, u64, i64)> {
        let split: Vec<&str> = payload.split('-').collect();
        if split.len() != 4 {
            error!("incorrect payload format. Expected: SourceName/ViewName/Offset/TS");
            return None;
        }
        let source_name = String::from(split[0]);
        let view_name = String::from(split[1]);
        let offset = match split[2].parse::<i64>() {
            Ok(i) => i,
            Err(err) => {
                error!("incorrect timestamp format {}", err);
                return None;
            }
        };
        let ts = match split[3].parse::<u64>() {
            Ok(i) => i,
            Err(err) => {
                error!("incorrect timestamp format {}", err);
                return None;
            }
        };
        Some((source_name, view_name, ts, offset))
    }

    /// Updates list of timestamp sources based on coordinator information. If using
    /// using the real-time timestamping logic, then maintain a list of Kafka consumers
    /// that poll topics to check how much data has been generated. If using the Kafka
    /// source timestamping logic, then keep a mapping of (name,id) to translate user-
    /// defined timestamps to GlobalIds
    fn update_sources(&mut self) {
        // First check if there are some new source that we should
        // start checking
        while let Ok(update) = self.coord_channel.receiver.try_recv() {
            match update {
                TimestampMessage::Add(source_name, view_name, id, addr, topic) => {
                    if !self.sources.contains_key(&id) {
                        // Did not know about source, must update
                        if self.ts_source.is_none() {
                            let last_offset = self.recover_source(id);
                            let connector = create_connector(addr, topic, last_offset);
                            self.sources.insert(id, connector);
                        } else {
                            self.source_names
                                .insert((source_name.item.clone(), view_name.item.clone()), id);
                            // Check if they are any updates that have been buffered which we can now
                            // assign timestamps to
                            if let Some(buffered_msg) = self
                                .ts_buffered_updates
                                .remove(&(source_name.item, view_name.item))
                            {
                                for msg in buffered_msg {
                                    self.coord_channel
                                        .sender
                                        .send(TimestampMessage::Update(id, msg.0, msg.1))
                                        .expect("Failed to send timestamp update to coordinator");
                                }
                            }
                        }
                    }
                }
                TimestampMessage::DropView(view_name, vid) => {
                    self.sqllite
                        .prepare_cached("DELETE from timestamp where vid = ?")
                        .expect("Failed to prepare delete statement")
                        .execute(params![SqlVal(&vid)])
                        .expect("Failed to execute delete statement");
                    if self.ts_source.is_none() {
                        self.sources.retain(|&k, _| k.vid != vid);
                    } else {
                        self.source_names
                            .retain(|(_, vname), _| vname != &view_name.item);
                        self.ts_buffered_updates
                            .retain(|(_, vname), _| vname != &view_name.item);
                    }
                }
                TimestampMessage::DropInstance(source_name, view_name, id) => {
                    self.sqllite
                        .prepare_cached("DELETE from timestamp where sid = ? and vid = ? ")
                        .expect("Failed to prepare delete statement")
                        .execute(params![SqlVal(&id.sid), SqlVal(&id.vid)])
                        .expect("Failed to execute delete statement");
                    if self.ts_source.is_none() {
                        self.sources.remove(&id);
                    } else {
                        self.source_names
                            .remove(&(source_name.item.clone(), view_name.item.clone()));
                        self.ts_buffered_updates
                            .remove(&(source_name.item, view_name.item));
                    }
                }
                _ => {
                    // this should never happen
                }
            }
        }
    }

    /// Query monitored sources for the current max offset that has been generated for that source
    /// Set the new timestamped offset to min(max_offset, last_offset + increment_size): this ensures
    /// that we never create an overly large batch of messages for the same timestamp (which would
    /// prevent views from becoming visible in a timely fashion)
    fn query_sources(&mut self) -> Vec<(SourceInstanceId, i64)> {
        let mut result = vec![];
        for (id, cons) in self.sources.iter_mut() {
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
                    error!("Failed to obtain Kafka Watermark Information: {}", e);
                }
            }
        }
        result
    }

    /// Persist timestamp updates to the underlying SQLlite store when using the real-time
    /// timestamping logic
    fn persist_timestamp(&self, ts_updates: &[(SourceInstanceId, i64)]) {
        for (id, offset) in ts_updates {
            let mut stmt = self
                .sqllite
                .prepare_cached(
                    "INSERT INTO timestamp (sid, vid, timestamp, offset) VALUES (?, ?, ?, ?)",
                )
                .expect("Failed to insert statement into persistent store");
            stmt.execute(params![
                SqlVal(&id.sid),
                SqlVal(&id.vid),
                SqlVal(&self.current_timestamp),
                SqlVal(&offset)
            ])
            .expect("Failed to insert statement into persistent store");
        }
    }

    /// Notify coordinator of a batch of timestamp updates, all with the same timestamp
    fn notify_coordinator(&self, ts_updates: Vec<(SourceInstanceId, i64)>) {
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
    fn generate_next_timestamp(&mut self) {
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
