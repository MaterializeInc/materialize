use std::path::Path;
use std::collections::HashMap;
use futures::channel::mpsc;
use rusqlite::Connection;
use rusqlite::params;

// use catalog::SqlVal;

use expr::GlobalId;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{ClientConfig, ClientContext};
use rdkafka::{Message, Timestamp as KafkaTimestamp};
use rdkafka::error::KafkaResult;
use rdkafka::client::DefaultClientContext;

#[derive(Debug)]
pub enum Update {
    Add(GlobalId, SocketAddr, String),
    Drop(GlobalId)
}

struct TimestampConsumer {
    consumer: BaseConsumer,
    topic: String
}

pub struct Timestamper {
    // TODO(natacha, figure out correct datatype)

    // Current list of up to date sources
    sources: HashMap<GlobalId, TimestampConsumer>,

    // Connection to the underlying SQL lite instance
    sqllite: rusqlite::Connection,

    // Receiver channel for receiving source add/drop updates
    source_ic: std::sync::mpsc::Receiver<Update>,

    // Last Timestamp (necessary because not necessarily increasing otherwise)
    current_timestamp: u128,

}

impl Timestamper {

    pub fn new(path: Option<&Path>, source_ic: std::sync::mpsc::Receiver<Update>) -> Self {

        // open the underlying SQL lite connection
        let sqlite = match path {
            //TODO(natacha) improve error handling
            Some(path) => rusqlite::Connection::open(path).expect("Could not connect"),
            None => rusqlite::Connection::open_in_memory().expect("Could not connect"),
        };

        let ts = Self {
            sources:  HashMap::new(),
            sqllite: sqlite,
            current_timestamp: 0,
            source_ic
        };

        ts
    }

    pub fn update(&mut self) {
        loop {
            self.update_sources();
            let watermarks = self.query_sources();
            self.generate_next_timestamp();
            self.persist_ts(watermarks);
            self.notify_coordinator();

            // TODO(natacha): sleep for a configuratable parameter
        }
    }

    fn update_sources(&mut self) {
        // First check if there are some new source that we should
        // start checking
        while let update =  self.source_ic.try_recv().unwrap() {
            match update {
               Update::Add(id, addr, topic) => {
                   if !self.sources.contains_key(&id) {
                       // Did not know about source, must update
                       let connector = self.create_connector(topic, addr);
                       self.sources.insert(id,connector);
                   }
               },
               Update::Drop(id) => {
                   self.sources.remove(&id);
               }
            }
        }
    }

    fn create_connector(&self, topic: String, addr: SocketAddr) -> TimestampConsumer {

        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &addr.to_string());

        let k_consumer: BaseConsumer = config.create()
            .expect("Failed to create Kakfa consumer");

        TimestampConsumer {
            consumer: k_consumer,
            topic
        }
    }

    fn query_sources(&self) -> Vec<(&GlobalId,i64)> {

        let mut result = vec![];

        // TODO(natacha) make parallel
        for (id, cons) in &self.sources {
            let watermark = cons.consumer
                .fetch_watermarks(&cons.topic, 0, Duration::from_millis(0))
                .expect("Failed to obtain Kafka watermark information ");
            assert!(watermark.1 >= 0);
            result.push((id,watermark.1))
        }
        result
    }

    fn persist_ts(&self, ts_updates: Vec<(&GlobalId,i64)>)  {
        // TODO(natacha): figure out how to batch inserts
        for (id, offset) in ts_updates {
            let mut stmt = self
                .sqllite
                .prepare_cached("INSERT INTO timestamp (id, name, item) VALUES (?, ?, ?)")
                .expect("Failed to insert statement into persistent store");
            // TODO(natacha): figure out how to convert values
          //  stmt.execute(params![SqlVal(&id), SqlVal(&self.current_timestamp), SqlVal(&offset)])
          //      .expect("Failed to insert statement into persistent store");
        }
    }

    fn notify_coordinator(&self) {
        unimplemented!();
    }

    /// Generates a timestamp that is guaranteed to be monotonic
    fn generate_next_timestamp(&mut self) -> u128 {
        let mut new_ts = 0;
        while new_ts< self.current_timestamp {
            let start = SystemTime::now();
            new_ts = start.duration_since(UNIX_EPOCH)
                .expect("Time went backwards").as_millis();
        }
        new_ts
    }

}

