// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use avro::{
    self,
    types::{Record, Value},
    Schema,
};
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::executor::block_on;
use futures::future::{self};
use futures::stream::{FuturesUnordered, TryStreamExt};
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::error::RDKafkaError;
use rdkafka::producer::FutureRecord;

const CONSISTENCY_SCHEMA: &str = "{
    \"name\": \"materialize.byo.consistency\",
    \"type\": \"record\",
    \"fields\": [
        {
          \"name\": \"source\",
          \"type\": \"string\"
        },
        {
          \"name\": \"partition_count\",
          \"type\": \"int\"
        },
        {
          \"name\": \"partition_id\",
          \"type\": [\"int\",\"string\"]
        },
        {
           \"name\": \"timestamp\",
           \"type\": \"long\"
        },
        {
          \"name\": \"offset\",
          \"type\": \"long\"
        }
    ]
 }";

pub struct Config {
    pub kafka_addr: Option<String>,
}

pub struct State {
    kafka_admin: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
    kafka_admin_opts: rdkafka::admin::AdminOptions,
    kafka_producer: rdkafka::producer::FutureProducer<rdkafka::client::DefaultClientContext>,
    consistency_schema: Option<Schema>,
}

impl State {
    pub fn new(config: Config, byo: bool) -> Result<Self, String> {
        let (kafka_admin, kafka_admin_opts, kafka_producer) = {
            use rdkafka::admin::{AdminClient, AdminOptions};
            use rdkafka::client::DefaultClientContext;
            use rdkafka::config::ClientConfig;
            use rdkafka::producer::FutureProducer;

            let addr = config
                .kafka_addr
                .as_deref()
                .unwrap_or_else(|| "localhost:9092");

            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &addr);

            let admin: AdminClient<DefaultClientContext> = config
                .create()
                .map_err(|e| format!("Error opening Kafka connection {}", e.to_string()))?;

            let admin_opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

            let producer: FutureProducer = config.create().map_err(|e| {
                format!("Error opening Kafka producer connection: {}", e.to_string())
            })?;

            (admin, admin_opts, producer)
        };
        let consistency_schema = if byo {
            let json_schema = serde_json::from_str(CONSISTENCY_SCHEMA).unwrap();
            Some(Schema::parse(&json_schema).unwrap())
        } else {
            None
        };
        Ok(State {
            kafka_admin,
            kafka_admin_opts,
            kafka_producer,
            consistency_schema,
        })
    }

    pub fn create_topic(
        &mut self,
        topic_name: &str,
        partitions: i32,
        replication: i32,
        topic_configs: Vec<(String, String)>,
    ) -> Result<(), String> {
        println!(
            "Creating Kafka topic {} with partition count of {}",
            topic_name, partitions
        );
        let mut new_topic = NewTopic::new(
            &topic_name,
            partitions,
            TopicReplication::Fixed(replication),
        );
        let mut explicit_cleanup = false;
        for (name, value) in topic_configs.iter() {
            if name == "cleanup.policy" {
                explicit_cleanup = true;
            }
            new_topic = new_topic.set(name, value);
        }
        if !explicit_cleanup {
            // enable log-compaction by default
            new_topic = new_topic.set("cleanup.policy", "compact");
        }
        let res = block_on(
            self.kafka_admin
                .create_topics(&[new_topic], &self.kafka_admin_opts),
        );
        let res = match res {
            Err(err) => return Err(err.to_string()),
            Ok(res) => res,
        };
        if res.len() != 1 {
            return Err(format!(
                "kafka topic creation returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        match res.into_iter().next().unwrap() {
            Ok(_) | Err((_, RDKafkaError::TopicAlreadyExists)) => Ok(()),
            Err((_, err)) => Err(err.to_string()),
        }?;

        // Topic creation is asynchronous, and if we don't wait for it to
        // complete, we might produce a message (below) that causes it to
        // get automatically created with multiple partitions. (Since
        // multiple partitions have no ordering guarantees, this violates
        // many assumptions that our tests make.)
        let mut i = 0;
        loop {
            let res = (|| {
                let metadata = self
                    .kafka_producer
                    .client()
                    // N.B. It is extremely important not to ask specifically
                    // about the topic here, even though the API supports it!
                    // Asking about the topic will create it automatically...
                    // with the wrong number of partitions. Yes, this is
                    // unbelievably horrible.
                    .fetch_metadata(None, Some(Duration::from_secs(1)))
                    .map_err(|e| e.to_string())?;
                if metadata.topics().is_empty() {
                    return Err("metadata fetch returned no topics".to_string());
                }
                let topic = match metadata.topics().iter().find(|t| t.name() == topic_name) {
                    Some(topic) => topic,
                    None => {
                        return Err(format!(
                            "metadata fetch did not return topic {}",
                            topic_name,
                        ))
                    }
                };
                if topic.partitions().is_empty() {
                    return Err("metadata fetch returned a topic with no partitions".to_string());
                } else if topic.partitions().len() as i32 != partitions {
                    return Err(format!(
                        "topic {} was created with {} partitions when exactly {} was expected",
                        topic_name,
                        topic.partitions().len(),
                        partitions
                    ));
                }
                Ok(())
            })();
            match res {
                Ok(()) => break,
                Err(e) if i == 6 => return Err(e),
                _ => {
                    thread::sleep(Duration::from_millis(100 * 2_u64.pow(i)));
                    i += 1;
                }
            }
        }
        Ok(())
    }

    pub fn create_consistency_topic(
        &mut self,
        topic_name: &str,
        replication: i32,
    ) -> Result<(), String> {
        //disable deleting old segments of the stream
        self.create_topic(
            topic_name,
            1,
            replication,
            vec![
                ("cleanup.policy".to_owned(), "delete".to_owned()),
                ("retention.ms".to_owned(), "-1".to_owned()),
            ],
        )
    }

    pub fn ingest(
        &mut self,
        topic_name: &str,
        partition: i32,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<(), String> {
        let val_buf = if let Some(value) = value {
            value.as_bytes().to_vec()
        } else {
            Vec::new()
        };
        let key_buf = if let Some(key) = key {
            key.as_bytes().to_vec()
        } else {
            Vec::new()
        };
        self.ingest_inner(topic_name, partition, key_buf, val_buf)
    }

    fn ingest_inner(
        &mut self,
        topic_name: &str,
        partition: i32,
        key_buf: Vec<u8>,
        val_buf: Vec<u8>,
    ) -> Result<(), String> {
        let futs = FuturesUnordered::new();
        let record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
            .payload(&val_buf)
            .key(&key_buf)
            .partition(partition);

        futs.push(self.kafka_producer.send(record, Duration::from_secs(1)));
        block_on(
            futs.map_err(|(e, _)| e.to_string())
                .try_for_each(|_| future::ok(())),
        )
    }

    pub fn ingest_consistency(
        &mut self,
        consistency_name: &str,
        topic_name: &str,
        timestamp: i64,
        offset: i64,
    ) -> Result<(), String> {
        let schema = &self.consistency_schema.as_ref().unwrap();
        let mut val = Record::new(schema.top_node()).unwrap();
        val.put("source", topic_name);
        val.put("partition_count", 1i32);
        val.put(
            "partition_id",
            Value::Union {
                index: 0,
                inner: Box::new(Value::Int(0i32)),
                n_variants: 2,
                null_variant: None,
            },
        );
        val.put("timestamp", timestamp);
        val.put("offset", offset);
        let mut val_buf = vec![];
        // The first byte is a magic byte (0) that indicates the Confluent
        // serialization format version, and the next four bytes are a
        // 32-bit schema ID.
        //
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        val_buf.write_u8(0).unwrap();
        val_buf.write_i32::<NetworkEndian>(1).unwrap();
        val_buf.extend(avro::to_avro_datum(schema, val).map_err(|e| e.to_string())?);

        self.ingest_inner(consistency_name, 0, vec![], val_buf)
    }
}
