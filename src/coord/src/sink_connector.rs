// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use interchange::avro::get_debezium_transaction_schema;
use mz_avro::types::Value;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::{Message, Offset, TopicPartitionList};

use dataflow_types::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, KafkaSinkConsistencyConnector, SinkConnector, SinkConnectorBuilder,
};
use expr::GlobalId;
use ore::collections::CollectionExt;
use rdkafka::consumer::{BaseConsumer, Consumer};
use repr::Timestamp;

use crate::error::CoordError;

pub async fn build(
    builder: SinkConnectorBuilder,
    id: GlobalId,
) -> Result<SinkConnector, CoordError> {
    match builder {
        SinkConnectorBuilder::Kafka(k) => build_kafka(k, id).await,
        SinkConnectorBuilder::AvroOcf(a) => build_avro_ocf(a, id),
    }
}

/// Polls a message from a Kafka Source
fn get_next_message(
    consumer: &mut BaseConsumer,
    timeout: Duration,
) -> Result<Option<Vec<u8>>, anyhow::Error> {
    if let Some(result) = consumer.poll(timeout) {
        match result {
            Ok(message) => match message.payload() {
                Some(p) => Ok(Some(p.to_vec())),
                None => {
                    bail!("unexpected null payload")
                }
            },
            Err(err) => {
                bail!("Failed to process message {}", err)
            }
        }
    } else {
        Ok(None)
    }
}

/// Return the list of partition ids associated with a specific topic
fn get_partitions(
    consumer: &BaseConsumer,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = consumer.fetch_metadata(Some(&topic), timeout)?;
    if meta.topics().len() != 1 {
        bail!(
            "topic {} has {} metadata entries; expected 1",
            topic,
            meta.topics().len()
        );
    }
    let meta_topic = meta.topics().into_element();
    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }

    if meta_topic.partitions().len() == 0 {
        bail!("topic {} does not exist", topic);
    }

    Ok(meta_topic.partitions().iter().map(|x| x.id()).collect())
}

// Retrieves the latest committed timestamp from the consistency topic
async fn get_latest_ts(
    consistency_topic: &str,
    consumer: &mut BaseConsumer,
    timeout: Duration,
) -> Result<Option<Timestamp>, anyhow::Error> {
    // ensure the consistency topic has exactly one partition
    let partitions = get_partitions(&consumer, consistency_topic, timeout).with_context(|| {
        format!(
            "Unable to fetch metadata about consistency topic {}",
            consistency_topic
        )
    })?;

    if partitions.len() != 1 {
        bail!(
            "Consistency topic {} should contain a single partition, but instead contains {} partitions",
            consistency_topic, partitions.len(),
            );
    }

    let partition = partitions.into_element();

    // Seek to end-1 offset
    let mut tps = TopicPartitionList::new();
    tps.add_partition(consistency_topic, partition);
    tps.set_partition_offset(consistency_topic, partition, Offset::Beginning)?;

    consumer.assign(&tps).with_context(|| {
        format!(
            "Error seeking in consistency topic {}:{}",
            consistency_topic, partition
        )
    })?;

    // We scan from the beginning and see if we can find an END record. We have
    // to do it like this because Kafka Control Batches mess with offsets. We
    // therefore cannot simply take the last offset from the back and expect an
    // END message there. With a transactional producer, the OffsetTail(1) will
    // not point to an END message but a control message. With aborted
    // transactions, there might even be a lot of garbage at the end of the
    // topic or in between.

    let mut latest_message = None;
    while let Some(message) = get_next_message(consumer, timeout)? {
        latest_message = Some(message);
    }

    if latest_message.is_none() {
        // fetch watermarks to distinguish between a timeout reading end-1 and an empty topic
        match consumer.fetch_watermarks(consistency_topic, 0, timeout) {
            Ok((lo, hi)) => {
                if hi == 0 {
                    return Ok(None);
                } else {
                    bail!(
                        "uninitialized consistency topic {}:{}, lo/hi: {}/{}",
                        consistency_topic,
                        partition,
                        lo,
                        hi
                    );
                }
            }
            Err(e) => {
                bail!(
                    "Failed to fetch metadata while reading from consistency topic: {}",
                    e
                );
            }
        }
    }

    let latest_message = latest_message.expect("known to exist");

    // the latest valid message should be an END message. If not, things have
    // gone wrong!
    let timestamp = decode_consistency_end_record(&latest_message, consistency_topic)?;

    Ok(Some(timestamp))
}

fn decode_consistency_end_record(
    bytes: &[u8],
    consistency_topic: &str,
) -> Result<Timestamp, anyhow::Error> {
    // The first 5 bytes are reserved for the schema id/schema registry information
    let mut bytes = &bytes[5..];
    let record = mz_avro::from_avro_datum(get_debezium_transaction_schema(), &mut bytes)
        .context("Failed to decode consistency topic message")?;

    if let Value::Record(r) = record {
        let m: HashMap<String, Value> = r.into_iter().collect();
        let status = m.get("status");
        let id = m.get("id");
        match (status, id) {
            (Some(Value::String(status)), Some(Value::String(id))) if status == "END" => {
                if let Ok(ts) = id.parse::<u64>() {
                    Ok(Timestamp::from(ts))
                } else {
                    bail!(
                        "Malformed consistency record, failed to parse timestamp {} in topic {}",
                        id,
                        consistency_topic
                    );
                }
            }
            _ => {
                bail!(
                    "Malformed consistency record in topic {}, expected END with a timestamp but record was {:?}, tried matching {:?} {:?}",
                    consistency_topic, m, status, id);
            }
        }
    } else {
        bail!("Failed to decode consistency topic message, was not a parseable record");
    }
}

#[allow(clippy::too_many_arguments)]
async fn register_kafka_topic(
    client: &AdminClient<DefaultClientContext>,
    topic: &str,
    mut partition_count: i32,
    mut replication_factor: i32,
    ccsr: &ccsr::Client,
    value_schema: &str,
    key_schema: Option<&str>,
    succeed_if_exists: bool,
) -> Result<(Option<i32>, i32), CoordError> {
    // if either partition count or replication factor should be defaulted to the broker's config
    // (signaled by a value of -1), explicitly poll the broker to discover the defaults.
    // Newer versions of Kafka can instead send create topic requests with -1 and have this happen
    // behind the scenes, but this is unsupported and will result in errors on pre-2.4 Kafka
    if partition_count == -1 || replication_factor == -1 {
        let metadata = client
            .inner()
            .fetch_metadata(None, Duration::from_secs(5))
            .with_context(|| {
                format!(
                    "error fetching metadata when creating new topic {} for sink",
                    topic
                )
            })?;

        if metadata.brokers().len() == 0 {
            coord_bail!("zero brokers discovered in metadata request");
        }

        let broker = metadata.brokers()[0].id();
        let configs = client
            .describe_configs(
                &[ResourceSpecifier::Broker(broker)],
                &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            )
            .await
            .with_context(|| {
                format!(
                    "error fetching configuration from broker {} when creating new topic {} for sink",
                    broker,
                    topic
                )
        })?;

        if configs.len() != 1 {
            coord_bail!(
                "error creating topic {} for sink: broker {} returned {} config results, but one was expected",
                topic,
                broker,
                configs.len()
            );
        }

        let config = configs.into_element().map_err(|e| {
            anyhow!(
                "error reading broker configuration when creating topic {} for sink: {}",
                topic,
                e
            )
        })?;

        for entry in config.entries {
            if entry.name == "num.partitions" && partition_count == -1 {
                if let Some(s) = entry.value {
                    partition_count = s.parse::<i32>().with_context(|| {
                        format!(
                            "default partition count {} cannot be parsed into an integer",
                            s
                        )
                    })?;
                }
            } else if entry.name == "default.replication.factor" && replication_factor == -1 {
                if let Some(s) = entry.value {
                    replication_factor = s.parse::<i32>().with_context(|| {
                        format!(
                            "default replication factor {} cannot be parsed into an integer",
                            s
                        )
                    })?;
                }
            }
        }

        if partition_count == -1 {
            coord_bail!("default was requested for partition_count, but num.partitions was not found in broker config");
        }

        if replication_factor == -1 {
            coord_bail!("default was requested for replication_factor, but default.replication.factor was not found in broker config");
        }
    }

    let res = client
        .create_topics(
            &[NewTopic::new(
                &topic,
                partition_count,
                TopicReplication::Fixed(replication_factor),
            )],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|| format!("error creating new topic {} for sink", topic))?;
    if res.len() != 1 {
        coord_bail!(
            "error creating topic {} for sink: \
             kafka topic creation returned {} results, but exactly one result was expected",
            topic,
            res.len()
        );
    }
    if let Err((_, e)) = res.into_element() {
        // if the topic already exists and we reuse_existing, don't fail - instead proceed
        // to read the schema
        if !(succeed_if_exists && e == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists) {
            coord_bail!("error creating topic {} for sink: {}", topic, e)
        }
    }

    // Publish value schema for the topic.
    //
    // TODO(benesch): do we need to delete the Kafka topic if publishing the
    // schema fails?
    let value_schema_id = ccsr
        .publish_schema(&format!("{}-value", topic), value_schema)
        .await
        .context("unable to publish value schema to registry in kafka sink")?;

    let key_schema_id = if let Some(key_schema) = key_schema {
        Some(
            ccsr.publish_schema(&format!("{}-key", topic), key_schema)
                .await
                .context("unable to publish key schema to registry in kafka sink")?,
        )
    } else {
        None
    };

    Ok((key_schema_id, value_schema_id))
}

async fn build_kafka(
    builder: KafkaSinkConnectorBuilder,
    id: GlobalId,
) -> Result<SinkConnector, CoordError> {
    let topic = if builder.exactly_once {
        format!("{}-{}", builder.topic_prefix, id)
    } else {
        format!(
            "{}-{}-{}",
            builder.topic_prefix, id, builder.topic_suffix_nonce
        )
    };

    // Create Kafka topic with single partition.
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &builder.broker_addrs.to_string());
    for (k, v) in builder.config_options.iter() {
        config.set(k, v);
    }
    let client = config
        .create::<AdminClient<_>>()
        .context("creating admin client failed")?;
    let ccsr = builder.ccsr_config.build()?;

    let (key_schema_id, value_schema_id) = register_kafka_topic(
        &client,
        &topic,
        builder.partition_count,
        builder.replication_factor,
        &ccsr,
        &builder.value_schema,
        builder.key_schema.as_deref(),
        builder.exactly_once,
    )
    .await
    .context("error registering kafka topic for sink")?;

    let consistency = if let Some(consistency_value_schema) = builder.consistency_value_schema {
        let consistency_topic = format!("{}-consistency", topic);

        // create consistency topic/schema and retrieve schema id
        let (_, consistency_schema_id) = register_kafka_topic(
            &client,
            &consistency_topic,
            1,
            builder.replication_factor,
            &ccsr,
            &consistency_value_schema,
            None,
            builder.exactly_once,
        )
        .await
        .context("error registering kafka consistency topic for sink")?;

        // get latest committed timestamp from consistencty topic
        let gate_ts = if builder.exactly_once {
            let mut consumer_config = config.clone();
            consumer_config
                .set("group.id", format!("materialize-bootstrap-{}", topic))
                .set("isolation.level", "read_committed")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest");

            let mut consumer = consumer_config
                .create::<BaseConsumer>()
                .context("creating consumer client failed")?;

            get_latest_ts(&consistency_topic, &mut consumer, Duration::from_secs(5))
                .await
                .context("error restarting from existing kafka consistency topic for sink")?
        } else {
            None
        };

        Some(KafkaSinkConsistencyConnector {
            topic: consistency_topic,
            schema_id: consistency_schema_id,
            gate_ts,
        })
    } else {
        None
    };

    Ok(SinkConnector::Kafka(KafkaSinkConnector {
        key_schema_id,
        value_schema_id,
        topic,
        addrs: builder.broker_addrs,
        key_desc_and_indices: builder.key_desc_and_indices,
        value_desc: builder.value_desc,
        consistency,
        exactly_once: builder.exactly_once,
        fuel: builder.fuel,
        config_options: builder.config_options,
    }))
}

fn build_avro_ocf(
    builder: AvroOcfSinkConnectorBuilder,
    id: GlobalId,
) -> Result<SinkConnector, CoordError> {
    let mut name = match builder.path.file_stem() {
        None => coord_bail!(
            "unable to read file name from path {}",
            builder.path.display()
        ),
        Some(stem) => stem.to_owned(),
    };
    name.push("-");
    name.push(id.to_string());
    name.push("-");
    name.push(builder.file_name_suffix);
    if let Some(extension) = builder.path.extension() {
        name.push(".");
        name.push(extension);
    }

    let path = builder.path.with_file_name(name);

    // Try to create a new sink file
    let _ = OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| {
            anyhow!(
                "unable to create avro ocf sink file {} : {}",
                path.display(),
                e
            )
        })?;
    Ok(SinkConnector::AvroOcf(AvroOcfSinkConnector {
        path,
        value_desc: builder.value_desc,
    }))
}
