// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use rdkafka::config::ClientConfig;
use rdkafka::{Message, Offset, TopicPartitionList};

use ::kafka_util::client::MzClientContext;
use dataflow_types::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, KafkaSinkConnectorRetention, KafkaSinkConsistencyConnector,
    PublishedSchemaInfo, SinkConnector, SinkConnectorBuilder,
};
use expr::GlobalId;
use ore::collections::CollectionExt;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
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
) -> Result<Option<(Vec<u8>, i64)>, anyhow::Error> {
    if let Some(result) = consumer.poll(timeout) {
        match result {
            Ok(message) => match message.payload() {
                Some(p) => Ok(Some((p.to_vec(), message.offset()))),
                None => bail!("unexpected null payload"),
            },
            Err(KafkaError::PartitionEOF(_)) => Ok(None),
            Err(err) => bail!("Failed to process message {}", err),
        }
    } else {
        Ok(None)
    }
}

// Retrieves the latest committed timestamp from the consistency topic
fn get_latest_ts(
    consistency_topic: &str,
    mut consumer_config: ClientConfig,
    timeout: Duration,
) -> Result<Option<Timestamp>, anyhow::Error> {
    let mut consumer = consumer_config
        .set(
            "group.id",
            format!("materialize-bootstrap-{}", consistency_topic),
        )
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create::<BaseConsumer>()
        .context("creating consumer client failed")?;

    // ensure the consistency topic has exactly one partition
    let partitions = sql::kafka_util::get_partitions(&consumer, consistency_topic, timeout)
        .with_context(|| {
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

    // We scan from the beginning and see if we can find an END record. We have
    // to do it like this because Kafka Control Batches mess with offsets. We
    // therefore cannot simply take the last offset from the back and expect an
    // END message there. With a transactional producer, the OffsetTail(1) will
    // not point to an END message but a control message. With aborted
    // transactions, there might even be a lot of garbage at the end of the
    // topic or in between.

    let mut tps = TopicPartitionList::new();
    tps.add_partition(consistency_topic, partition);
    tps.set_partition_offset(consistency_topic, partition, Offset::Beginning)?;

    consumer.assign(&tps).with_context(|| {
        format!(
            "Error seeking in consistency topic {}:{}",
            consistency_topic, partition
        )
    })?;

    let (lo, hi) = consumer
        .fetch_watermarks(consistency_topic, 0, timeout)
        .map_err(|e| {
            anyhow!(
                "Failed to fetch metadata while reading from consistency topic: {}",
                e
            )
        })?;

    // Empty topic.  Return early to avoid unnecesasry call to kafka below.
    if hi == 0 {
        return Ok(None);
    }

    let mut latest_ts = None;
    let mut latest_offset = None;
    while let Some((message, offset)) = get_next_message(&mut consumer, timeout)? {
        debug_assert!(offset >= latest_offset.unwrap_or(0));
        latest_offset = Some(offset);

        if let Some(ts) = maybe_decode_consistency_end_record(&message, consistency_topic)? {
            if ts >= latest_ts.unwrap_or(0) {
                latest_ts = Some(ts);
            }
        }
    }

    // Topic not empty but we couldn't read any messages.  We don't expect this to happen but we
    // have no reason to rely on kafka not inserting any internal messages at the beginning.
    if latest_offset.is_none() {
        tracing::debug!(
            "unable to read any messages from non-empty topic {}:{}, lo/hi: {}/{}",
            consistency_topic,
            partition,
            lo,
            hi
        );
    }
    Ok(latest_ts)
}

// There may be arbitrary messages in this topic that we cannot decode.  We only
// return an error when we know we've found an END message but cannot decode it.
fn maybe_decode_consistency_end_record(
    bytes: &[u8],
    consistency_topic: &str,
) -> Result<Option<Timestamp>, anyhow::Error> {
    // The first 5 bytes are reserved for the schema id/schema registry information
    let mut bytes = &bytes[5..];
    let record = mz_avro::from_avro_datum(get_debezium_transaction_schema(), &mut bytes)
        .context("Failed to decode consistency topic message")?;

    if let Value::Record(ref r) = record {
        let m: HashMap<String, Value> = r.clone().into_iter().collect();
        let status = m.get("status");
        let id = m.get("id");
        match (status, id) {
            (Some(Value::String(status)), Some(Value::String(id))) if status == "END" => {
                if let Ok(ts) = id.parse::<u64>() {
                    Ok(Some(ts))
                } else {
                    bail!(
                        "Malformed consistency record, failed to parse timestamp {} in topic {}",
                        id,
                        consistency_topic
                    );
                }
            }
            _ => Ok(None),
        }
    } else {
        Ok(None)
    }
}

async fn register_kafka_topic(
    client: &AdminClient<MzClientContext>,
    topic: &str,
    mut partition_count: i32,
    mut replication_factor: i32,
    succeed_if_exists: bool,
    retention: KafkaSinkConnectorRetention,
) -> Result<(), CoordError> {
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

    let mut kafka_topic = NewTopic::new(
        &topic,
        partition_count,
        TopicReplication::Fixed(replication_factor),
    );

    let retention_ms_str = retention.retention_ms.map(|s| s.to_string());
    let retention_bytes_str = retention.retention_bytes.map(|s| s.to_string());
    if let Some(ref retention_ms) = retention_ms_str {
        kafka_topic = kafka_topic.set("retention.ms", retention_ms);
    }
    if let Some(ref retention_bytes) = retention_bytes_str {
        kafka_topic = kafka_topic.set("retention.bytes", retention_bytes);
    }

    if succeed_if_exists {
        kafka_util::admin::ensure_topic(
            client,
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            &kafka_topic,
        )
        .await
    } else {
        kafka_util::admin::create_new_topic(
            client,
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            &kafka_topic,
        )
        .await
    }
    .with_context(|| format!("Error creating topic {} for sink", topic))?;

    Ok(())
}

/// Publish value and optional key schemas for a given topic.
///
/// TODO(benesch): do we need to delete the Kafka topic if publishing the
// schema fails?
async fn publish_kafka_schemas(
    ccsr: &ccsr::Client,
    topic: &str,
    key_schema: Option<&str>,
    key_schema_type: Option<ccsr::SchemaType>,
    value_schema: &str,
    value_schema_type: ccsr::SchemaType,
) -> Result<(Option<i32>, i32), CoordError> {
    let value_schema_id = ccsr
        .publish_schema(
            &format!("{}-value", topic),
            value_schema,
            value_schema_type,
            &[],
        )
        .await
        .context("unable to publish value schema to registry in kafka sink")?;

    let key_schema_id = if let Some(key_schema) = key_schema {
        let key_schema_type = key_schema_type.ok_or_else(|| {
            CoordError::Unstructured(anyhow!("expected schema type for key schema"))
        })?;
        Some(
            ccsr.publish_schema(&format!("{}-key", topic), key_schema, key_schema_type, &[])
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
    let maybe_append_nonce = {
        let reuse_topic = builder.reuse_topic;
        let topic_suffix_nonce = builder.topic_suffix_nonce;
        move |topic: &str| {
            if reuse_topic {
                topic.to_string()
            } else {
                format!("{}-{}-{}", topic, id, topic_suffix_nonce)
            }
        }
    };
    let topic = maybe_append_nonce(&builder.topic_prefix);

    // Create Kafka topic
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &builder.broker_addrs.to_string());
    for (k, v) in builder.config_options.iter() {
        // Explicitly reject the statistics interval option here because its not
        // properly supported for this client.
        // Explicitly reject isolation.level as it's a consumer-specific
        // parameter and will generate a benign WARN for admin clients
        if k != "statistics.interval.ms" && k != "isolation.level" {
            config.set(k, v);
        }
    }

    let client: AdminClient<_> = config
        .create_with_context(MzClientContext)
        .context("creating admin client failed")?;

    register_kafka_topic(
        &client,
        &topic,
        builder.partition_count,
        builder.replication_factor,
        builder.reuse_topic,
        builder.retention,
    )
    .await
    .context("error registering kafka topic for sink")?;
    let published_schema_info = match builder.format {
        dataflow_types::KafkaSinkFormat::Avro {
            key_schema,
            value_schema,
            ccsr_config,
            ..
        } => {
            let ccsr = ccsr_config.build()?;
            let (key_schema_id, value_schema_id) = publish_kafka_schemas(
                &ccsr,
                &topic,
                key_schema.as_deref(),
                Some(ccsr::SchemaType::Avro),
                &value_schema,
                ccsr::SchemaType::Avro,
            )
            .await
            .context("error publishing kafka schemas for sink")?;
            Some(PublishedSchemaInfo {
                key_schema_id,
                value_schema_id,
            })
        }
        dataflow_types::KafkaSinkFormat::Json => None,
    };

    let consistency = match builder.consistency_format {
        Some(dataflow_types::KafkaSinkFormat::Avro {
            value_schema,
            ccsr_config,
            ..
        }) => {
            let consistency_topic = maybe_append_nonce(
                builder
                    .consistency_topic_prefix
                    .as_ref()
                    .expect("known to exist"),
            );
            // create consistency topic/schema and retrieve schema id
            register_kafka_topic(
                &client,
                &consistency_topic,
                1,
                builder.replication_factor,
                builder.reuse_topic,
                KafkaSinkConnectorRetention::default(),
            )
            .await
            .context("error registering kafka consistency topic for sink")?;

            let ccsr = ccsr_config.build()?;
            let (_, consistency_schema_id) = publish_kafka_schemas(
                &ccsr,
                &consistency_topic,
                None,
                None,
                &value_schema,
                ccsr::SchemaType::Avro,
            )
            .await
            .context("error publishing kafka consistency schemas for sink")?;

            // get latest committed timestamp from consistency topic
            let gate_ts = if builder.reuse_topic {
                get_latest_ts(&consistency_topic, config.clone(), Duration::from_secs(10))
                    .context("error restarting from existing kafka consistency topic for sink")?
            } else {
                None
            };

            Some(KafkaSinkConsistencyConnector {
                topic: consistency_topic,
                schema_id: consistency_schema_id,
                gate_ts,
            })
        }
        Some(other) => unreachable!("non-Avro consistency format for Kafka sink {:#?}", &other),
        _ => None,
    };

    Ok(SinkConnector::Kafka(KafkaSinkConnector {
        topic,
        topic_prefix: builder.topic_prefix,
        addrs: builder.broker_addrs,
        relation_key_indices: builder.relation_key_indices,
        key_desc_and_indices: builder.key_desc_and_indices,
        value_desc: builder.value_desc,
        published_schema_info,
        consistency,
        exactly_once: builder.reuse_topic,
        transitive_source_dependencies: builder.transitive_source_dependencies,
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
