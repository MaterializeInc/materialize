// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::OpenOptions;
use std::time::Duration;

use anyhow::{anyhow, Context};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::config::ClientConfig;

use mz_dataflow_types::sinks::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, KafkaSinkConnectorRetention, KafkaSinkConsistencyConnector,
    PublishedSchemaInfo, SinkConnector, SinkConnectorBuilder,
};
use mz_expr::GlobalId;
use mz_kafka_util::client::MzClientContext;
use mz_ore::collections::CollectionExt;

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

    let retention_ms_str = match retention.duration {
        Some(Some(d)) => Some(d.as_millis().to_string()),
        Some(None) => Some("-1".to_string()),
        None => None,
    };
    let retention_bytes_str = retention.bytes.map(|s| s.to_string());
    if let Some(ref retention_ms) = retention_ms_str {
        kafka_topic = kafka_topic.set("retention.ms", retention_ms);
    }
    if let Some(ref retention_bytes) = retention_bytes_str {
        kafka_topic = kafka_topic.set("retention.bytes", retention_bytes);
    }

    if succeed_if_exists {
        mz_kafka_util::admin::ensure_topic(
            client,
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            &kafka_topic,
        )
        .await
    } else {
        mz_kafka_util::admin::create_new_topic(
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
    ccsr: &mz_ccsr::Client,
    topic: &str,
    key_schema: Option<&str>,
    key_schema_type: Option<mz_ccsr::SchemaType>,
    value_schema: &str,
    value_schema_type: mz_ccsr::SchemaType,
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
        mz_dataflow_types::sinks::KafkaSinkFormat::Avro {
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
                Some(mz_ccsr::SchemaType::Avro),
                &value_schema,
                mz_ccsr::SchemaType::Avro,
            )
            .await
            .context("error publishing kafka schemas for sink")?;
            Some(PublishedSchemaInfo {
                key_schema_id,
                value_schema_id,
            })
        }
        mz_dataflow_types::sinks::KafkaSinkFormat::Json => None,
    };

    let consistency = match builder.consistency_format {
        Some(mz_dataflow_types::sinks::KafkaSinkFormat::Avro {
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
                mz_ccsr::SchemaType::Avro,
            )
            .await
            .context("error publishing kafka consistency schemas for sink")?;

            Some(KafkaSinkConsistencyConnector {
                topic: consistency_topic,
                schema_id: consistency_schema_id,
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
