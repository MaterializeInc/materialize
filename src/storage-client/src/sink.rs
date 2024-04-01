// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use mz_kafka_util::client::MzClientContext;
use mz_ore::collections::CollectionExt;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::ContextCreationErrorExt;
use mz_storage_types::sinks::KafkaSinkConnection;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::ClientContext;
use tracing::warn;

pub mod progress_key {
    use std::fmt;

    use mz_repr::GlobalId;
    use rdkafka::message::ToBytes;

    /// A key identifying a given sink within a progress topic.
    #[derive(Debug, Clone)]
    pub struct ProgressKey(String);

    impl ProgressKey {
        /// Constructs a progress key for the sink with the specified ID.
        pub fn new(sink_id: GlobalId) -> ProgressKey {
            ProgressKey(format!("mz-sink-{sink_id}"))
        }
    }

    impl ToBytes for ProgressKey {
        fn to_bytes(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }

    impl fmt::Display for ProgressKey {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.fmt(f)
        }
    }
}

struct TopicConfigs {
    partition_count: i32,
    replication_factor: i32,
}

async fn discover_topic_configs<C: ClientContext>(
    client: &AdminClient<C>,
    topic: &str,
    fetch_timeout: Duration,
) -> Result<TopicConfigs, anyhow::Error> {
    let mut partition_count = -1;
    let mut replication_factor = -1;

    let metadata = client
        .inner()
        .fetch_metadata(None, fetch_timeout)
        .with_context(|| {
            format!(
                "error fetching metadata when creating new topic {} for sink",
                topic
            )
        })?;

    if metadata.brokers().len() == 0 {
        Err(anyhow!("zero brokers discovered in metadata request"))?;
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
                broker, topic
            )
        })?;

    if configs.len() != 1 {
        Err(anyhow!(
                "error creating topic {} for sink: broker {} returned {} config results, but one was expected",
                topic,
                broker,
                configs.len()
            ))?;
    }

    let config = configs.into_element().map_err(|e| {
        anyhow!(
            "error reading broker configuration when creating topic {} for sink: {}",
            topic,
            e
        )
    })?;

    if config.entries.is_empty() {
        bail!("read empty cluster configuration; do we have DescribeConfigs permissions?")
    }

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

    Ok(TopicConfigs {
        partition_count,
        replication_factor,
    })
}

/// Configuration of a topic created by `ensure_kafka_topic`.
// TODO(benesch): some fields here use `-1` to indicate broker default, while
// others use `None` to indicate broker default and `-1` has a different special
// meaning. This is not very Rusty and very easy to get wrong. We should adjust
// the API for this method to use types that are safer and have more obvious
// meaning. For example, `partition_count` could have type
// `Option<NonNeg<i32>>`, where `-1` is prohibited as a value and the Rustier
// `None` value represents the broker default (n.b.: `u32` is not a good choice
// because the maximum number of Kafka partitions is `i32::MAX`, not
// `u32::MAX`).
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// The number of partitions to create.
    ///
    /// Use `-1` to indicate broker default.
    pub partition_count: i32,
    /// The replication factor.
    ///
    /// Use `-1` to indicate broker default.
    pub replication_factor: i32,
    /// Describes how to clean up old data in the topic.
    pub cleanup_policy: TopicCleanupPolicy,
}

/// Describes how to clean up old data in the topic.
#[derive(Debug, Clone)]
pub enum TopicCleanupPolicy {
    /// Clean up the topic using a time and/or size based retention policies.
    Retention {
        /// A time-based retention policy.
        ///
        /// `None` indicates broker default. `Some(-1)` indicates infinite
        /// retention.
        ms: Option<i64>,
        /// A size based retention policy.
        ///
        /// `None` indicates broker default. `Some(-1)` indicates infinite
        /// retention.
        bytes: Option<i64>,
    },
    /// Clean up the topic using key-based compaction.
    Compaction,
}

/// Ensures that the named Kafka topic exists.
///
/// If the topic does not exist, the function creates the topic with the
/// provided `config`. Note that if the topic already exists, the function does
/// *not* verify that the topic's configuration matches `config`.
///
/// Returns a boolean indicating whether the topic already existed.
pub async fn ensure_kafka_topic(
    connection: &KafkaSinkConnection,
    storage_configuration: &StorageConfiguration,
    topic: &str,
    TopicConfig {
        mut partition_count,
        mut replication_factor,
        cleanup_policy,
    }: TopicConfig,
) -> Result<bool, anyhow::Error> {
    let client: AdminClient<_> = connection
        .connection
        .create_with_context(
            storage_configuration,
            MzClientContext::default(),
            &BTreeMap::new(),
        )
        .await
        .add_context("creating admin client failed")?;
    // if either partition count or replication factor should be defaulted to the broker's config
    // (signaled by a value of -1), explicitly poll the broker to discover the defaults.
    // Newer versions of Kafka can instead send create topic requests with -1 and have this happen
    // behind the scenes, but this is unsupported and will result in errors on pre-2.4 Kafka.
    if partition_count == -1 || replication_factor == -1 {
        let fetch_timeout = storage_configuration
            .parameters
            .kafka_timeout_config
            .fetch_metadata_timeout;
        match discover_topic_configs(&client, topic, fetch_timeout).await {
            Ok(configs) => {
                if partition_count == -1 {
                    partition_count = configs.partition_count;
                }
                if replication_factor == -1 {
                    replication_factor = configs.replication_factor;
                }
            }
            Err(e) => {
                // Since recent versions of Kafka can handle an explicit -1 config, this
                // request will probably still succeed. Logging anyways for visibility.
                warn!("Failed to discover default values for topic configs: {e}");
            }
        };
    }

    let mut kafka_topic = NewTopic::new(
        topic,
        partition_count,
        TopicReplication::Fixed(replication_factor),
    );

    let retention_ms;
    let retention_bytes;
    match cleanup_policy {
        TopicCleanupPolicy::Retention { ms, bytes } => {
            kafka_topic = kafka_topic.set("cleanup.policy", "delete");
            if let Some(ms) = ms {
                retention_ms = ms.to_string();
                kafka_topic = kafka_topic.set("retention.ms", &retention_ms);
            }
            if let Some(bytes) = &bytes {
                retention_bytes = bytes.to_string();
                kafka_topic = kafka_topic.set("retention.bytes", &retention_bytes);
            }
        }
        TopicCleanupPolicy::Compaction => {
            kafka_topic = kafka_topic.set("cleanup.policy", "compact");
        }
    }

    mz_kafka_util::admin::ensure_topic(
        &client,
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        &kafka_topic,
    )
    .await
    .with_context(|| format!("Error creating topic {} for sink", topic))
}

/// Publish value and optional key schemas for a given topic.
///
/// TODO(benesch): do we need to delete the Kafka topic if publishing the
/// schema fails?
pub async fn publish_kafka_schemas(
    ccsr: &mz_ccsr::Client,
    topic: &str,
    key_schema: Option<&str>,
    key_schema_type: Option<mz_ccsr::SchemaType>,
    value_schema: &str,
    value_schema_type: mz_ccsr::SchemaType,
) -> Result<(Option<i32>, i32), anyhow::Error> {
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
        let key_schema_type =
            key_schema_type.ok_or_else(|| anyhow!("expected schema type for key schema"))?;
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
