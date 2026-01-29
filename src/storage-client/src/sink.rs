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

use anyhow::{Context, anyhow, bail};
use mz_ccsr::GetSubjectConfigError;
use mz_kafka_util::admin::EnsureTopicConfig;
use mz_kafka_util::client::MzClientContext;
use mz_ore::collections::CollectionExt;
use mz_ore::future::{InTask, OreFutureExt};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::KafkaTopicOptions;
use mz_storage_types::errors::ContextCreationErrorExt;
use mz_storage_types::sinks::KafkaSinkConnection;
use rdkafka::ClientContext;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
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
    KafkaTopicOptions {
        partition_count,
        replication_factor,
        topic_config,
    }: &KafkaTopicOptions,
    ensure_topic_config: EnsureTopicConfig,
) -> Result<bool, anyhow::Error> {
    let mut storage_configuration = storage_configuration.clone();
    // With recent librdkafka the topic metadata propagation for a sink error takes longer
    storage_configuration
        .parameters
        .kafka_timeout_config
        .topic_metadata_propagation_max = Duration::from_secs(10);
    let client: AdminClient<_> = connection
        .connection
        .create_with_context(
            &storage_configuration,
            MzClientContext::default(),
            &BTreeMap::new(),
            // Only called from `mz_storage`.
            InTask::Yes,
        )
        .await
        .add_context("creating admin client failed")?;
    let mut partition_count = partition_count.map(|f| *f);
    let mut replication_factor = replication_factor.map(|f| *f);
    // If either partition count or replication factor should be defaulted to the broker's config
    // (signaled by a value of None), explicitly poll the broker to discover the defaults.
    // Newer versions of Kafka can instead send create topic requests with -1 and have this happen
    // behind the scenes, but this is unsupported and will result in errors on pre-2.4 Kafka.
    if partition_count.is_none() || replication_factor.is_none() {
        let fetch_timeout = storage_configuration
            .parameters
            .kafka_timeout_config
            .fetch_metadata_timeout;
        match discover_topic_configs(&client, topic, fetch_timeout).await {
            Ok(configs) => {
                if partition_count.is_none() {
                    partition_count = Some(configs.partition_count);
                }
                if replication_factor.is_none() {
                    replication_factor = Some(configs.replication_factor);
                }
            }
            Err(e) => {
                // Recent versions of Kafka can handle an explicit -1 config, so use this instead
                // and the request will probably still succeed. Logging anyways for visibility.
                warn!("Failed to discover default values for topic configs: {e}");
                if partition_count.is_none() {
                    partition_count = Some(-1);
                }
                if replication_factor.is_none() {
                    replication_factor = Some(-1);
                }
            }
        };
    }

    let mut kafka_topic = NewTopic::new(
        topic,
        partition_count.expect("always set above"),
        TopicReplication::Fixed(replication_factor.expect("always set above")),
    );

    for (key, value) in topic_config {
        kafka_topic = kafka_topic.set(key, value);
    }

    mz_kafka_util::admin::ensure_topic(
        &client,
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        &kafka_topic,
        ensure_topic_config,
    )
    .await
    .with_context(|| format!("Error creating topic {} for sink", topic))
}

/// Publish a schema for a given subject, and set
/// compatibility levels for the schema if applicable.
///
/// TODO(benesch): do we need to delete the Kafka topic if publishing the
/// schema fails?
pub async fn publish_kafka_schema(
    ccsr: mz_ccsr::Client,
    subject: String,
    schema: String,
    schema_type: mz_ccsr::SchemaType,
    compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
) -> Result<i32, anyhow::Error> {
    if let Some(compatibility_level) = compatibility_level {
        let ccsr = ccsr.clone();
        let subject = subject.clone();
        async move {
            // Only update the compatibility level if it's not already set to something.
            match ccsr.get_subject_config(&subject).await {
                Ok(config) => {
                    if config.compatibility_level != compatibility_level {
                        tracing::debug!(
                            "compatibility level '{}' does not match intended '{}'",
                            config.compatibility_level,
                            compatibility_level
                        );
                    }
                    Ok(())
                }
                Err(GetSubjectConfigError::SubjectCompatibilityLevelNotSet)
                | Err(GetSubjectConfigError::SubjectNotFound) => ccsr
                    .set_subject_compatibility_level(&subject, compatibility_level)
                    .await
                    .map_err(anyhow::Error::from),
                Err(e) => Err(e.into()),
            }
        }
        .run_in_task(|| "set_compatibility_level".to_string())
        .await
        .context("unable to update schema compatibility level in kafka sink")?;
    }

    let schema_id = async move {
        ccsr.publish_schema(&subject, &schema, schema_type, &[])
            .await
    }
    .run_in_task(|| "publish_kafka_schema".to_string())
    .await
    .context("unable to publish schema to registry in kafka sink")?;

    Ok(schema_id)
}
