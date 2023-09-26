// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use maplit::btreemap;
use mz_kafka_util::client::{
    BrokerRewritingClientContext, MzClientContext, DEFAULT_FETCH_METADATA_TIMEOUT,
};
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkAvroFormatState, KafkaSinkConnection,
    KafkaSinkConnectionRetention, KafkaSinkFormat,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Formatter for Kafka group.id setting
pub struct SinkGroupId;

impl SinkGroupId {
    pub fn new(sink_id: GlobalId) -> String {
        format!("materialize-bootstrap-sink-{sink_id}")
    }
}

/// Formatter for the progress topic's key's
pub struct ProgressKey;

impl ProgressKey {
    pub fn new(sink_id: GlobalId) -> String {
        format!("mz-sink-{sink_id}")
    }
}

struct TopicConfigs {
    partition_count: i32,
    replication_factor: i32,
}

async fn discover_topic_configs<C: ClientContext>(
    client: &AdminClient<C>,
    topic: &str,
) -> Result<TopicConfigs, anyhow::Error> {
    let mut partition_count = -1;
    let mut replication_factor = -1;

    let metadata = client
        .inner()
        .fetch_metadata(None, DEFAULT_FETCH_METADATA_TIMEOUT)
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
        bail!("read empty custer configuration; do we have DescribeConfigs permissions?")
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

async fn ensure_kafka_topic<C>(
    client: &AdminClient<C>,
    topic: &str,
    mut partition_count: i32,
    mut replication_factor: i32,
    retention: KafkaSinkConnectionRetention,
) -> Result<(), anyhow::Error>
where
    C: ClientContext,
{
    // if either partition count or replication factor should be defaulted to the broker's config
    // (signaled by a value of -1), explicitly poll the broker to discover the defaults.
    // Newer versions of Kafka can instead send create topic requests with -1 and have this happen
    // behind the scenes, but this is unsupported and will result in errors on pre-2.4 Kafka.
    if partition_count == -1 || replication_factor == -1 {
        match discover_topic_configs(client, topic).await {
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

    let retention_ms_str = retention.duration.map(|d| d.to_string());
    let retention_bytes_str = retention.bytes.map(|s| s.to_string());
    if let Some(ref retention_ms) = retention_ms_str {
        kafka_topic = kafka_topic.set("retention.ms", retention_ms);
    }
    if let Some(ref retention_bytes) = retention_bytes_str {
        kafka_topic = kafka_topic.set("retention.bytes", retention_bytes);
    }

    mz_kafka_util::admin::ensure_topic(
        client,
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        &kafka_topic,
    )
    .await
    .with_context(|| format!("Error creating topic {} for sink", topic))?;

    Ok(())
}

/// Publish value and optional key schemas for a given topic.
///
/// TODO(benesch): do we need to delete the Kafka topic if publishing the
/// schema fails?
async fn publish_kafka_schemas(
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

/// Ensures that the Kafka sink's topic and consistency collateral exist.
///
/// # Errors
/// - If the [`KafkaSinkConnection`]'s consistency collateral exists and
///   contains data for this sink, but the sink's data topic does not exist.
pub async fn build_kafka(
    sink_id: mz_repr::GlobalId,
    connection: &mut KafkaSinkConnection,
    connection_cx: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let client: AdminClient<_> = connection
        .connection
        .create_with_context(connection_cx, MzClientContext::default(), &BTreeMap::new())
        .await
        .context("creating admin client failed")?;

    // Check for existence of progress topic; if it exists and contains data for
    // this sink, we expect the data topic to exist, as well. Note that we don't
    // expect the converse to be true because we don't want to prevent users
    // from creating topics before setting up their sinks.
    let meta = client
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))?;

    // Check if the broker's metadata already contains the progress topic.
    let progress_topic = match &connection.consistency_config {
        KafkaConsistencyConfig::Progress { topic } => {
            meta.topics().iter().find(|t| t.name() == topic)
        }
    };

    // If the consistency topic exists, check to see if it contains this sink's
    // data.
    if let Some(progress_topic) = progress_topic {
        let progress_client: BaseConsumer<_> = connection
            .connection
            .create_with_context(
                connection_cx,
                MzClientContext::default(),
                &btreemap! {
                    "group.id" => SinkGroupId::new(sink_id),
                    "isolation.level" => "read_committed".into(),
                    "enable.auto.commit" => "false".into(),
                    "auto.offset.reset" => "earliest".into(),
                    "enable.partition.eof" => "true".into(),
                },
            )
            .await?;

        tracing::info!("progress_topic {}", progress_topic.name());

        let latest_ts = determine_latest_progress_record(
            format!("build_kafka_{}", sink_id),
            progress_topic.name().to_string(),
            ProgressKey::new(sink_id),
            Arc::new(progress_client),
        )
        .await?;

        tracing::info!(
            "connection.topic {}, progress_topic {}, latest_ts {:?}, all topics {:?}",
            connection.topic,
            progress_topic.name(),
            latest_ts,
            meta.topics()
                .iter()
                .map(|t| t.name().to_string())
                .collect::<Vec<_>>()
        );

        // If we have progress data, we should have the topic listed in the
        // broker's metadata. If we don't, error.
        if latest_ts.is_some() && !meta.topics().iter().any(|t| t.name() == connection.topic) {
            bail!("sink progress data exists, but sink data topic is missing");
        }
    }

    // Create Kafka topic.
    ensure_kafka_topic(
        &client,
        &connection.topic,
        connection.partition_count,
        connection.replication_factor,
        connection.retention,
    )
    .await
    .context("error registering kafka topic for sink")?;

    match &connection.format {
        KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::UnpublishedMaybe {
            key_schema,
            value_schema,
            csr_connection,
        }) => {
            let ccsr = csr_connection.connect(connection_cx).await?;
            let (key_schema_id, value_schema_id) = publish_kafka_schemas(
                &ccsr,
                &connection.topic,
                key_schema.as_deref(),
                Some(mz_ccsr::SchemaType::Avro),
                value_schema,
                mz_ccsr::SchemaType::Avro,
            )
            .await
            .context("error publishing kafka schemas for sink")?;

            connection.format = KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::Published {
                key_schema_id,
                value_schema_id,
            })
        }
        KafkaSinkFormat::Avro(_) | KafkaSinkFormat::Json => {}
    }

    match &connection.consistency_config {
        KafkaConsistencyConfig::Progress { topic } => {
            ensure_kafka_topic(
                &client,
                topic,
                1,
                connection.replication_factor,
                KafkaSinkConnectionRetention::default(),
            )
            .await
            .context("error registering kafka consistency topic for sink")?;
        }
    };

    Ok(())
}

#[derive(Serialize, Deserialize)]
/// This struct is emitted as part of a transactional produce, and captures the information we
/// need to resume the Kafka sink at the correct place in the sunk collection. (Currently, all
/// we need is the timestamp... this is a record to make it easier to add more metadata in the
/// future if needed.) It's encoded as JSON to make it easier to introspect while debugging, and
/// because we expect it to remain small.
///
/// Unlike the old consistency topic, this is not intended to be a user-facing feature; it's there
/// purely so the sink can maintain its transactional guarantees. Any future user-facing consistency
/// information should be added elsewhere instead of overloading this record.
pub struct ProgressRecord {
    pub timestamp: Timestamp,
}

/// Determines the latest progress record from the specified topic for the given
/// key (e.g. akin to a sink's GlobalId).
pub async fn determine_latest_progress_record(
    name: String,
    progress_topic: String,
    progress_key: String,
    progress_client: Arc<BaseConsumer<BrokerRewritingClientContext<MzClientContext>>>,
) -> Result<Option<Timestamp>, anyhow::Error> {
    // Polls a message from a Kafka Source.  Blocking so should always be called on background
    // thread.
    fn get_next_message<C>(
        consumer: &BaseConsumer<C>,
        timeout: Duration,
    ) -> Result<Option<(Vec<u8>, Vec<u8>, i64)>, anyhow::Error>
    where
        C: ConsumerContext,
    {
        if let Some(result) = consumer.poll(timeout) {
            match result {
                Ok(message) => match message.payload() {
                    Some(p) => Ok(Some((
                        message.key().unwrap_or(&[]).to_vec(),
                        p.to_vec(),
                        message.offset(),
                    ))),
                    None => bail!("unexpected null payload"),
                },
                Err(KafkaError::PartitionEOF(_)) => Ok(None),
                Err(err) => bail!("Failed to process message {}", err),
            }
        } else {
            Ok(None)
        }
    }

    // Retrieves the latest committed timestamp from the progress topic.  Blocking so should
    // always be called on background thread
    fn get_latest_ts<C>(
        progress_topic: &str,
        progress_key: &str,
        progress_client: &BaseConsumer<C>,
        timeout: Duration,
    ) -> Result<Option<Timestamp>, anyhow::Error>
    where
        C: ConsumerContext,
    {
        // ensure the progress topic has exactly one partition
        let partitions = mz_kafka_util::client::get_partitions(
            progress_client.client(),
            progress_topic,
            timeout,
        )
        .with_context(|| {
            format!(
                "Unable to fetch metadata about progress topic {}",
                progress_topic
            )
        })?;

        if partitions.len() != 1 {
            bail!(
                    "Progress topic {} should contain a single partition, but instead contains {} partitions",
                    progress_topic, partitions.len(),
                );
        }

        let partition = partitions.into_element();

        // We scan from the beginning and see if we can find a progress record. We have
        // to do it like this because Kafka Control Batches mess with offsets. We
        // therefore cannot simply take the last offset from the back and expect a
        // progress message there. With a transactional producer, the OffsetTail(1) will
        // not point to an progress message but a control message. With aborted
        // transactions, there might even be a lot of garbage at the end of the
        // topic or in between.

        let mut tps = TopicPartitionList::new();
        tps.add_partition(progress_topic, partition);
        tps.set_partition_offset(progress_topic, partition, Offset::Beginning)?;

        progress_client.assign(&tps).with_context(|| {
            format!(
                "Error seeking in progress topic {}:{}",
                progress_topic, partition
            )
        })?;

        let (lo, hi) = progress_client
            .fetch_watermarks(progress_topic, 0, timeout)
            .map_err(|e| {
                anyhow!(
                    "Failed to fetch metadata while reading from progress topic: {}",
                    e
                )
            })?;

        // Empty topic.  Return early to avoid unnecessary call to kafka below.
        if hi == 0 {
            return Ok(None);
        }

        let mut latest_ts = None;
        let mut latest_offset = None;

        let progress_key_bytes = progress_key.as_bytes();
        while let Some((key, message, offset)) = get_next_message(progress_client, timeout)? {
            debug_assert!(offset >= latest_offset.unwrap_or(0));
            latest_offset = Some(offset);

            let timestamp_opt = if &key == progress_key_bytes {
                let progress: ProgressRecord = serde_json::from_slice(&message)?;
                Some(progress.timestamp)
            } else {
                None
            };

            if let Some(ts) = timestamp_opt {
                if ts >= latest_ts.unwrap_or_else(timely::progress::Timestamp::minimum) {
                    latest_ts = Some(ts);
                }
            }

            // If the next possible offset for the client is past the high watermark, we've seen
            // everything we expect to see.
            let position = progress_client
                .position()?
                .find_partition(progress_topic, partition)
                .ok_or_else(|| anyhow!("No progress info for known partition"))?
                .offset();
            if let Offset::Offset(i) = position {
                if i >= hi {
                    break;
                }
            }
        }

        // Topic not empty but we couldn't read any messages.  We don't expect this to happen but we
        // have no reason to rely on kafka not inserting any internal messages at the beginning.
        if latest_offset.is_none() {
            debug!(
                "unable to read any messages from non-empty topic {}:{}, lo/hi: {}/{}",
                progress_topic, partition, lo, hi
            );
        }
        Ok(latest_ts)
    }

    // Only actually used for retriable errors.
    Retry::default()
        .max_tries(3)
        .clamp_backoff(Duration::from_secs(60 * 10))
        .retry_async(|_| async {
            let progress_topic = progress_topic.clone();
            let progress_key = progress_key.clone();
            let progress_client = Arc::clone(&progress_client);
            task::spawn_blocking(
                || format!("get_latest_ts:{name}"),
                move || {
                    get_latest_ts(
                        &progress_topic,
                        &progress_key,
                        &progress_client,
                        DEFAULT_FETCH_METADATA_TIMEOUT,
                    )
                },
            )
            .await
            .unwrap_or_else(|e| bail!(e))
        })
        .await
}
