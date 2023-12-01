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
    GetPartitionsError, MzClientContext, TunnelingClientContext, DEFAULT_FETCH_METADATA_TIMEOUT,
};
use mz_ore::collections::CollectionExt;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::{ContextCreationError, ContextCreationErrorExt};
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkConnection, KafkaSinkConnectionRetention,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::sink::progress_key::ProgressKey;

/// Formatter for Kafka group.id setting
pub struct SinkGroupId;

impl SinkGroupId {
    pub fn new(sink_id: GlobalId) -> String {
        format!("materialize-bootstrap-sink-{sink_id}")
    }
}

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

/// Ensures that the Kafka sink's data and consistency collateral exist, as well
/// as returning the last complete timestamp that the last incarnation of the
/// sink committed to Kafka.
///
/// IMPORTANT: to achieve exactly once guarantees, the producer that will resume
/// production at the returned timestamp *must* have called `init_transactions`
/// prior to calling this method.
///
/// # Errors
/// - If the [`KafkaSinkConnection`]'s consistency collateral exists and
///   contains data for this sink, but the sink's data topic does not exist.
pub async fn build_kafka(
    sink_id: mz_repr::GlobalId,
    connection: &KafkaSinkConnection,
    connection_cx: &ConnectionContext,
) -> Result<Option<Timestamp>, ContextCreationError> {
    // Fetch the progress of the last incarnation of the sink, if any.
    let progress_topic = match &connection.consistency_config {
        KafkaConsistencyConfig::Progress { topic } => topic,
    };
    // For details about the two clients constructed here, see
    // `determine_latest_progress_record`.
    let make_progress_client = |isolation_level: &'static str| async {
        connection
            .connection
            .create_with_context(
                connection_cx,
                MzClientContext::default(),
                &btreemap! {
                    "group.id" => SinkGroupId::new(sink_id),
                    "isolation.level" => isolation_level.into(),
                    "enable.auto.commit" => "false".into(),
                    "auto.offset.reset" => "earliest".into(),
                    "enable.partition.eof" => "true".into(),
                },
            )
            .await
    };
    let progress_client_read_committed = Arc::new(make_progress_client("read_committed").await?);
    let progress_client_read_uncommitted =
        Arc::new(make_progress_client("read_uncommitted").await?);
    let latest_ts = determine_latest_progress_record(
        format!("build_kafka_{}", sink_id),
        Arc::clone(&progress_client_read_committed),
        progress_client_read_uncommitted,
        progress_topic.to_string(),
        ProgressKey::new(sink_id),
    )
    .await
    .check_ssh_status(progress_client_read_committed.client().context())?;

    let admin_client: AdminClient<_> = connection
        .connection
        .create_with_context(connection_cx, MzClientContext::default(), &BTreeMap::new())
        .await
        .add_context("creating admin client failed")?;

    // If the progress topic existed and contained data for this sink, we expect
    // the data topic to exist, as well. Note that we don't expect the converse
    // to be true because we don't want to prevent users from creating topics
    // before setting up their sinks.
    let meta = admin_client
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))
        .check_ssh_status(admin_client.inner().context())
        .add_context("fetching metadata")?;
    if latest_ts.is_some() && !meta.topics().iter().any(|t| t.name() == connection.topic) {
        Err(anyhow::anyhow!(
            "sink progress data exists, but sink data topic is missing"
        ))?;
    }

    // Create Kafka topics.
    ensure_kafka_topic(
        &admin_client,
        progress_topic,
        1,
        connection.replication_factor,
        KafkaSinkConnectionRetention::default(),
    )
    .await
    .check_ssh_status(admin_client.inner().context())
    .add_context("error registering kafka progress topic for sink")?;
    ensure_kafka_topic(
        &admin_client,
        &connection.topic,
        connection.partition_count,
        connection.replication_factor,
        connection.retention,
    )
    .await
    .check_ssh_status(admin_client.inner().context())
    .add_context("error registering kafka topic for sink")?;

    Ok(latest_ts)
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
/// progress key.
///
/// IMPORTANT: to achieve exactly once guarantees, the producer that will resume
/// production at the returned timestamp *must* have called `init_transactions`
/// prior to calling this method.
///
/// IMPORTANT: the `progress_client` must have `enable.partition.eof` set to
/// `true`.
async fn determine_latest_progress_record(
    name: String,
    progress_client_read_committed: Arc<BaseConsumer<TunnelingClientContext<MzClientContext>>>,
    progress_client_read_uncommitted: Arc<BaseConsumer<TunnelingClientContext<MzClientContext>>>,
    progress_topic: String,
    progress_key: ProgressKey,
) -> Result<Option<Timestamp>, anyhow::Error> {
    // ****************************** WARNING ******************************
    // Be VERY careful when editing the code in this function. It is very easy
    // to accidentally introduce a correctness or liveness bug when refactoring
    // this code.
    // ****************************** WARNING ******************************

    /// The timeout for reading records from the progress topic. Set to
    /// something slightly longer than the idle transaction timeout (60s) to
    /// wait out any stuck producers.
    const PROGRESS_RECORD_FETCH_TIMEOUT: Duration = Duration::from_secs(90);

    /// Retrieves the latest committed timestamp from the progress topic.
    ///
    /// Blocking so should always be called on background thread.
    fn get_latest_ts<C>(
        progress_client_read_committed: &BaseConsumer<C>,
        progress_client_read_uncommitted: &BaseConsumer<C>,
        progress_topic: &str,
        progress_key: &ProgressKey,
    ) -> Result<Option<Timestamp>, anyhow::Error>
    where
        C: ConsumerContext,
    {
        // Ensure the progress topic has exactly one partition. Kafka only
        // guarantees ordering within a single partition, and we need a strict
        // order on the progress messages we read and write.
        let partitions = match mz_kafka_util::client::get_partitions(
            progress_client_read_committed.client(),
            progress_topic,
            DEFAULT_FETCH_METADATA_TIMEOUT,
        ) {
            Ok(partitions) => partitions,
            Err(GetPartitionsError::TopicDoesNotExist) => {
                // The progress topic doesn't exist, which indicates there is
                // no committed timestamp.
                return Ok(None);
            }
            e => e.with_context(|| {
                format!(
                    "Unable to fetch metadata about progress topic {}",
                    progress_topic
                )
            })?,
        };
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

        // First, determine the current high water mark for the progress topic.
        // This is the position our `progress_client` consumer *must* reach
        // before we can conclude that we've seen the latest progress record for
        // the specified `progress_key`. A safety argument:
        //
        //   * Our caller has initialized transactions before calling this
        //     method, which prevents the prior incarnation of this sink from
        //     committing any further progress records.
        //
        //   * We use `read_uncommitted` isolation to ensure that we fetch the
        //     true high water mark for the topic, even if there are pending
        //     transactions in the topic. If we used the `read_committed`
        //     isolation level, we'd instead get the "last stable offset" (LSO),
        //     which is the offset of the first message in an open transaction,
        //     which might not include the last progress message committed for
        //     this sink! (While the caller of this function has fenced out
        //     older producers for this sink, *other* sinks writing using the
        //     same progress topic might have long-running transactions that
        //     hold back the LSO.)
        //
        //   * If another sink spins up and fences out the producer for this
        //     incarnation of the sink, we may not see the latest progress
        //     record... but since the producer has been fenced out, it will be
        //     unable to act on our stale information.
        //
        let (lo, hi) = progress_client_read_uncommitted
            .fetch_watermarks(progress_topic, partition, DEFAULT_FETCH_METADATA_TIMEOUT)
            .map_err(|e| {
                anyhow!(
                    "Failed to fetch metadata while reading from progress topic: {}",
                    e
                )
            })?;

        // Seek to the beginning of the progress topic.
        let mut tps = TopicPartitionList::new();
        tps.add_partition(progress_topic, partition);
        tps.set_partition_offset(progress_topic, partition, Offset::Beginning)?;
        progress_client_read_committed
            .assign(&tps)
            .with_context(|| {
                format!(
                    "Error seeking in progress topic {}:{}",
                    progress_topic, partition
                )
            })?;

        // Helper to get the progress consumer's current position.
        let get_position = || {
            let position = progress_client_read_committed
                .position()?
                .find_partition(progress_topic, partition)
                .ok_or_else(|| {
                    anyhow!(
                        "No position info found for progress topic {}",
                        progress_topic
                    )
                })?
                .offset();
            match position {
                Offset::Offset(position) => Ok(position),
                // An invalid offset indicates the consumer has not yet read a
                // message. Since we assigned the consumer to the beginning of
                // the topic, it's safe to return 0 here, which indicates the
                // position before the first possible message.
                Offset::Invalid => Ok(0),
                _ => bail!(
                    "Consumer::position returned offset of wrong type: {:?}",
                    position
                ),
            }
        };

        info!("fetching latest progress record for {progress_key}, lo/hi: {lo}/{hi}");

        // Read messages until the consumer is positioned at or beyond the high
        // water mark.
        //
        // We use `read_committed` isolation to ensure we don't see progress
        // records for transactions that did not commit. This means we have to
        // wait for the LSO to progress to the high water mark `hi`, which means
        // waiting for any open transactions for other sinks using the same
        // progress topic to complete. We set a short transaction timeout (10s)
        // to ensure we never need to wait more than 10s.
        //
        // Note that the stall time on the progress topic is not a function of
        // transaction size. We've designed our transactions so that the
        // progress record is always written last, after all the data has been
        // written, and so the window of time in which the progress topic has an
        // open transaction is quite small. The only vulnerability is if another
        // sink using the same progress topic crashes in that small window
        // between writing the progress record and committing the transaction,
        // in which case we have to wait out the transaction timeout.
        //
        // Important invariant: we only exit this loop successfully (i.e., not
        // returning an error) if we have positive proof of a position at or
        // beyond the high water mark. To make this invariant easy to check, do
        // not use `break` in the body of the loop.
        let mut last_timestamp = None;
        while get_position()? < hi {
            let message = match progress_client_read_committed.poll(PROGRESS_RECORD_FETCH_TIMEOUT) {
                Some(Ok(message)) => message,
                Some(Err(KafkaError::PartitionEOF(_))) => {
                    // No message, but the consumer's position may have advanced
                    // past a transaction control message that positions us at
                    // or beyond the high water mark. Go around the loop again
                    // to check.
                    continue;
                }
                Some(Err(e)) => bail!("failed to fetch progress message {e}"),
                None => {
                    bail!(
                        "timed out while waiting to reach high water mark of non-empty \
                         topic {progress_topic}:{partition}, lo/hi: {lo}/{hi}"
                    );
                }
            };

            if message.key() != Some(progress_key.to_bytes()) {
                // This is a progress message for a different sink.
                continue;
            }

            let ProgressRecord { timestamp } =
                serde_json::from_slice(message.payload().unwrap_or(&[]))?;
            match last_timestamp {
                Some(last_timestamp) if timestamp < last_timestamp => {
                    bail!(
                        "timestamp regressed in topic {progress_topic}:{partition} \
                        from {last_timestamp} to {timestamp}"
                    );
                }
                _ => last_timestamp = Some(timestamp),
            };
        }

        // If we get here, we are assured that we've read all messages up to
        // the high water mark, and therefore `last_timestamp` contains the
        // most recent timestamp for the sink under consideration.
        Ok(last_timestamp)
    }

    task::spawn_blocking(
        || format!("get_latest_ts:{name}"),
        move || {
            get_latest_ts(
                &progress_client_read_committed,
                &progress_client_read_uncommitted,
                &progress_topic,
                &progress_key,
            )
        },
    )
    .await?
}
