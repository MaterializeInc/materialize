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
use mz_kafka_util::client::{GetPartitionsError, MzClientContext, TimeoutConfig};
use mz_ore::collections::CollectionExt;
use mz_ore::task;
use mz_repr::Timestamp;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{ContextCreationError, ContextCreationErrorExt};
use mz_storage_types::sinks::KafkaSinkConnection;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Deserializer, Serialize};
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::{info, warn};

use crate::sink::progress_key::ProgressKey;

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

/// This is the legacy struct that used to be emitted as part of a transactional produce and
/// contains the largest timestamp within the batch committed. Since it is just a timestamp it
/// cannot encode the fact that a sink has finished and deviates from upper frontier semantics.
/// Materialize no longer produces this record but it's possible that we encounter this in topics
/// written by older versions. In those cases we convert it into upper semantics by stepping the
/// timestamp forward.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyProgressRecord {
    // Double Option to tell apart an omitted field from one set to null explicitly
    // https://github.com/serde-rs/serde/issues/984
    #[serde(default, deserialize_with = "deserialize_some")]
    pub timestamp: Option<Option<Timestamp>>,
}

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

/// This struct is emitted as part of a transactional produce, and contains the upper frontier of
/// the batch committed. It is used to recover the frontier a sink needs to resume at.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgressRecord {
    pub frontier: Vec<Timestamp>,
}

fn parse_progress_record(payload: &[u8]) -> Result<Antichain<Timestamp>, anyhow::Error> {
    Ok(match serde_json::from_slice::<ProgressRecord>(payload) {
        Ok(progress) => Antichain::from(progress.frontier),
        // If we fail to deserialize we might be reading a legacy progress record
        Err(_) => match serde_json::from_slice::<LegacyProgressRecord>(payload) {
            Ok(LegacyProgressRecord {
                timestamp: Some(Some(time)),
            }) => Antichain::from_elem(time.step_forward()),
            Ok(LegacyProgressRecord {
                timestamp: Some(None),
            }) => Antichain::new(),
            _ => match std::str::from_utf8(payload) {
                Ok(payload) => bail!("invalid progress record: {payload}"),
                Err(_) => bail!("invalid progress record bytes: {payload:?}"),
            },
        },
    })
}

/// Determines the latest progress record from the specified topic for the given
/// progress key.
///
/// IMPORTANT: to achieve exactly once guarantees, the producer that will resume
/// production at the returned timestamp *must* have called `init_transactions`
/// prior to calling this method.
pub async fn determine_sink_resume_upper(
    sink_id: mz_repr::GlobalId,
    connection: &KafkaSinkConnection,
    storage_configuration: &StorageConfiguration,
) -> Result<Option<Antichain<Timestamp>>, ContextCreationError> {
    // ****************************** WARNING ******************************
    // Be VERY careful when editing the code in this function. It is very easy
    // to accidentally introduce a correctness or liveness bug when refactoring
    // this code.
    // ****************************** WARNING ******************************

    let TimeoutConfig {
        fetch_metadata_timeout,
        progress_record_fetch_timeout,
        ..
    } = storage_configuration.parameters.kafka_timeout_config;

    let client_id = connection.client_id(&storage_configuration.connection_context, sink_id);
    let group_id = connection.progress_group_id(&storage_configuration.connection_context, sink_id);
    let progress_topic = connection
        .progress_topic(&storage_configuration.connection_context)
        .into_owned();
    let progress_key = ProgressKey::new(sink_id);

    let common_options = btreemap! {
        // Consumer group ID, which may have been overridden by the user. librdkafka requires this,
        // even though we'd prefer to disable the consumer group protocol entirely.
        "group.id" => group_id,
        // Allow Kafka monitoring tools to identify this consumer.
        "client.id" => client_id,
        "enable.auto.commit" => "false".into(),
        "auto.offset.reset" => "earliest".into(),
        // The fetch loop below needs EOF notifications to reliably detect that we have reached the
        // high watermark.
        "enable.partition.eof" => "true".into(),
    };

    // Construct two cliens in read committed and read uncommitted isolations respectively. See
    // comment below for an explanation on why we need it.
    let progress_client_read_committed: BaseConsumer<_> = {
        let mut opts = common_options.clone();
        opts.insert("isolation.level", "read_committed".into());
        let ctx = MzClientContext::default();
        connection
            .connection
            .create_with_context(storage_configuration, ctx, &opts)
            .await?
    };

    let progress_client_read_uncommitted: BaseConsumer<_> = {
        let mut opts = common_options;
        opts.insert("isolation.level", "read_uncommitted".into());
        let ctx = MzClientContext::default();
        connection
            .connection
            .create_with_context(storage_configuration, ctx, &opts)
            .await?
    };

    let ctx = Arc::clone(progress_client_read_committed.client().context());

    // Ensure the progress topic exists.
    ensure_kafka_topic(
        connection,
        storage_configuration,
        &progress_topic,
        TopicConfig {
            partition_count: 1,
            // TODO: introduce and use `PROGRESS TOPIC REPLICATION FACTOR`
            // on Kafka connections.
            replication_factor: -1,
            cleanup_policy: TopicCleanupPolicy::Compaction,
        },
    )
    .await
    .add_context("error registering kafka progress topic for sink")?;

    let task_name = format!("get_latest_ts:{sink_id}");
    task::spawn_blocking(|| task_name, move || {
        let progress_topic = progress_topic.as_ref();
        // Ensure the progress topic has exactly one partition. Kafka only
        // guarantees ordering within a single partition, and we need a strict
        // order on the progress messages we read and write.
        let partitions = match mz_kafka_util::client::get_partitions(
            progress_client_read_committed.client(),
            progress_topic,
            fetch_metadata_timeout,
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
            .fetch_watermarks(progress_topic, partition, fetch_metadata_timeout)
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
        let mut last_upper = None;
        while get_position()? < hi {
            let message = match progress_client_read_committed.poll(progress_record_fetch_timeout) {
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

            let Some(payload) = message.payload() else {
                continue
            };
            let upper = parse_progress_record(payload)?;

            match last_upper {
                Some(last_upper) if !PartialOrder::less_equal(&last_upper, &upper) => {
                    bail!(
                        "upper regressed in topic {progress_topic}:{partition} \
                        from {last_upper:?} to {upper:?}"
                    );
                }
                _ => last_upper = Some(upper),
            }
        }

        // If we get here, we are assured that we've read all messages up to
        // the high water mark, and therefore `last_timestamp` contains the
        // most recent timestamp for the sink under consideration.
        Ok(last_upper)
    }).await.unwrap().check_ssh_status(&ctx)
}

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
    fn progress_record_migration() {
        assert!(parse_progress_record(b"{}").is_err());

        assert_eq!(
            parse_progress_record(b"{\"timestamp\":1}").unwrap(),
            Antichain::from_elem(2.into()),
        );

        assert_eq!(
            parse_progress_record(b"{\"timestamp\":null}").unwrap(),
            Antichain::new(),
        );

        assert_eq!(
            parse_progress_record(b"{\"frontier\":[1]}").unwrap(),
            Antichain::from_elem(1.into()),
        );

        assert_eq!(
            parse_progress_record(b"{\"frontier\":[]}").unwrap(),
            Antichain::new(),
        );

        assert!(parse_progress_record(b"{\"frontier\":null}").is_err());
    }
}
