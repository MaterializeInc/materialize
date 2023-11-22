// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use futures::TryFutureExt as _;
use maplit::btreemap;
use mz_kafka_util::client::{
    GetPartitionsError, MzClientContext, TunnelingClientContext, DEFAULT_FETCH_METADATA_TIMEOUT,
};
use mz_ore::collections::CollectionExt;
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::connections::{ConnectionContext, KafkaConnection};
use mz_storage_types::errors::{ContextCreationError, ContextCreationErrorExt};
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkAvroFormatState, KafkaSinkConnection,
    KafkaSinkConnectionRetention, KafkaSinkFormat,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{OwnedMessage, ToBytes};
use rdkafka::producer::{
    BaseRecord, DeliveryResult, Producer as _, ProducerContext, ThreadedProducer,
};
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::sink::kafka::SinkMetrics;

// 30s is a good maximum backoff for network operations. Long enough to reduce
// load on an upstream system, but short enough that we can respond quickly when
// the upstream system comes back online.
pub(super) const BACKOFF_CLAMP: Duration = Duration::from_secs(30);

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

pub async fn publish_schemas(
    connection_cx: &ConnectionContext,
    format: &mut KafkaSinkFormat,
    topic: &str,
) -> Result<(), ContextCreationError> {
    if let KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::UnpublishedMaybe {
        key_schema,
        value_schema,
        csr_connection,
    }) = format
    {
        let ccsr = csr_connection.connect(connection_cx).await?;
        let (key_schema_id, value_schema_id) = publish_kafka_schemas(
            &ccsr,
            topic,
            key_schema.as_deref(),
            Some(mz_ccsr::SchemaType::Avro),
            value_schema,
            mz_ccsr::SchemaType::Avro,
        )
        .await
        .context("error publishing kafka schemas for sink")?;

        *format = KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::Published {
            key_schema_id,
            value_schema_id,
        });
    }

    Ok(())
}

/// Ensures that the Kafka sink's data and consistency collateral exist.
///
/// Out of convenience, we also return the most recent timestamp persisted to
/// the consistency collateral because we use it in this function.
///
/// # Errors
/// - If the [`KafkaSinkConnection`]'s consistency collateral exists and
///   contains data for this sink, but the sink's data topic does not exist.
pub(super) async fn build_kafka(
    sink_id: mz_repr::GlobalId,
    connection: &KafkaSinkConnection,
    connection_cx: &ConnectionContext,
    producer: &KafkaTxProducer,
) -> Result<Option<Timestamp>, ContextCreationError> {
    let client: AdminClient<_> = connection
        .connection
        .create_with_context(connection_cx, MzClientContext::default(), &BTreeMap::new())
        .await
        .add_context("creating admin client failed")?;

    let latest_ts = match &connection.consistency_config {
        KafkaConsistencyConfig::Progress { topic } => {
            ensure_kafka_topic(
                &client,
                topic,
                1,
                connection.replication_factor,
                KafkaSinkConnectionRetention::default(),
            )
            .await
            .check_ssh_status(client.inner().context())
            .add_context("error registering kafka consistency topic for sink")?;

            let progress_client: BaseConsumer<_> = connection
                .connection
                .create_with_context(
                    connection_cx,
                    MzClientContext::default(),
                    &btreemap! {
                        "group.id" => SinkGroupId::new(sink_id),
                        "isolation.level" => "read_uncommitted".into(),
                        "enable.auto.commit" => "false".into(),
                        "auto.offset.reset" => "earliest".into(),
                        "enable.partition.eof" => "true".into(),
                    },
                )
                .await?;

            let progress_client = Arc::new(progress_client);
            let latest_ts = determine_latest_progress_record(
                format!("build_kafka_{}", sink_id),
                topic.to_string(),
                ProgressKey::new(sink_id),
                Arc::clone(&progress_client),
                producer,
            )
            .await
            .check_ssh_status(progress_client.client().context())?;

            // Check for existence of progress topic; if it exists and contains data for
            // this sink, we expect the data topic to exist, as well. Note that we don't
            // expect the converse to be true because we don't want to prevent users
            // from creating topics before setting up their sinks.
            let meta = client
                .inner()
                .fetch_metadata(None, Duration::from_secs(10))
                .check_ssh_status(client.inner().context())
                .add_context("fetching metadata")?;

            if latest_ts.is_some() && !meta.topics().iter().any(|t| t.name() == connection.topic) {
                Err(anyhow::anyhow!(
                    "sink progress data exists, but sink data topic is missing"
                ))?;
            }

            latest_ts
        }
    };

    // Create Kafka topic.
    ensure_kafka_topic(
        &client,
        &connection.topic,
        connection.partition_count,
        connection.replication_factor,
        connection.retention,
    )
    .await
    .check_ssh_status(client.inner().context())
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
/// key (e.g. akin to a sink's GlobalId).
pub(super) async fn determine_latest_progress_record(
    name: String,
    progress_topic: String,
    progress_key: String,
    progress_client: Arc<BaseConsumer<TunnelingClientContext<MzClientContext>>>,
    producer: &KafkaTxProducer,
) -> Result<Option<Timestamp>, anyhow::Error> {
    // Ensure that the producer has initialized transactions and is the current
    // holder of its transaction ID. We need this to ensure that we get the most
    // recent offsets from the progress topic.
    producer.begin_transaction().await?;
    producer.abort_transaction().await?;

    // Polls a message from a Kafka Source.  Blocking so should always be called on background
    // thread.
    fn get_next_message<C>(
        consumer: &BaseConsumer<C>,
        timeout: Duration,
    ) -> Result<Option<(Vec<u8>, Vec<u8>, i64)>, anyhow::Error>
    where
        C: ConsumerContext,
    {
        match consumer.poll(timeout) {
            Some(Ok(message)) => match message.payload() {
                Some(p) => Ok(Some((
                    message.key().unwrap_or(&[]).to_vec(),
                    p.to_vec(),
                    message.offset(),
                ))),
                None => bail!("unexpected null payload"),
            },
            Some(Err(KafkaError::PartitionEOF(_))) => Ok(None),
            Some(Err(err)) => bail!("Failed to process message {}", err),
            None => Ok(None),
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
        let partitions = match mz_kafka_util::client::get_partitions(
            progress_client.client(),
            progress_topic,
            timeout,
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

        info!("fetching latest progress record for {progress_key}, lo/hi: {lo}/{hi}");

        // Empty topic. Return early to avoid unnecessary call to kafka below.
        if hi == 0 {
            return Ok(None);
        }

        let mut latest_ts = None;
        let mut latest_offset = None;
        while let Some((key, message, offset)) = get_next_message(progress_client, timeout)? {
            assert!(latest_offset < Some(offset));
            latest_offset = Some(offset);

            if &key != progress_key.as_bytes() {
                continue;
            }

            let ProgressRecord { timestamp } = serde_json::from_slice(&message)?;
            match latest_ts {
                Some(prev_ts) if timestamp < prev_ts => {
                    bail!(
                        "timestamp regressed in topic {progress_topic}:{partition} \
                         from {prev_ts} to {timestamp}"
                    );
                }
                _ => latest_ts = Some(timestamp),
            };

            let position = progress_client
                .position()?
                .find_partition(progress_topic, partition)
                .ok_or_else(|| anyhow!("No progress info for known partition"))?
                .offset();

            if let Offset::Offset(upper) = position {
                if hi <= upper {
                    break;
                }
            }
        }

        let position = progress_client
            .position()?
            .find_partition(progress_topic, partition)
            .ok_or_else(|| anyhow!("No progress info for known partition"))?
            .offset();

        // We must check that we indeed read all messages until the high watermark because the
        // previous loop could early exit due to a timeout or partition EOF.
        match position {
            Offset::Offset(upper) if hi <= upper => Ok(latest_ts),
            _ => Err(anyhow!(
                "failed to reach high watermark of non-empty \
                 topic {progress_topic}:{partition}, lo/hi: {lo}/{hi}"
            )),
        }
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

#[derive(Clone)]
pub(super) struct SinkProducerContext {
    pub metrics: Arc<SinkMetrics>,
    pub retry_manager: Arc<Mutex<KafkaSinkSendRetryManager>>,
    pub inner: MzClientContext,
}

impl ClientContext for SinkProducerContext {
    // The shape of the rdkafka *Context traits require us to forward to the `MzClientContext`
    // implementation.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }
    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }
}

impl ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _: Self::DeliveryOpaque) {
        match result {
            Ok(_) => self.retry_manager.blocking_lock().record_success(),
            Err((e, msg)) => {
                self.metrics.message_delivery_errors_counter.inc();
                // TODO: figure out a good way to back these retries off.  Should be okay without
                // because we seem to very rarely end up in a constant state where rdkafka::send
                // works but everything is immediately rejected and hits this branch.
                warn!("Kafka producer delivery error {:?} for {:?}", e, msg);
                self.retry_manager
                    .blocking_lock()
                    .record_error(msg.detach());
            }
        }
    }
}

pub(super) struct KafkaTxProducerConfig<'a> {
    pub name: String,
    pub connection: &'a KafkaConnection,
    pub connection_context: &'a ConnectionContext,
    pub producer_context: SinkProducerContext,
    pub sink_id: GlobalId,
    pub worker_id: String,
}

#[derive(Clone)]
pub(super) struct KafkaTxProducer {
    pub name: String,
    pub inner: Arc<ThreadedProducer<TunnelingClientContext<SinkProducerContext>>>,
    pub timeout: Duration,
}

impl KafkaTxProducer {
    pub async fn new<'a>(
        KafkaTxProducerConfig {
            name,
            connection,
            connection_context,
            producer_context,
            sink_id,
            worker_id,
        }: KafkaTxProducerConfig<'a>,
    ) -> Result<KafkaTxProducer, ContextCreationError> {
        let producer = connection
            .create_with_context(
                connection_context,
                producer_context,
                &btreemap! {
                    // Ensure that messages are sinked in order and without
                    // duplicates. Note that this only applies to a single
                    // instance of a producer - in the case of restarts, all
                    // bets are off and full exactly once support is required.
                    "enable.idempotence" => "true".into(),
                    // Increase limits for the Kafka producer's internal
                    // buffering of messages Currently we don't have a great
                    // backpressure mechanism to tell indexes or views to slow
                    // down, so the only thing we can do with a message that we
                    // can't immediately send is to put it in a buffer and
                    // there's no point having buffers within the dataflow layer
                    // and Kafka If the sink starts falling behind and the
                    // buffers start consuming too much memory the best thing to
                    // do is to drop the sink Sets the buffer size to be 16 GB
                    // (note that this setting is in KB)
                    "queue.buffering.max.kbytes" => format!("{}", 16 << 20),
                    // Set the max messages buffered by the producer at any time
                    // to 10MM which is the maximum allowed value.
                    "queue.buffering.max.messages" => format!("{}", 10_000_000),
                    // Make the Kafka producer wait at least 10 ms before
                    // sending out MessageSets TODO(rkhaitan): experiment with
                    // different settings for this value to see if it makes a
                    // big difference.
                    "queue.buffering.max.ms" => format!("{}", 10),
                    "transactional.id" => format!("mz-producer-{sink_id}-{worker_id}"),
                    // Time out transactions after 10 seconds
                    "transaction.timeout.ms" => format!("{}", 10_000),
                },
            )
            .await?;

        let producer = KafkaTxProducer {
            name,
            inner: Arc::new(producer),
            timeout: Duration::from_secs(5),
        };

        producer
            .retry_on_txn_error(|p| p.init_transactions())
            .await?;

        Ok(producer)
    }

    pub fn init_transactions(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("init_transactions:{}", self.name),
            move || self_producer.init_transactions(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    pub fn begin_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        task::spawn_blocking(
            || format!("begin_transaction:{}", self.name),
            move || self_producer.begin_transaction(),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    pub fn commit_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("commit_transaction:{}", self.name),
            move || self_producer.commit_transaction(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    pub fn abort_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("abort_transaction:{}", self.name),
            move || self_producer.abort_transaction(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    pub fn flush(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("flush:{}", self.name),
            move || self_producer.flush(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<(), (KafkaError, Box<BaseRecord<'a, K, P>>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.inner
            .send(record)
            // box the entire record the rdkakfa crate gives us back
            .map_err(|(e, record)| (e, Box::new(record)))
    }

    pub async fn retry_on_txn_error<'a, F, Fut, T>(&self, f: F) -> Result<T, anyhow::Error>
    where
        F: Fn(KafkaTxProducer) -> Fut,
        Fut: Future<Output = KafkaResult<T>>,
    {
        Retry::default()
            .clamp_backoff(BACKOFF_CLAMP)
            .max_tries(3)
            .retry_async(|_| async {
                match f(self.clone()).await {
                    Ok(value) => RetryResult::Ok(value),
                    Err(KafkaError::Transaction(e)) if e.txn_requires_abort() => {
                        // Make one attempt at aborting the transaction before letting the error
                        // percolate up and the process exit. Aborting allows the consumers of the
                        // topic to skip over any messages we've written in the transaction, so it's
                        // polite to do... but if it fails, the transaction will be aborted either
                        // when fenced out by a future version of this producer or by the
                        // broker-side timeout.
                        if let Err(e) = self.abort_transaction().await {
                            warn!(
                                error =? e,
                                "failed to abort transaction after an error that required it"
                            );
                        }
                        RetryResult::FatalErr(
                            anyhow!(e).context("transaction error requiring abort"),
                        )
                    }
                    Err(KafkaError::Transaction(e)) if e.is_retriable() => {
                        RetryResult::RetryableErr(anyhow!(e).context("retriable transaction error"))
                    }
                    Err(e) => {
                        RetryResult::FatalErr(anyhow!(e).context("non-retriable transaction error"))
                    }
                }
            })
            .await
    }
}

#[derive(Clone)]
pub(super) struct KafkaSinkSendRetryManager {
    // Because we flush this fully before moving onto the next timestamp, this queue cannot contain
    // more messages than are contained in a single timely timestamp.  It's also unlikely to get
    // beyond the size of the rdkafka internal message queue because that seems to stop accepting
    // new messages when errors are being returned by the delivery callback -- though we should not
    // rely on that fact if/when we eventually (tm) get around to making our queues bounded.
    //
    // If perf becomes an issue, we can do something slightly more complex with a crossbeam channel
    pub q: VecDeque<OwnedMessage>,
    pub outstanding_send_count: u64,
}

impl KafkaSinkSendRetryManager {
    pub fn new() -> Self {
        Self {
            q: VecDeque::new(),
            outstanding_send_count: 0,
        }
    }
    pub fn record_send(&mut self) {
        self.outstanding_send_count += 1;
    }
    pub fn record_error(&mut self, msg: OwnedMessage) {
        self.q.push_back(msg);
        self.outstanding_send_count -= 1;
    }
    pub fn record_success(&mut self) {
        self.outstanding_send_count -= 1;
    }
    pub fn sends_flushed(&mut self) -> bool {
        self.outstanding_send_count == 0 && self.q.is_empty()
    }
    pub fn pop_retry(&mut self) -> Option<OwnedMessage> {
        self.q.pop_front()
    }
}
