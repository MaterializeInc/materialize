// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use timely::scheduling::activate::SyncActivator;
use tokio::runtime::Handle as TokioHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use mz_expr::PartitionId;
use mz_kafka_util::{client::create_new_client_config, client::MzClientContext};
use mz_ore::thread::{JoinHandleExt, UnparkOnDropHandle};
use mz_repr::{adt::jsonb::Jsonb, GlobalId};

use crate::source::{
    NextMessage, SourceMessage, SourceMessageType, SourceReader, SourceReaderError,
};
use crate::types::connections::{ConnectionContext, KafkaConnection, StringOrSecret};
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::{KafkaOffset, KafkaSourceConnection, MzOffset, SourceConnection};

use self::metrics::KafkaPartitionMetrics;

mod metrics;

/// Contains all information necessary to ingest data from Kafka
pub struct KafkaSourceReader {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source global ID
    id: GlobalId,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<GlueConsumerContext>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: VecDeque<PartitionConsumer>,
    /// Worker ID
    worker_id: usize,
    /// Total count of workers
    worker_count: usize,
    /// Map from partition -> most recently read offset. Can be -1,
    /// if we are starting at the beginning.
    last_offsets: HashMap<i32, KafkaOffset>,
    /// Map from partition -> offset to start reading at. 0-indexed
    start_offsets: HashMap<i32, u64>,
    /// Channel to receive Kafka statistics JSON blobs from the stats callback.
    stats_rx: crossbeam_channel::Receiver<Jsonb>,
    /// The last partition we received
    partition_info: Arc<Mutex<Option<Vec<i32>>>>,
    /// A handle to the spawned metadata thread
    // Drop order is important here, we want the thread to be unparked after the `partition_info`
    // Arc has been dropped, so that the unpacked thread notices it and exits immediately
    _metadata_thread_handle: UnparkOnDropHandle<()>,
    /// A handle to the partition specific metrics
    partition_metrics: KafkaPartitionMetrics,
    /// Whether or not to unpack and allocate headers and pass them through in the `SourceMessage`
    include_headers: bool,
}

impl SourceReader for KafkaSourceReader {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Diff = ();

    /// Create a new instance of a Kafka reader.
    fn new(
        source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        connection: SourceConnection,
        restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        let kc = match connection {
            SourceConnection::Kafka(kc) => kc,
            _ => unreachable!(),
        };
        let KafkaSourceConnection {
            connection,
            options,
            topic,
            group_id_prefix,
            cluster_id,
            ..
        } = kc;
        let kafka_config = TokioHandle::current().block_on(create_kafka_config(
            &source_name,
            group_id_prefix,
            cluster_id,
            &connection,
            &options,
            &connection_context,
        ));
        let (stats_tx, stats_rx) = crossbeam_channel::unbounded();
        let consumer: BaseConsumer<GlueConsumerContext> = kafka_config
            .create_with_context(GlueConsumerContext {
                activator: consumer_activator,
                stats_tx,
            })
            .expect("Failed to create Kafka Consumer");
        let consumer = Arc::new(consumer);

        // Start offsets is a map from pid -> next 0-indexed offset to read from,
        // which is equivalent to 1 + the last 0-indexed offset read.
        let mut start_offsets: HashMap<_, u64> = kc
            .start_offsets
            .into_iter()
            .map(|(k, v)| (k, v.offset))
            .collect();

        // Restored offsets are 1-indexed, so convert to 0-indexed offsets by
        // subtracting 1. The bindings in sqlite already encode 1 offset past the
        // last read offset.
        for (pid, offset) in restored_offsets {
            let pid = match pid {
                PartitionId::Kafka(id) => id,
                _ => panic!("unexpected partition id type"),
            };
            if let Some(offset) = offset {
                if let Some(start_offset) = start_offsets.get_mut(&pid) {
                    *start_offset = std::cmp::max(offset.offset - 1, *start_offset);
                } else {
                    start_offsets.insert(pid, offset.offset - 1);
                }
            }
        }

        let partition_info = Arc::new(Mutex::new(None));
        let metadata_thread_handle = {
            let partition_info = Arc::downgrade(&partition_info);
            let topic = topic.clone();
            let consumer = Arc::clone(&consumer);
            let metadata_refresh_frequency = kafka_config
                .get("topic.metadata.refresh.interval.ms")
                // Safe conversion: statement::extract_config enforces that option is a value
                // between 0 and 3600000
                .map(|s| Duration::from_millis(s.parse().unwrap()))
                // Default value obtained from https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                .unwrap_or_else(|| Duration::from_secs(300));

            thread::Builder::new()
                .name("kafka-metadata".to_string())
                .spawn(move || {
                    while let Some(partition_info) = partition_info.upgrade() {
                        match get_kafka_partitions(&consumer, &topic, Duration::from_secs(30)) {
                            Ok(info) => {
                                *partition_info.lock().unwrap() = Some(info);
                                thread::park_timeout(metadata_refresh_frequency);
                            }
                            Err(_) => thread::park_timeout(Duration::from_secs(30)),
                        }
                    }
                })
                .unwrap()
                .unpark_on_drop()
        };
        let partition_ids = start_offsets.keys().copied().collect();
        Ok(KafkaSourceReader {
            topic_name: topic.clone(),
            source_name,
            id: source_id,
            partition_consumers: VecDeque::new(),
            consumer,
            worker_id,
            worker_count,
            last_offsets: HashMap::new(),
            start_offsets,
            stats_rx,
            partition_info,
            include_headers: kc.include_headers.is_some(),
            _metadata_thread_handle: metadata_thread_handle,
            partition_metrics: KafkaPartitionMetrics::new(metrics, partition_ids, topic, source_id),
        })
    }

    /// This function polls from the next consumer for which a message is available. This function
    /// polls the set round-robin: when a consumer is polled, it is placed at the back of the
    /// queue.
    ///
    /// If a message has an offset that is smaller than the next expected offset for this consumer
    /// (and this partition) we skip this message, and seek to the appropriate offset
    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        let partition_info = self.partition_info.lock().unwrap().take();
        if let Some(partitions) = partition_info {
            for pid in partitions {
                self.add_partition(PartitionId::Kafka(pid));
            }
        }
        let mut next_message = NextMessage::Pending;

        // Poll the consumer once. We split the consumer's partitions out into separate queues and
        // poll those individually, but it's still necessary to drive logic that consumes from
        // rdkafka's internal event queue, such as statistics callbacks.
        //
        // Additionally, assigning topics and splitting them off into separate queues is not
        // atomic, so we expect to see at least some messages to show up when polling the consumer
        // directly.
        if let Some(result) = self.consumer.poll(Duration::from_secs(0)) {
            match result {
                Err(e) => error!(
                    "kafka error when polling consumer for source: {} topic: {} : {}",
                    self.source_name, self.topic_name, e
                ),
                Ok(message) => {
                    let source_message = construct_source_message(&message, self.include_headers)?;
                    next_message = self.handle_message(source_message);
                }
            }
        }

        self.update_stats();

        let consumer_count = self.get_partition_consumers_count();
        let mut attempts = 0;
        while attempts < consumer_count {
            // First, see if we have a message already, either from polling the consumer, above, or
            // from polling the partition queues below.
            if let NextMessage::Ready(_) = next_message {
                // Found a message, exit the loop and return message
                break;
            }

            let message = self.poll_from_next_queue()?;
            attempts += 1;

            if let Some(message) = message {
                next_message = self.handle_message(message);
            }
        }

        Ok(next_message)
    }
}

impl KafkaSourceReader {
    /// Ensures that a partition queue for `pid` exists.
    /// In Kafka, partitions are assigned contiguously. This function consequently
    /// creates partition queues for every p <= pid
    fn add_partition(&mut self, pid: PartitionId) {
        if !crate::source::responsible_for(&self.id, self.worker_id, self.worker_count, &pid) {
            return;
        }
        let pid = match pid {
            PartitionId::Kafka(p) => p,
            _ => unreachable!(),
        };
        if self.last_offsets.contains_key(&pid) {
            return;
        }

        let start_offset = match self.start_offsets.get(&pid) {
            Some(offset) => *offset,
            None => 0,
        };

        let start_offset: i64 = start_offset.try_into().expect("offset to be < i64::MAX");
        self.create_partition_queue(pid, Offset::Offset(start_offset));

        // Indicate a last offset of -1 if we have not been instructed to have a specific start
        // offset for this topic.
        let prev = self.last_offsets.insert(
            pid,
            KafkaOffset {
                offset: start_offset - 1,
            },
        );

        assert!(prev.is_none());
    }

    /// Returns a count of total number of consumers for this source
    fn get_partition_consumers_count(&self) -> i32 {
        // Note: the number of consumers is guaranteed to always be smaller than
        // expected_partition_count (i32)
        self.partition_consumers.len().try_into().unwrap()
    }

    /// Creates a new partition queue for `partition_id`.
    fn create_partition_queue(&mut self, partition_id: i32, initial_offset: Offset) {
        info!(
            "Activating Kafka queue for {} [{}] (source {}) on worker {}",
            self.topic_name, partition_id, self.id, self.worker_id
        );

        // Collect old partition assignments
        let tpl = self.consumer.assignment().unwrap();
        // Create list from assignments
        let mut partition_list = TopicPartitionList::new();
        for partition in tpl.elements_for_topic(&self.topic_name) {
            partition_list
                .add_partition_offset(partition.topic(), partition.partition(), partition.offset())
                .expect("offset known to be valid");
        }
        // Add new partition
        partition_list
            .add_partition_offset(&self.topic_name, partition_id, initial_offset)
            .expect("offset known to be valid");
        self.consumer
            .assign(&partition_list)
            .expect("assignment known to be valid");

        // Since librdkafka v1.6.0, we need to recreate all partition queues
        // after every call to `self.consumer.assign`.
        let context = Arc::clone(&self.consumer.context());
        for pc in &mut self.partition_consumers {
            pc.partition_queue = self
                .consumer
                .split_partition_queue(&self.topic_name, pc.pid)
                .expect("partition known to be valid");
            pc.partition_queue.set_nonempty_callback({
                let context = Arc::clone(&context);
                move || context.activate()
            });
        }

        let mut partition_queue = self
            .consumer
            .split_partition_queue(&self.topic_name, partition_id)
            .expect("partition known to be valid");
        partition_queue.set_nonempty_callback(move || context.activate());
        self.partition_consumers.push_front(PartitionConsumer::new(
            partition_id,
            partition_queue,
            self.include_headers,
        ));
        assert_eq!(
            self.consumer
                .assignment()
                .unwrap()
                .elements_for_topic(&self.topic_name)
                .len(),
            self.partition_consumers.len()
        );
    }

    /// Fast-forward consumer to specified Kafka Offset. Prints a warning if failed to do so
    /// Assumption: if offset does not exist (for instance, because of compaction), will seek
    /// to the next available offset
    fn fast_forward_consumer(&self, pid: i32, next_offset: i64) {
        let res = self.consumer.seek(
            &self.topic_name,
            pid,
            Offset::Offset(next_offset),
            Duration::from_secs(1),
        );
        match res {
            Ok(_) => {
                let res = self.consumer.position().unwrap_or_default().to_topic_map();
                let position = res
                    .get(&(self.topic_name.clone(), pid))
                    .and_then(|p| match p {
                        Offset::Offset(o) => Some(o),
                        _ => None,
                    });
                if let Some(position) = position {
                    if *position != next_offset {
                        warn!("Did not fast-forward consumer on partition PID: {} to the correct Kafka offset. Currently at offset: {} Expected offset: {}",
                              pid, position, next_offset);
                    } else {
                        info!("Successfully fast-forwarded consumer on partition PID: {} to Kafka offset {}.", pid, position);
                    }
                } else {
                    warn!("Tried to fast-forward consumer on partition PID: {} to Kafka offset {}. Could not obtain new consumer position",
                          pid, next_offset);
                }
            }
            Err(e) => error!(
                "Failed to fast-forward consumer for source:{}, Error:{}",
                self.source_name, e
            ),
        }
    }

    /// Read any statistics JSON blobs generated via the rdkafka statistics callback.
    fn update_stats(&mut self) {
        while let Ok(stats) = self.stats_rx.try_recv() {
            match serde_json::from_str::<Statistics>(&stats.to_string()) {
                Ok(statistics) => {
                    let topic = statistics.topics.get(&self.topic_name);
                    match topic {
                        Some(topic) => {
                            for (id, partition) in &topic.partitions {
                                self.partition_metrics
                                    .set_offset_max(*id, partition.hi_offset);
                            }
                        }
                        None => error!("No stats found for topic: {}", &self.topic_name),
                    }
                }
                Err(e) => {
                    error!("failed decoding librdkafka statistics JSON: {}", e);
                }
            }
        }
    }

    /// Polls from the next partition queue and returns the message, if any.
    ///
    /// We maintain the list of partition queues in a queue, and add queues that we polled from to
    /// the end of the queue. We thus swing through all available partition queues in a somewhat
    /// fair manner.
    fn poll_from_next_queue(
        &mut self,
    ) -> Result<Option<SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()>>, anyhow::Error> {
        let mut partition_queue = self.partition_consumers.pop_front().unwrap();

        let message = match partition_queue.get_next_message()? {
            Err(e) => {
                let pid = partition_queue.pid();
                let last_offset = self
                    .last_offsets
                    .get(&pid)
                    .expect("partition known to be installed");

                error!(
                        "kafka error consuming from source: {} topic: {}: partition: {} last processed offset: {} : {}",
                        self.source_name,
                        self.topic_name,
                        pid,
                        last_offset.offset,
                        e
                    );
                None
            }
            Ok(m) => m,
        };

        self.partition_consumers.push_back(partition_queue);

        Ok(message)
    }

    /// Checks if the given message is viable for emission. This checks if the message offset is
    /// past the expected offset and seeks the consumer if it is not.
    fn handle_message(
        &mut self,
        message: SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()>,
    ) -> NextMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()> {
        let partition = match message.partition {
            PartitionId::Kafka(pid) => pid,
            _ => unreachable!(),
        };

        // Convert the received offset back from a 1-indexed MzOffset to the correct offset.
        let offset = message.offset.offset - 1;
        // Offsets are guaranteed to be 1) monotonically increasing *unless* there is
        // a network issue or a new partition added, at which point the consumer may
        // start processing the topic from the beginning, or we may see duplicate offsets
        // At all times, the guarantee : if we see offset x, we have seen all offsets [0,x-1]
        // that we are ever going to see holds.
        // Offsets are guaranteed to be contiguous when compaction is disabled. If compaction
        // is enabled, there may be gaps in the sequence.
        // If we see an "old" offset, we ast-forward the consumer and skip that message

        // Given the explicit consumer to partition assignment, we should never receive a message
        // for a partition for which we have no metadata
        assert!(self.last_offsets.contains_key(&partition));

        let last_offset_ref = self
            .last_offsets
            .get_mut(&partition)
            .expect("partition known to be installed");

        let last_offset = *last_offset_ref;
        let offset_as_i64: i64 = offset.try_into().expect("offset to be < i64::MAX");
        if offset_as_i64 <= last_offset.offset {
            info!(
                "Kafka message before expected offset, skipping: \
                             source {} (reading topic {}, partition {}) \
                             received offset {} expected offset {:?}",
                self.source_name,
                self.topic_name,
                partition,
                offset,
                last_offset.offset + 1,
            );
            // Seek to the *next* 0 indexed offset that we have not yet processed
            self.fast_forward_consumer(partition, last_offset.offset + 1);
            // We explicitly should not consume the message as we have already processed it
            // However, we make sure to activate the source to make sure that we get a chance
            // to read from this consumer again (even if no new data arrives)
            NextMessage::TransientDelay
        } else {
            *last_offset_ref = KafkaOffset {
                offset: offset_as_i64,
            };
            NextMessage::Ready(SourceMessageType::Finalized(message))
        }
    }
}

/// Creates a Kafka config.
///
/// `options` set additional configuration operations from the user. While these
/// look arbitrary, other layers of the system tightly control which
/// configuration options are allowable.
async fn create_kafka_config(
    name: &str,
    group_id_prefix: Option<String>,
    cluster_id: Uuid,
    kafka_connection: &KafkaConnection,
    options: &BTreeMap<String, StringOrSecret>,
    connection_context: &ConnectionContext,
) -> ClientConfig {
    let mut kafka_config = create_new_client_config(connection_context.librdkafka_log_level);

    crate::types::connections::populate_client_config(
        kafka_connection.clone(),
        options,
        std::collections::HashSet::new(),
        &mut kafka_config,
        &*connection_context.secrets_reader,
    )
    .await;

    // Default to disabling Kafka auto commit. This can be explicitly enabled
    // by the user if they want to use it for progress tracking.
    kafka_config.set("enable.auto.commit", "false");

    // Always begin ingest at 0 when restarted, even if Kafka contains committed
    // consumer read offsets
    kafka_config.set("auto.offset.reset", "earliest");

    // How often to refresh metadata from the Kafka broker. This can have a
    // minor impact on startup latency and latency after adding a new partition,
    // as the metadata for a partition must be fetched before we can retrieve
    // data from it. We try to manually trigger metadata fetches when it makes
    // sense, but if those manual fetches fail, this is the interval at which we
    // retry.
    //
    // 30s may seem low, but the default is 5m. More frequent metadata refresh
    // rates are surprising to Kafka users, as topic partition counts hardly
    // ever change in production.
    kafka_config.set("topic.metadata.refresh.interval.ms", "30000"); // 30 seconds

    kafka_config.set("fetch.message.max.bytes", "134217728");

    // Consumer group ID. librdkafka requires this, and we use offset committing
    // to provide a way for users to monitor ingest progress (though we do not
    // rely on the committed offsets for any functionality)
    //
    // This is partially dictated by the user and partially dictated by us.
    // Users can set a prefix so they can see which consumers belong to which
    // Materialize deployment, but we set a suffix to ensure uniqueness. A
    // unique consumer group ID is the most surefire way to ensure that
    // librdkafka does not try to perform its own consumer group balancing,
    // which would wreak havoc with our careful partition assignment strategy.
    kafka_config.set(
        "group.id",
        &format!(
            "{}materialize-{}-{}",
            group_id_prefix.unwrap_or_else(String::new),
            cluster_id,
            name
        ),
    );

    kafka_config
}

fn construct_source_message(
    msg: &BorrowedMessage<'_>,
    include_headers: bool,
) -> Result<SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()>, anyhow::Error> {
    let kafka_offset = KafkaOffset {
        offset: msg.offset(),
    };
    let headers = match msg.headers() {
        Some(headers) if include_headers => Some(
            headers
                .iter()
                .map(|h| (h.key.into(), h.value.map(|v| v.to_vec())))
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };
    Ok(SourceMessage {
        partition: PartitionId::Kafka(msg.partition()),
        offset: kafka_offset.try_into().map_err(|_| {
            anyhow::anyhow!(
                "got negative offset ({}) from otherwise non-error'd kafka message",
                kafka_offset.offset
            )
        })?,
        upstream_time_millis: msg.timestamp().to_millis(),
        key: msg.key().map(|k| k.to_vec()),
        value: msg.payload().map(|p| p.to_vec()),
        headers,
        specific_diff: (),
    })
}

/// Wrapper around a partition containing the underlying consumer
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: i32,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<GlueConsumerContext>,
    /// Whether or not to unpack and allocate headers and pass them through in the `SourceMessage`
    include_headers: bool,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(
        pid: i32,
        partition_queue: PartitionQueue<GlueConsumerContext>,
        include_headers: bool,
    ) -> Self {
        PartitionConsumer {
            pid,
            partition_queue,
            include_headers,
        }
    }

    /// Returns the next message to process for this partition (if any).
    ///
    /// The outer `Result` represents irrecoverable failures, the inner one can and will
    /// be transformed into empty values.
    ///
    /// The inner `Option` represents if there is a message to process.
    fn get_next_message(
        &mut self,
    ) -> Result<
        Result<Option<SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>, ()>>, KafkaError>,
        anyhow::Error,
    > {
        match self.partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(msg)) => {
                let result = construct_source_message(&msg, self.include_headers)?;
                assert_eq!(result.partition, PartitionId::Kafka(self.pid));
                Ok(Ok(Some(result)))
            }
            Some(Err(err)) => Ok(Err(err)),
            _ => Ok(Ok(None)),
        }
    }

    /// Return the partition id for this PartitionConsumer
    fn pid(&self) -> i32 {
        self.pid
    }
}

/// An implementation of [`ConsumerContext`] that forwards statistics to the
/// worker
struct GlueConsumerContext {
    activator: SyncActivator,
    stats_tx: crossbeam_channel::Sender<Jsonb>,
}

impl ClientContext for GlueConsumerContext {
    fn stats_raw(&self, statistics: &[u8]) {
        match Jsonb::from_slice(statistics) {
            Ok(statistics) => {
                self.stats_tx
                    .send(statistics)
                    .expect("timely operator hung up while Kafka source active");
                self.activate();
            }
            Err(e) => error!("failed decoding librdkafka statistics JSON: {}", e),
        };
    }

    // The shape of the rdkafka *Context traits require us to forward to the `MzClientContext`
    // implementation.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        MzClientContext.log(level, fac, log_message)
    }
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        MzClientContext.error(error, reason)
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}

impl ConsumerContext for GlueConsumerContext {}

/// Return the list of partition ids associated with a specific topic
fn get_kafka_partitions(
    consumer: &BaseConsumer<GlueConsumerContext>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    Ok(metadata.topics()[0]
        .partitions()
        .iter()
        .map(|x| x.id())
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{Message, Offset, TopicPartitionList};
    use uuid::Uuid;

    use mz_kafka_util::client::create_new_client_config_simple;

    // Splitting off a partition queue with an `Offset` that is not `Offset::Beginning` seems to
    // lead to a race condition where sometimes we receive messages from polling the main consumer
    // instead of on the partition queue. This can be surfaced by running the test in a loop (in
    // the dataflow directory) using:
    //
    // cargo stress --lib --release source::kafka::tests::reproduce_kafka_queue_issue
    //
    // cargo-stress can be installed via `cargo install cargo-stress`
    //
    // You need to set up a topic "queue-test" with 1000 "hello" messages in it. Obviously, running
    // this test requires a running Kafka instance at localhost:9092.
    #[test]
    #[ignore]
    fn demonstrate_kafka_queue_race_condition() -> Result<(), anyhow::Error> {
        let topic_name = "queue-test";
        let pid = 0;

        let mut kafka_config = create_new_client_config_simple();
        kafka_config.set("bootstrap.servers", "localhost:9092".to_string());
        kafka_config.set("enable.auto.commit", "false");
        kafka_config.set("group.id", Uuid::new_v4().to_string());
        kafka_config.set("fetch.message.max.bytes", "100");
        let consumer: BaseConsumer<_> = kafka_config.create()?;

        let consumer = Arc::new(consumer);

        let mut partition_list = TopicPartitionList::new();
        // Using Offset:Beginning here will work fine, only Offset:Offset(0) leads to the race
        // condition.
        partition_list.add_partition_offset(topic_name, pid, Offset::Offset(0))?;

        consumer.assign(&partition_list)?;

        let partition_queue = consumer
            .split_partition_queue(topic_name, pid)
            .expect("missing partition queue");

        let expected_messages = 1_000;

        let mut common_queue_count = 0;
        let mut partition_queue_count = 0;

        loop {
            if let Some(msg) = consumer.poll(Duration::from_millis(0)) {
                match msg {
                    Ok(msg) => {
                        let _payload =
                            std::str::from_utf8(msg.payload().expect("missing payload"))?;
                        if partition_queue_count > 0 {
                            anyhow::bail!("Got message from common queue after we internally switched to partition queue.");
                        }

                        common_queue_count += 1;
                    }
                    Err(err) => anyhow::bail!("{}", err),
                }
            }

            match partition_queue.poll(Duration::from_millis(0)) {
                Some(Ok(msg)) => {
                    let _payload = std::str::from_utf8(msg.payload().expect("missing payload"))?;
                    partition_queue_count += 1;
                }
                Some(Err(err)) => anyhow::bail!("{}", err),
                _ => (),
            }

            if (common_queue_count + partition_queue_count) == expected_messages {
                break;
            }
        }

        assert!(
            common_queue_count == 0,
            "Got {} out of {} messages from common queue. Partition queue: {}",
            common_queue_count,
            expected_messages,
            partition_queue_count
        );

        Ok(())
    }
}
