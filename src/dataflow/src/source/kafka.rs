// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use timely::scheduling::activate::SyncActivator;

use dataflow_types::{
    ExternalSourceConnector, KafkaOffset, KafkaSourceConnector, MzOffset, SourceDataEncoding,
};
use expr::{PartitionId, SourceInstanceId};
use kafka_util::KafkaAddrs;
use log::{error, info, log_enabled, warn};
use repr::adt::jsonb::Jsonb;
use uuid::Uuid;

use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::source::{NextMessage, SourceMessage, SourceReader};

use super::metrics::SourceBaseMetrics;

/// Contains all information necessary to ingest data from Kafka
pub struct KafkaSourceReader {
    /// Name of the topic on which this source is backed on
    topic_name: String,
    /// Name of the source (will have format kafka-source-id)
    source_name: String,
    /// Source instance ID
    id: SourceInstanceId,
    /// Kafka consumer for this source
    consumer: Arc<BaseConsumer<GlueConsumerContext>>,
    /// List of consumers. A consumer should be assigned per partition to guarantee fairness
    partition_consumers: VecDeque<PartitionConsumer>,
    /// The number of known partitions.
    known_partitions: i32,
    /// Worker ID
    worker_id: i32,
    /// Map from partition -> most recently read offset
    last_offsets: HashMap<i32, i64>,
    /// Map from partition -> offset to start reading at
    start_offsets: HashMap<i32, i64>,
    /// Timely worker logger for source events
    logger: Option<Logger>,
    /// Channel to receive Kafka statistics JSON blobs from the stats callback.
    stats_rx: crossbeam_channel::Receiver<Jsonb>,
    // The last statistics JSON blob received.
    last_stats: Option<Jsonb>,
}

impl SourceReader for KafkaSourceReader {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;

    /// Create a new instance of a Kafka reader.
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _: SourceDataEncoding,
        logger: Option<Logger>,
        _: SourceBaseMetrics,
    ) -> Result<(KafkaSourceReader, Option<PartitionId>), anyhow::Error> {
        match connector {
            ExternalSourceConnector::Kafka(kc) => Ok((
                KafkaSourceReader::new(
                    source_name,
                    source_id,
                    worker_id,
                    consumer_activator,
                    kc,
                    logger,
                ),
                None,
            )),
            _ => unreachable!(),
        }
    }
    /// Ensures that a partition queue for `pid` exists.
    /// In Kafka, partitions are assigned contiguously. This function consequently
    /// creates partition queues for every p <= pid
    fn add_partition(&mut self, pid: PartitionId, restored_offset: Option<MzOffset>) {
        let pid = match pid {
            PartitionId::Kafka(p) => p,
            _ => unreachable!(),
        };

        // Passed-in initial offsets take precedence over potential user-configured start offsets.
        // The reason is that an initial offset most likely comes from source state that we
        // restored from persistence while start offsets are something that a user configured
        // during the initial creation of the source. When restarting, we don't want to go all the
        // way back to those starting offsets.
        let start_offset = if let Some(restored_offset) = restored_offset {
            // We can either start after the first message, which would be `MzOffset{1}` or not
            // have a start offset. Other places use `MzOffset{0}` to denote a missing offset but
            // we chose to use the more idiomatic `Option<MzOffset>` here.
            assert!(
                restored_offset.offset >= 1,
                "Invalid initial offset {}",
                restored_offset
            );

            // We subtract 1 here to convert from MzOffset (which is 1-based) to Kafka offset
            // (which are 0-based).
            let restored_offset = restored_offset.offset - 1;

            // Also verify that we didn't regress from any user-configured start offsets.
            if let Some(start_offset) = self.start_offsets.get(&pid) {
                assert!(restored_offset >= *start_offset);
            }

            // We subtract 1 again because this will be put into `last_offsets`, which record the
            // last offset that we read. A start offset of 5 means that the last read offset would
            // have to be 4.
            //
            // Because of the assert above, this cannot go below -1, which would indicate that we
            // start reading at 0.
            restored_offset - 1
        } else {
            // Indicate a last offset of -1 if we have not been instructed to have a specific start
            // offset for this topic.
            *self.start_offsets.get(&pid).unwrap_or(&-1)
        };

        // Seek to the *next* offset (aka start_offset + 1) that we have not yet processed
        self.create_partition_queue(pid, Offset::Offset(start_offset + 1));

        let prev = self.last_offsets.insert(pid, start_offset);

        assert!(prev.is_none());
        self.known_partitions = cmp::max(self.known_partitions, pid + 1);
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error> {
        self.get_next_kafka_message()
    }
}

impl KafkaSourceReader {
    /// Constructor
    pub fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        kc: KafkaSourceConnector,
        logger: Option<Logger>,
    ) -> KafkaSourceReader {
        let KafkaSourceConnector {
            addrs,
            topic,
            config_options,
            group_id_prefix,
            cluster_id,
            ..
        } = kc;
        let worker_id = worker_id.try_into().unwrap();
        let kafka_config = create_kafka_config(
            &source_name,
            &addrs,
            group_id_prefix,
            cluster_id,
            &config_options,
        );
        let (stats_tx, stats_rx) = crossbeam_channel::unbounded();
        let consumer: BaseConsumer<GlueConsumerContext> = kafka_config
            .create_with_context(GlueConsumerContext {
                activator: consumer_activator,
                stats_tx: stats_tx,
            })
            .expect("Failed to create Kafka Consumer");

        let start_offsets = kc.start_offsets.iter().map(|(k, v)| (*k, v - 1)).collect();

        KafkaSourceReader {
            topic_name: topic,
            source_name,
            id: source_id,
            partition_consumers: VecDeque::new(),
            known_partitions: 0,
            consumer: Arc::new(consumer),
            worker_id,
            last_offsets: HashMap::new(),
            start_offsets,
            logger,
            stats_rx,
            last_stats: None,
        }
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
        let context = self.consumer.context().clone();
        for pc in &mut self.partition_consumers {
            pc.partition_queue = self
                .consumer
                .split_partition_queue(&self.topic_name, pc.pid)
                .expect("partition known to be valid");
            pc.partition_queue.set_nonempty_callback({
                let context = context.clone();
                move || context.activate()
            });
        }

        let mut partition_queue = self
            .consumer
            .split_partition_queue(&self.topic_name, partition_id)
            .expect("partition known to be valid");
        partition_queue.set_nonempty_callback(move || context.activate());
        self.partition_consumers
            .push_front(PartitionConsumer::new(partition_id, partition_queue));
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

    /// This function polls from the next consumer for which a message is available. This function polls the set
    /// round-robin: when a consumer is polled, it is placed at the back of the queue.
    ///
    /// If a message has an offset that is smaller than the next expected offset for this consumer (and this partition)
    /// we skip this message, and seek to the appropriate offset
    fn get_next_kafka_message(
        &mut self,
    ) -> Result<NextMessage<Option<Vec<u8>>, Option<Vec<u8>>>, anyhow::Error> {
        // Poll the consumer once. Since we split the consumer's partitions out into separate queues and poll those individually,
        // we expect this poll to always return None - but it's necessary to drive logic that consumes from rdkafka's internal
        // event queue, such as statistics callbacks.
        if let Some(result) = self.consumer.poll(Duration::from_secs(0)) {
            match result {
                Err(e) => error!(
                    "kafka error when polling consumer for source: {} topic: {} : {}",
                    self.source_name, self.topic_name, e
                ),
                Ok(m) => error!(
                    "unexpected receipt of kafka message from non-partitioned queue for source: {} topic: {} partition: {} offset: {}",
                    self.source_name, self.topic_name, m.partition(), m.offset()
                ),
            }
        }

        // Read any statistics JSON blobs generated via the rdkafka statistics
        // callback.
        while let Ok(stats) = self.stats_rx.try_recv() {
            if let Some(logger) = self.logger.as_mut() {
                logger.log(MaterializedEvent::KafkaSourceStatistics {
                    source_id: self.id,
                    old: self.last_stats.take(),
                    new: Some(stats.clone()),
                });
                self.last_stats = Some(stats);
            }
        }

        let mut next_message = NextMessage::Pending;
        let consumer_count = self.get_partition_consumers_count();
        let mut attempts = 0;
        while attempts < consumer_count {
            let mut partition_queue = self.partition_consumers.pop_front().unwrap();
            let message = match partition_queue.get_next_message() {
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
                        last_offset,
                        e
                    );
                    None
                }
                Ok(m) => m,
            };

            if let Some(message) = message {
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
                if offset <= last_offset {
                    info!(
                        "Kafka message before expected offset, skipping: \
                             source {} (reading topic {}, partition {}) \
                             received offset {} expected offset {:?}",
                        self.source_name,
                        self.topic_name,
                        partition,
                        offset,
                        last_offset + 1,
                    );
                    // Seek to the *next* offset (aka last_offset + 1) that we have not yet processed
                    self.fast_forward_consumer(partition, last_offset + 1);
                    // We explicitly should not consume the message as we have already processed it
                    // However, we make sure to activate the source to make sure that we get a chance
                    // to read from this consumer again (even if no new data arrives)
                    next_message = NextMessage::TransientDelay;
                } else {
                    next_message = NextMessage::Ready(message);
                    *last_offset_ref = offset;
                }
            }
            self.partition_consumers.push_back(partition_queue);
            if let NextMessage::Ready(_) = next_message {
                // Found a message, exit the loop and return message
                break;
            } else {
                attempts += 1;
            }
        }

        Ok(next_message)
    }
}

impl Drop for KafkaSourceReader {
    fn drop(&mut self) {
        // Retract any metrics logged for this source.
        if let Some(logger) = self.logger.as_mut() {
            logger.log(MaterializedEvent::KafkaSourceStatistics {
                source_id: self.id,
                old: self.last_stats.take(),
                new: None,
            });
        }
    }
}

/// Creates a Kafka config.
fn create_kafka_config(
    name: &str,
    addrs: &KafkaAddrs,
    group_id_prefix: Option<String>,
    cluster_id: Uuid,
    config_options: &BTreeMap<String, String>,
) -> ClientConfig {
    let mut kafka_config = ClientConfig::new();

    // Broker configuration.
    kafka_config.set("bootstrap.servers", &addrs.to_string());

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

    // Consumer group ID. librdkafka requires this, and we use offset commiting
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

    // Patch the librdkafka debug log system into the Rust `log` ecosystem.
    // This is a very simple integration at the moment; enabling `debug`-level
    // logs for the `librdkafka` target enables the full firehouse of librdkafka
    // debug logs. We may want to investigate finer-grained control.
    if log_enabled!(target: "librdkafka", log::Level::Debug) {
        kafka_config.set("debug", "all");
    }

    // Set additional configuration operations from the user. While these look
    // arbitrary, other layers of the system tightly control which configuration
    // options are allowable.
    for (k, v) in config_options {
        kafka_config.set(k, v);
    }

    kafka_config
}

impl<'a> From<&BorrowedMessage<'a>> for SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>> {
    fn from(msg: &BorrowedMessage<'a>) -> Self {
        let kafka_offset = KafkaOffset {
            offset: msg.offset(),
        };
        Self {
            partition: PartitionId::Kafka(msg.partition()),
            offset: kafka_offset.into(),
            upstream_time_millis: msg.timestamp().to_millis(),
            key: msg.key().map(|k| k.to_vec()),
            value: msg.payload().map(|p| p.to_vec()),
        }
    }
}

/// Wrapper around a partition containing the underlying consumer
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: i32,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<GlueConsumerContext>,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(pid: i32, partition_queue: PartitionQueue<GlueConsumerContext>) -> Self {
        PartitionConsumer {
            pid,
            partition_queue,
        }
    }

    /// Returns the next message to process for this partition (if any).
    fn get_next_message(
        &mut self,
    ) -> Result<Option<SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>>, KafkaError> {
        match self.partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(msg)) => {
                let result = SourceMessage::from(&msg);
                assert_eq!(result.partition, PartitionId::Kafka(self.pid));
                Ok(Some(result))
            }
            Some(Err(err)) => Err(err),
            _ => Ok(None),
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
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}

impl ConsumerContext for GlueConsumerContext {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
    use uuid::Uuid;

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

        let mut kafka_config = ClientConfig::new();
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
