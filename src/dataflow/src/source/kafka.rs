// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::statistics::Window;
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics, TopicPartitionList};
use timely::scheduling::activate::SyncActivator;

use dataflow_types::{DataEncoding, ExternalSourceConnector, KafkaOffset, KafkaSourceConnector};
use expr::{PartitionId, SourceInstanceId};
use kafka_util::KafkaAddrs;
use log::{error, info, log_enabled, warn};
use uuid::Uuid;

use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::source::{NextMessage, SourceMessage, SourceReader};

/// Values recorded from the last rdkafka statistics callback, used to generate a
/// diff of values for logging
#[derive(Default)]
pub struct PreviousStats {
    consumer_name: Option<String>,
    rxmsgs: i64,
    rxbytes: i64,
    txmsgs: i64,
    txbytes: i64,
    lo_offset: i64,
    hi_offset: i64,
    ls_offset: i64,
    app_offset: i64,
    consumer_lag: i64,
}

#[derive(Clone, Copy, Default)]
pub struct BrokerRTTWindow {
    min: i64,
    max: i64,
    avg: i64,
    sum: i64,
    cnt: i64,
    stddev: i64,
    p50: i64,
    p75: i64,
    p90: i64,
    p95: i64,
    p99: i64,
    p99_99: i64,
}

impl BrokerRTTWindow {
    /// Return the value required to negate the last value recorded for this window
    fn negate(
        self,
        consumer_name: String,
        source_id: SourceInstanceId,
        broker_name: String,
    ) -> MaterializedEvent {
        MaterializedEvent::KafkaBrokerRtt {
            consumer_name: consumer_name,
            source_id: source_id,
            broker_name: broker_name,
            min: -self.min,
            max: -self.max,
            avg: -self.avg,
            sum: -self.sum,
            cnt: -self.cnt,
            stddev: -self.stddev,
            p50: -self.p50,
            p75: -self.p75,
            p90: -self.p90,
            p95: -self.p95,
            p99: -self.p99,
            p99_99: -self.p99_99,
        }
    }
    /// Update the value for window, returning a MaterializedEvent that represents the
    /// difference between the previous values and the new values
    fn update(
        &mut self,
        consumer_name: String,
        source_id: SourceInstanceId,
        broker_name: String,
        stats: &Window,
    ) -> MaterializedEvent {
        let event = MaterializedEvent::KafkaBrokerRtt {
            consumer_name,
            source_id,
            broker_name,
            min: stats.min - self.min,
            max: stats.max - self.max,
            avg: stats.avg - self.avg,
            sum: stats.sum - self.sum,
            cnt: stats.cnt - self.cnt,
            stddev: stats.stddev - self.stddev,
            p50: stats.p50 - self.p50,
            p75: stats.p75 - self.p75,
            p90: stats.p90 - self.p90,
            p95: stats.p95 - self.p95,
            p99: stats.p99 - self.p99,
            p99_99: stats.p99_99 - self.p99_99,
        };

        self.min = stats.min;
        self.max = stats.max;
        self.avg = stats.avg;
        self.sum = stats.sum;
        self.cnt = stats.cnt;
        self.stddev = stats.stddev;
        self.p50 = stats.p50;
        self.p75 = stats.p75;
        self.p90 = stats.p90;
        self.p95 = stats.p95;
        self.p99 = stats.p99;
        self.p99_99 = stats.p99_99;

        event
    }
}

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
    /// Channel to receive Kafka statistics objects from the stats callback
    stats_rx: crossbeam_channel::Receiver<Statistics>,
}

impl SourceReader<Vec<u8>> for KafkaSourceReader {
    /// Create a new instance of a Kafka reader.
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _: DataEncoding,
        logger: Option<Logger>,
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
    fn add_partition(&mut self, pid: PartitionId) {
        let pid = match pid {
            PartitionId::Kafka(p) => p,
            _ => unreachable!(),
        };

        self.create_partition_queue(pid);
        // Indicate a last offset of -1 if we have not been instructed to
        // have a specific start offset for this topic.
        let start_offset = *self.start_offsets.get(&pid).unwrap_or(&-1);
        let prev = self.last_offsets.insert(pid, start_offset);

        assert!(prev.is_none());
        self.known_partitions = cmp::max(self.known_partitions, pid + 1);
    }

    /// This function polls from the next consumer for which a message is available. This function polls the set
    /// round-robin: when a consumer is polled, it is placed at the back of the queue.
    ///
    /// If a message has an offset that is smaller than the next expected offset for this consumer (and this partition)
    /// we skip this message, and seek to the appropriate offset
    fn get_next_message(&mut self) -> Result<NextMessage<Vec<u8>>, anyhow::Error> {
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

        // Read any statistics objects generated via the GlueConsumerContext::stats callback
        while let Ok(statistics) = self.stats_rx.try_recv() {
            if let Some(logger) = self.logger.as_mut() {
                for mut part in self.partition_consumers.iter_mut() {
                    // If this is the first callback, initialize our consumer name
                    // so that we can later retract this when the source is dropped
                    match part.previous_stats.consumer_name {
                        None => part.previous_stats.consumer_name = Some(statistics.name.clone()),
                        _ => (),
                    }

                    for (broker, stats) in &statistics.brokers {
                        match &stats.rtt {
                            Some(rtt) => {
                                let window = part
                                    .broker_windows
                                    .entry(broker.into())
                                    .or_insert_with(BrokerRTTWindow::default);

                                logger.log(window.update(
                                    statistics.name.to_string(),
                                    self.id,
                                    broker.to_string(),
                                    rtt,
                                ));
                            }
                            None => (),
                        }
                    }

                    let topic_stats = match statistics.topics.get(self.topic_name.as_str()) {
                        Some(t) => t,
                        None => continue,
                    };

                    let partition_stats = match topic_stats.partitions.get(&part.pid) {
                        Some(p) => p,
                        None => continue,
                    };

                    logger.log(MaterializedEvent::KafkaConsumerPartition {
                        consumer_name: statistics.name.to_string(),
                        source_id: self.id,
                        partition_id: partition_stats.partition.to_string(),
                        rxmsgs: partition_stats.rxmsgs - part.previous_stats.rxmsgs,
                        rxbytes: partition_stats.rxbytes - part.previous_stats.rxbytes,
                        txmsgs: partition_stats.txmsgs - part.previous_stats.txmsgs,
                        txbytes: partition_stats.txbytes - part.previous_stats.txbytes,
                        lo_offset: partition_stats.lo_offset - part.previous_stats.lo_offset,
                        hi_offset: partition_stats.hi_offset - part.previous_stats.hi_offset,
                        ls_offset: partition_stats.ls_offset - part.previous_stats.ls_offset,
                        app_offset: partition_stats.app_offset - part.previous_stats.app_offset,
                        consumer_lag: partition_stats.consumer_lag
                            - part.previous_stats.consumer_lag,
                    });

                    part.previous_stats.rxmsgs = partition_stats.rxmsgs;
                    part.previous_stats.rxbytes = partition_stats.rxbytes;
                    part.previous_stats.txmsgs = partition_stats.txmsgs;
                    part.previous_stats.txbytes = partition_stats.txbytes;
                    part.previous_stats.lo_offset = partition_stats.lo_offset;
                    part.previous_stats.hi_offset = partition_stats.hi_offset;
                    part.previous_stats.ls_offset = partition_stats.ls_offset;
                    part.previous_stats.app_offset = partition_stats.app_offset;
                    part.previous_stats.consumer_lag = partition_stats.consumer_lag;
                }
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
                    warn!(
                        "Kafka message before expected offset: \
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
        }
    }

    /// Returns a count of total number of consumers for this source
    fn get_partition_consumers_count(&self) -> i32 {
        // Note: the number of consumers is guaranteed to always be smaller than
        // expected_partition_count (i32)
        self.partition_consumers.len().try_into().unwrap()
    }

    /// Creates a new partition queue for `partition_id`.
    fn create_partition_queue(&mut self, partition_id: i32) {
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
            .add_partition_offset(&self.topic_name, partition_id, Offset::Beginning)
            .expect("offset known to be valid");
        self.consumer
            .assign(&partition_list)
            .expect("assignment known to be valid");

        // Since librdkafka v1.6.0, we need to recreate all partition queues
        // after every call to `self.consumer.assign`.
        for pc in &mut self.partition_consumers {
            pc.partition_queue = self
                .consumer
                .split_partition_queue(&self.topic_name, pc.pid)
                .expect("partition known to be valid");
        }

        let partition_queue = self
            .consumer
            .split_partition_queue(&self.topic_name, partition_id)
            .expect("partition known to be valid");
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
}

impl Drop for KafkaSourceReader {
    fn drop(&mut self) {
        // Retract any metrics logged for this source
        if let Some(logger) = self.logger.as_mut() {
            for part in self.partition_consumers.iter_mut() {
                if let Some(consumer_name) = part.previous_stats.consumer_name.as_ref() {
                    logger.log(MaterializedEvent::KafkaConsumerPartition {
                        consumer_name: consumer_name.to_string(),
                        source_id: self.id,
                        partition_id: part.pid.to_string(),
                        rxmsgs: -part.previous_stats.rxmsgs,
                        rxbytes: -part.previous_stats.rxbytes,
                        txmsgs: -part.previous_stats.txmsgs,
                        txbytes: -part.previous_stats.txbytes,
                        lo_offset: -part.previous_stats.lo_offset,
                        hi_offset: -part.previous_stats.hi_offset,
                        ls_offset: -part.previous_stats.ls_offset,
                        app_offset: -part.previous_stats.app_offset,
                        consumer_lag: -part.previous_stats.consumer_lag,
                    });
                    for (broker, window) in part.broker_windows.iter() {
                        logger.log(window.negate(
                            consumer_name.to_string(),
                            self.id,
                            broker.to_string(),
                        ));
                    }
                }
            }
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

    // Automatically commit read offsets back to Kafka for monitoring purposes,
    // but on restart begin ingest at 0
    kafka_config
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");

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

    // Ensures that, when processing transactional topics, the consumer does not read data that
    // is not yet committed (and could later abort)
    kafka_config.set("isolation.level", "read_committed");

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

impl<'a> From<&BorrowedMessage<'a>> for SourceMessage<Vec<u8>> {
    fn from(msg: &BorrowedMessage<'a>) -> Self {
        let kafka_offset = KafkaOffset {
            offset: msg.offset(),
        };
        Self {
            payload: msg.payload().map(|p| p.to_vec()),
            partition: PartitionId::Kafka(msg.partition()),
            offset: kafka_offset.into(),
            upstream_time_millis: msg.timestamp().to_millis(),
            key: msg.key().map(|k| k.to_vec()),
        }
    }
}

/// Wrapper around a partition containing the underlying consumer
struct PartitionConsumer {
    /// the partition id with which this consumer is associated
    pid: i32,
    /// The underlying Kafka partition queue
    partition_queue: PartitionQueue<GlueConsumerContext>,
    /// Memoized Statistics for a partition consumer
    previous_stats: PreviousStats,
    /// Memoized Statistics for brokers
    broker_windows: HashMap<String, BrokerRTTWindow>,
}

impl PartitionConsumer {
    /// Creates a new partition consumer from underlying Kafka consumer
    fn new(pid: i32, partition_queue: PartitionQueue<GlueConsumerContext>) -> Self {
        PartitionConsumer {
            pid,
            partition_queue,
            previous_stats: PreviousStats::default(),
            broker_windows: HashMap::new(),
        }
    }

    /// Returns the next message to process for this partition (if any).
    fn get_next_message(&mut self) -> Result<Option<SourceMessage<Vec<u8>>>, KafkaError> {
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

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext {
    activator: SyncActivator,
    stats_tx: crossbeam_channel::Sender<Statistics>,
}

impl ClientContext for GlueConsumerContext {
    fn stats(&self, statistics: Statistics) {
        self.stats_tx
            .send(statistics)
            .expect("timely operator hung up while Kafka source active");
        self.activate();
    }
}

impl GlueConsumerContext {
    fn activate(&self) {
        self.activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        self.activate();
    }
}
