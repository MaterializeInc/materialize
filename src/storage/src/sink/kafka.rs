// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the sink dataflow of a [`KafkaSinkConnection`]. The dataflow consists
//! of two operators in order to take advantage of all the available workers.
//!
//! ```text
//!        ┏━━━━━━━━━━━━━━┓
//!        ┃   persist    ┃
//!        ┃    source    ┃
//!        ┗━━━━━━┯━━━━━━━┛
//!               │ row data, the input to this module
//!               │
//!        ┏━━━━━━v━━━━━━┓
//!        ┃    row      ┃
//!        ┃   encoder   ┃
//!        ┗━━━━━━┯━━━━━━┛
//!               │ encoded data
//!               │
//!        ┏━━━━━━v━━━━━━┓
//!        ┃    kafka    ┃ (single worker)
//!        ┃    sink     ┃
//!        ┗━━┯━━━━━━━━┯━┛
//!   records │        │ uppers
//!      ╭────v──╮ ╭───v──────╮
//!      │ data  │ │ progress │  <- records and uppers are produced
//!      │ topic │ │  topic   │     transactionally to both topics
//!      ╰───────╯ ╰──────────╯
//! ```
//!
//! # Encoding
//!
//! One part of the dataflow deals with encoding the rows that we read from persist. There isn't
//! anything surprizing here, it is *almost* just a `Collection::map` with the exception of an
//! initialization step that makes sure the schemas are published to the Schema Registry. After
//! that step the operator just encodes each batch it receives record by record.
//!
//! # Sinking
//!
//! The other part of the dataflow, and what this module mostly deals with, is interacting with the
//! Kafka cluster in order to transactionally commit batches (sets of records associated with a
//! frontier). All the processing happens in a single worker and so all previously encoded records
//! go through an exchange in order to arrive at the chosen worker. We may be able to improve this
//! in the future by committing disjoint partitions of the key space for independent workers but
//! for now we do the simple thing.
//!
//! ## Retries
//!
//! All of the retry logic heavy lifting is offloaded to `librdkafka` since it already implements
//! the required behavior[1]. In particular we only ever enqueue records to its send queue and
//! eventually call `commit_transaction` which will ensure that all queued messages are
//! successfully delivered before the transaction is reported as committed.
//!
//! The only error that is possible during sending is that the queue is full. We are purposefully
//! NOT handling this error and simply configure `librdkafka` with a very large queue. The reason
//! for this choice is that the only choice for hanlding such an error ourselves would be to queue
//! it, and there isn't a good argument about two small queues being better than one big one. If we
//! reach the queue limit we simply error out the entire sink dataflow and start over.
//!
//! # Error handling
//!
//! Both the encoding operator and the sinking operator can produce a transient error that is wired
//! up with our health monitoring and will trigger a restart of the sink dataflow.
//!
//! [1]: https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#message-reliability

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use differential_dataflow::{AsCollection, Collection, Hashable};
use mz_interchange::avro::{AvroEncoder, AvroSchemaGenerator, AvroSchemaOptions};
use mz_interchange::encode::Encode;
use mz_interchange::json::JsonEncoder;
use mz_kafka_util::client::{MzClientContext, TunnelingClientContext};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::task;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::sink::progress_key::ProgressKey;
use mz_storage_client::sink::{ProgressRecord, TopicCleanupPolicy, TopicConfig};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{ContextCreationError, ContextCreationErrorExt, DataflowError};
use mz_storage_types::sinks::{
    KafkaSinkConnection, KafkaSinkFormat, MetadataFilled, SinkEnvelope, StorageSinkDesc,
};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::metadata::Metadata;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Statistics;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{CapabilitySet, Concatenate, Map, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;
use tokio::sync::watch;
use tracing::{error, info};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::metrics::sink::kafka::KafkaSinkMetrics;
use crate::render::sinks::SinkRender;
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

impl<G: Scope<Timestamp = Timestamp>> SinkRender<G> for KafkaSinkConnection {
    fn uses_keys(&self) -> bool {
        true
    }

    fn get_key_indices(&self) -> Option<&[usize]> {
        self.key_desc_and_indices
            .as_ref()
            .map(|(_desc, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.relation_key_indices.as_deref()
    }

    fn render_continuous_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<MetadataFilled, Timestamp>,
        sink_id: GlobalId,
        input: Collection<G, (Option<Row>, Option<Row>), Diff>,
        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.
        _err_collection: Collection<G, DataflowError, Diff>,
    ) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>) {
        let mut scope = input.scope();

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        let (encoded, encode_status, encode_token) = encode_collection(
            format!("kafka-{sink_id}-{}-encode", self.format.get_format_name()),
            &input,
            sink.envelope,
            self.clone(),
            storage_state.storage_configuration.clone(),
        );

        let metrics = storage_state.metrics.get_kafka_sink_metrics(sink_id);
        let statistics = storage_state
            .aggregated_statistics
            .get_sink(&sink_id)
            .expect("statistics initialized")
            .clone();

        let (sink_status, sink_token) = sink_collection(
            format!("kafka-{sink_id}-sink"),
            &encoded,
            sink_id,
            self.clone(),
            storage_state.storage_configuration.clone(),
            sink.as_of.clone(),
            metrics,
            statistics,
            write_frontier,
        );

        let running_status = Some(HealthStatusMessage {
            index: 0,
            update: HealthStatusUpdate::Running,
            namespace: StatusNamespace::Kafka,
        })
        .to_stream(&mut scope);

        let status = scope.concatenate([running_status, encode_status, sink_status]);

        (status, vec![encode_token, sink_token])
    }
}

struct TransactionalProducer {
    /// The task name used for any blocking calls spawned onto the tokio threadpool.
    task_name: String,
    /// The topic where all the updates go.
    data_topic: String,
    /// The topic where all the upper frontiers go.
    progress_topic: String,
    /// The key each progress record is associated with.
    progress_key: ProgressKey,
    /// The underlying Kafka producer.
    producer: BaseProducer<TunnelingClientContext<MzClientContext>>,
    /// A handle to the metrics associated with this sink.
    statistics: SinkStatistics,
    /// The number of messages staged for the currently open transactions. It is reset to zero
    /// every time a transaction commits.
    staged_messages: u64,
    /// The total number bytes staged for the currently open transactions. It is reset to zero
    /// every time a transaction commits.
    staged_bytes: u64,
    /// The timeout to use for network operations.
    socket_timeout: Duration,
    /// The maximum duration of a transaction.
    transaction_timeout: Duration,
}

impl TransactionalProducer {
    /// Initializes a transcational producer for the sink identified by `sink_id`. After this call
    /// returns it is guranteed that all previous `TransactionalProducer` instances for the same
    /// sink have been fenced out (i.e `init_transations()` has been called successfully).
    async fn new(
        sink_id: GlobalId,
        connection: &KafkaSinkConnection,
        storage_configuration: &StorageConfiguration,
        metrics: KafkaSinkMetrics,
        statistics: SinkStatistics,
    ) -> Result<Self, ContextCreationError> {
        let client_id = connection.client_id(&storage_configuration.connection_context, sink_id);
        let transactional_id =
            connection.transactional_id(&storage_configuration.connection_context, sink_id);

        let timeout_config = &storage_configuration.parameters.kafka_timeout_config;
        let mut options = BTreeMap::new();
        // Ensure that messages are sinked in order and without duplicates. Note that this only
        // applies to a single instance of a producer - in the case of restarts, all bets are off
        // and full exactly once support is required.
        options.insert("enable.idempotence", "true".into());
        // Use the compression type requested by the user.
        options.insert(
            "compression.type",
            connection.compression_type.to_librdkafka_option().into(),
        );
        // Increase limits for the Kafka producer's internal buffering of messages. Currently we
        // don't have a great backpressure mechanism to tell indexes or views to slow down, so the
        // only thing we can do with a message that we can't immediately send is to put it in a
        // buffer and there's no point having buffers within the dataflow layer and Kafka. If the
        // sink starts falling behind and the buffers start consuming too much memory the best
        // thing to do is to drop the sink. Sets the buffer size to be 16 GB (note that this
        // setting is in KB)
        options.insert("queue.buffering.max.kbytes", format!("{}", 16 << 20));
        // Set the max messages buffered by the producer at any time to 10MM which is the maximum
        // allowed value.
        options.insert("queue.buffering.max.messages", format!("{}", 10_000_000));
        // Make the Kafka producer wait at least 10 ms before sending out MessageSets
        options.insert("queue.buffering.max.ms", format!("{}", 10));
        // Time out transactions after 60 seconds
        options.insert(
            "transaction.timeout.ms",
            format!("{}", timeout_config.transaction_timeout.as_millis()),
        );
        // Use the transactional ID requested by the user.
        options.insert("transactional.id", transactional_id);
        // Allow Kafka monitoring tools to identify this producer.
        options.insert("client.id", client_id);
        // We want to be notified regularly with statistics
        options.insert("statistics.interval.ms", "1000".into());

        let ctx = MzClientContext::default();

        let stats_receiver = ctx.subscribe_statistics();
        let task_name = format!("kafka_sink_metrics_collector:{sink_id}");
        task::spawn(|| &task_name, collect_statistics(stats_receiver, metrics));

        let producer: BaseProducer<_> = connection
            .connection
            .create_with_context(storage_configuration, ctx, &options)
            .await?;

        let task_name = format!("kafka_sink_producer:{sink_id}");
        let progress_key = ProgressKey::new(sink_id);

        let producer = Self {
            task_name,
            data_topic: connection.topic.clone(),
            progress_topic: connection
                .progress_topic(&storage_configuration.connection_context)
                .into_owned(),
            progress_key,
            producer,
            statistics,
            staged_messages: 0,
            staged_bytes: 0,
            socket_timeout: timeout_config.socket_timeout,
            transaction_timeout: timeout_config.transaction_timeout,
        };

        let timeout = timeout_config.socket_timeout;
        producer
            .spawn_blocking(move |p| p.init_transactions(timeout))
            .await?;

        Ok(producer)
    }

    /// Runs the blocking operation `f` on the producer in the tokio threadpool and checks for SSH
    /// status in case of failure.
    async fn spawn_blocking<F, R>(&self, f: F) -> Result<R, ContextCreationError>
    where
        F: FnOnce(BaseProducer<TunnelingClientContext<MzClientContext>>) -> Result<R, KafkaError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let producer = self.producer.clone();
        task::spawn_blocking(|| &self.task_name, move || f(producer))
            .await
            .unwrap()
            .check_ssh_status(self.producer.context())
    }

    async fn fetch_metadata(&self) -> Result<Metadata, ContextCreationError> {
        self.spawn_blocking(|p| p.client().fetch_metadata(None, Duration::from_secs(10)))
            .await
    }

    async fn begin_transaction(&mut self) -> Result<(), ContextCreationError> {
        self.spawn_blocking(|p| p.begin_transaction()).await
    }

    /// Synchronously puts the provided message to librdkafka's send queue. This method only
    /// returns an error if the queue is full. Handling this error by buffering the message and
    /// retrying is equivalent to adjusting the maximum number of queued items in rdkafka so it is
    /// adviced that callers only handle this error in order to apply backpressure to the rest of
    /// the system.
    async fn send(
        &mut self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        time: Timestamp,
        diff: Diff,
    ) -> Result<(), ContextCreationError> {
        assert_eq!(diff, 1, "invalid sink update");

        let headers = OwnedHeaders::new().insert(Header {
            key: "materialize-timestamp",
            value: Some(time.to_string().as_bytes()),
        });
        let record = BaseRecord {
            topic: &self.data_topic,
            key,
            payload: value,
            headers: Some(headers),
            partition: None,
            timestamp: None,
            delivery_opaque: (),
        };
        let key_size = key.map(|k| k.len()).unwrap_or(0);
        let value_size = value.map(|k| k.len()).unwrap_or(0);
        let record_size = u64::cast_from(key_size + value_size);
        self.statistics.inc_messages_staged_by(1);
        self.staged_messages += 1;
        self.statistics.inc_bytes_staged_by(record_size);
        self.staged_bytes += record_size;
        match self.producer.send(record) {
            Ok(()) => Ok(()),
            Err((err, record)) => match err.rdkafka_error_code() {
                Some(RDKafkaErrorCode::QueueFull) => {
                    // If the internal rdkafka queue is full we have no other option than to flush
                    // TODO(petrosagg): remove this logic once we fix upgrade to librdkafka 2.3 and
                    // increase the queue limits
                    let timeout = self.transaction_timeout;
                    self.spawn_blocking(move |p| p.flush(timeout)).await?;
                    self.producer.send(record).map_err(|(err, _)| err.into())
                }
                _ => Err(err.into()),
            },
        }
    }

    /// Commits all the staged updates of the currently open transaction plus a progress record
    /// describing `upper` to the progress topic.
    async fn commit_transaction(
        &mut self,
        upper: Antichain<Timestamp>,
    ) -> Result<(), ContextCreationError> {
        let progress = ProgressRecord {
            frontier: upper.into(),
        };
        let payload = serde_json::to_vec(&progress).expect("infallible");
        let record = BaseRecord::to(&self.progress_topic)
            .payload(&payload)
            .key(&self.progress_key);
        match self.producer.send(record) {
            Ok(()) => {}
            Err((err, record)) => match err.rdkafka_error_code() {
                Some(RDKafkaErrorCode::QueueFull) => {
                    // If the internal rdkafka queue is full we have no other option than to flush
                    // TODO(petrosagg): remove this logic once we fix the issue that cannot be
                    // named
                    let timeout = self.transaction_timeout;
                    self.spawn_blocking(move |p| p.flush(timeout)).await?;
                    self.producer.send(record).map_err(|(err, _)| err)?;
                }
                _ => return Err(err.into()),
            },
        }

        let timeout = self.socket_timeout;
        match self
            .spawn_blocking(move |p| p.commit_transaction(timeout))
            .await
        {
            Ok(()) => {
                self.statistics
                    .inc_messages_committed_by(self.staged_messages);
                self.statistics.inc_bytes_committed_by(self.staged_bytes);
                self.staged_messages = 0;
                self.staged_bytes = 0;
                Ok(())
            }
            Err(ContextCreationError::KafkaError(KafkaError::Transaction(err))) => {
                // Make one attempt at aborting the transaction before letting the error percolate
                // up and the process exit. Aborting allows the consumers of the topic to skip over
                // any messages we've written in the transaction, so it's polite to do... but if it
                // fails, the transaction will be aborted either when fenced out by a future
                // version of this producer or by the broker-side timeout.
                if err.txn_requires_abort() {
                    let timeout = self.socket_timeout;
                    self.spawn_blocking(move |p| p.abort_transaction(timeout))
                        .await?;
                }
                Err(ContextCreationError::KafkaError(KafkaError::Transaction(
                    err,
                )))
            }
            Err(err) => Err(err),
        }
    }
}

/// Listens for statistics updates from librdkafka and updates our Prometheus metrics.
async fn collect_statistics(mut receiver: watch::Receiver<Statistics>, metrics: KafkaSinkMetrics) {
    let mut outbuf_cnt: i64 = 0;
    let mut outbuf_msg_cnt: i64 = 0;
    let mut waitresp_cnt: i64 = 0;
    let mut waitresp_msg_cnt: i64 = 0;
    let mut txerrs: u64 = 0;
    let mut txretries: u64 = 0;
    let mut req_timeouts: u64 = 0;
    let mut connects: i64 = 0;
    let mut disconnects: i64 = 0;
    while receiver.changed().await.is_ok() {
        let stats = receiver.borrow();
        for broker in stats.brokers.values() {
            outbuf_cnt += broker.outbuf_cnt;
            outbuf_msg_cnt += broker.outbuf_msg_cnt;
            waitresp_cnt += broker.waitresp_cnt;
            waitresp_msg_cnt += broker.waitresp_msg_cnt;
            txerrs += broker.txerrs;
            txretries += broker.txretries;
            req_timeouts += broker.req_timeouts;
            connects += broker.connects.unwrap_or(0);
            disconnects += broker.disconnects.unwrap_or(0);
        }
        metrics.rdkafka_msg_cnt.set(stats.msg_cnt);
        metrics.rdkafka_msg_size.set(stats.msg_size);
        metrics.rdkafka_txmsgs.set(stats.txmsgs);
        metrics.rdkafka_txmsg_bytes.set(stats.txmsg_bytes);
        metrics.rdkafka_tx.set(stats.tx);
        metrics.rdkafka_tx_bytes.set(stats.tx_bytes);
        metrics.rdkafka_outbuf_cnt.set(outbuf_cnt);
        metrics.rdkafka_outbuf_msg_cnt.set(outbuf_msg_cnt);
        metrics.rdkafka_waitresp_cnt.set(waitresp_cnt);
        metrics.rdkafka_waitresp_msg_cnt.set(waitresp_msg_cnt);
        metrics.rdkafka_txerrs.set(txerrs);
        metrics.rdkafka_txretries.set(txretries);
        metrics.rdkafka_req_timeouts.set(req_timeouts);
        metrics.rdkafka_connects.set(connects);
        metrics.rdkafka_disconnects.set(disconnects);
    }
}

/// Sinks a collection of encoded rows to Kafka.
///
/// This operator exchanges all updates to a single worker by hashing on the given sink `id`.
///
/// Updates are sent in ascending timestamp order.
fn sink_collection<G: Scope<Timestamp = Timestamp>>(
    name: String,
    input: &Collection<G, (Option<Vec<u8>>, Option<Vec<u8>>), Diff>,
    sink_id: GlobalId,
    connection: KafkaSinkConnection,
    storage_configuration: StorageConfiguration,
    as_of: Antichain<Timestamp>,
    metrics: KafkaSinkMetrics,
    statistics: SinkStatistics,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> (Stream<G, HealthStatusMessage>, PressOnDropButton) {
    let scope = input.scope();
    let mut builder = AsyncOperatorBuilder::new(name.clone(), input.inner.scope());

    // We want exactly one worker to send all the data to the sink topic.
    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    let mut input = builder.new_disconnected_input(&input.inner, Exchange::new(move |_| hashed_id));

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                write_frontier.borrow_mut().clear();
                return Ok(());
            }

            fail::fail_point!("kafka_sink_creation_error", |_| Err(
                ContextCreationError::Other(anyhow::anyhow!("synthetic error"))
            ));

            let mut producer = TransactionalProducer::new(
                sink_id,
                &connection,
                &storage_configuration,
                metrics,
                statistics,
            )
            .await?;
            // Instantiating the transactional producer fences out all previous ones, making it
            // safe to determine the resume upper.
            let resume_upper = mz_storage_client::sink::determine_sink_resume_upper(
                sink_id,
                &connection,
                &storage_configuration,
            )
            .await?;

            let resume_upper = match resume_upper {
                Some(upper) => {
                    // If there are committed progress messages then we only check if the data
                    // topic exists, because if it does not then it must have been deleted after
                    // the fact, which is a bit of a problem.
                    let meta = producer.fetch_metadata().await?;
                    if !meta.topics().iter().any(|t| t.name() == &connection.topic) {
                        return Err(anyhow!(
                            "sink progress data exists, but sink data topic is missing"
                        )
                        .into());
                    }
                    upper
                }
                None => {
                    mz_storage_client::sink::ensure_kafka_topic(
                        &connection,
                        &storage_configuration,
                        &connection.topic,
                        // TODO: allow users to configure these parameters.
                        TopicConfig {
                            partition_count: -1,
                            replication_factor: -1,
                            cleanup_policy: TopicCleanupPolicy::Retention {
                                ms: Some(-1),
                                bytes: Some(-1),
                            },
                        },
                    )
                    .await?;
                    Antichain::from_elem(Timestamp::minimum())
                }
            };

            // The input has overcompacted if
            let overcompacted =
                // ..we have made some progress in the past
                *resume_upper != [Timestamp::minimum()] &&
                // ..but the since frontier is now beyond that
                !PartialOrder::less_equal(&as_of, &resume_upper);
            if overcompacted {
                let err = format!(
                    "{name}: input compacted past resume upper: as_of {}, resume_upper: {}",
                    as_of.pretty(),
                    resume_upper.pretty()
                );
                // This would normally be an assertion but because it can happen after a
                // Materialize backup/restore we log an error so that it appears on Sentry but
                // leaves the rest of the objects in the cluster unaffected.
                error!("{err}");
                return Err(anyhow!("{err}").into());
            }

            info!(
                "{name}: as_of: {}, resume upper: {}",
                as_of.pretty(),
                resume_upper.pretty()
            );

            // The section below relies on TotalOrder for correctness so we'll work with timestamps
            // directly to make sure this doesn't compile if someone attempts to make this operator
            // generic over partial orders in the future.
            let Some(mut upper) = resume_upper.clone().into_option() else {
                return Ok(());
            };
            let mut deferred_updates = vec![];
            let mut extra_updates = vec![];
            // We must wait until we have data to commit before starting a transaction because
            // Kafka doesn't have a heartbeating mechanism to keep a transaction open indefinitely.
            // This flag tracks whether we have started the transaction.
            let mut transaction_begun = false;
            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_cap, batch) => {
                        for ((key, value), time, diff) in batch {
                            // We want to publish updates in time order and we know that we have
                            // already committed all times not beyond `upper`. Therefore, if this
                            // update happens *exactly* at upper then it is the minimum pending
                            // time and so emitting it now will not violate the timestamp publish
                            // order. This optimization is load bearing because it is the mechanism
                            // by which we incrementally stream the initial snapshot out to Kafka
                            // instead of buffering it all in memory first. This argument doesn't
                            // hold for partially ordered time because many different timestamps
                            // can be *exactly* at upper but we can't know ahead of time which one
                            // will be advanced in the next progress message.
                            match upper.cmp(&time) {
                                Ordering::Less => deferred_updates.push(((key, value), time, diff)),
                                Ordering::Equal => {
                                    if !transaction_begun {
                                        producer.begin_transaction().await?;
                                        transaction_begun = true;
                                    }
                                    producer
                                        .send(key.as_deref(), value.as_deref(), time, diff)
                                        .await?;
                                }
                                Ordering::Greater => continue,
                            }
                        }
                    }
                    Event::Progress(progress) => {
                        // Ignore progress updates before our resumption frontier
                        if !PartialOrder::less_equal(&resume_upper, &progress) {
                            continue;
                        }
                        // Also ignore progress updates until we are past the as_of frontier. This
                        // is to avoid the following pathological scenario:
                        // 1. Sink gets instantiated with an as_of = {10}, resume_upper = {0}.
                        //    `progress` initially jumps at {10}, then the snapshot appears at time
                        //    10.
                        // 2. `progress` would normally advance to say {11} and we would commit the
                        //    snapshot but clusterd crashes instead.
                        // 3. A new cluster restarts the sink with an earlier as_of, say {5}. This
                        //    is valid, the earlier as_of has strictly more information. The
                        //    snapshot now appears at time 5.
                        //
                        // If we were to commit an empty transaction in step 1 and advanced the
                        // resume_upper to {10} then in step 3 we would ignore the snapshot that
                        // now appears at 5 completely. So it is important to only start committing
                        // transactions after we're strictly beyond the as_of.
                        // TODO(petrosagg): is this logic an indication of us holding something
                        // wrong elsewhere? Investigate.
                        // Note: !PartialOrder::less_than(as_of, progress) would not be equivalent
                        // nor correct for partially ordered times.
                        if !as_of.iter().all(|t| !progress.less_equal(t)) {
                            continue;
                        }
                        if !transaction_begun {
                            producer.begin_transaction().await?;
                        }
                        extra_updates.extend(
                            deferred_updates
                                .drain_filter_swapping(|(_, time, _)| !progress.less_equal(time)),
                        );
                        extra_updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                        for ((key, value), time, diff) in extra_updates.drain(..) {
                            producer
                                .send(key.as_deref(), value.as_deref(), time, diff)
                                .await?;
                        }

                        info!("{name}: committing transaction for {}", progress.pretty());
                        producer.commit_transaction(progress.clone()).await?;
                        transaction_begun = false;
                        *write_frontier.borrow_mut() = progress.clone();
                        match progress.into_option() {
                            Some(new_upper) => upper = new_upper,
                            None => break,
                        }
                    }
                }
            }
            Ok(())
        })
    });

    let statuses = errors.map(|error: Rc<ContextCreationError>| {
        let hint = match *error {
            ContextCreationError::KafkaError(KafkaError::Transaction(ref e)) => {
                if e.is_retriable() && e.code() == RDKafkaErrorCode::OperationTimedOut {
                    let hint = "If you're running a single Kafka broker, ensure that the configs \
                        transaction.state.log.replication.factor, transaction.state.log.min.isr, \
                        and offsets.topic.replication.factor are set to 1 on the broker";
                    Some(hint.to_owned())
                } else {
                    None
                }
            }
            _ => None,
        };

        HealthStatusMessage {
            index: 0,
            update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), hint),
            namespace: if matches!(*error, ContextCreationError::Ssh(_)) {
                StatusNamespace::Ssh
            } else {
                StatusNamespace::Kafka
            },
        }
    });

    (statuses, button.press_on_drop())
}

/// Encodes a stream of `(Option<Row>, Option<Row>)` updates using the specified encoder.
///
/// Input [`Row`] updates must me compatible with the given implementor of [`Encode`].
fn encode_collection<G: Scope>(
    name: String,
    input: &Collection<G, (Option<Row>, Option<Row>), Diff>,
    envelope: SinkEnvelope,
    connection: KafkaSinkConnection,
    storage_configuration: StorageConfiguration,
) -> (
    Collection<G, (Option<Vec<u8>>, Option<Vec<u8>>), Diff>,
    Stream<G, HealthStatusMessage>,
    PressOnDropButton,
) {
    let mut builder = AsyncOperatorBuilder::new(name, input.inner.scope());

    let (mut output, stream) = builder.new_output();
    let mut input = builder.new_input_for(&input.inner, Pipeline, &output);

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();
            let key_desc = connection
                .key_desc_and_indices
                .as_ref()
                .map(|(desc, _indices)| desc.clone());
            let value_desc = connection.value_desc;

            let encoder: Box<dyn Encode> = match connection.format {
                KafkaSinkFormat::Avro {
                    key_schema,
                    value_schema,
                    csr_connection,
                } => {
                    // Ensure that schemas are registered with the schema registry.
                    //
                    // Note that where this lies in the rendering cycle means that we will publish the
                    // schemas each time the sink is rendered.
                    let ccsr = csr_connection.connect(&storage_configuration).await?;
                    let (key_schema_id, value_schema_id) =
                        mz_storage_client::sink::publish_kafka_schemas(
                            &ccsr,
                            &connection.topic,
                            key_schema.as_deref(),
                            Some(mz_ccsr::SchemaType::Avro),
                            &value_schema,
                            mz_ccsr::SchemaType::Avro,
                        )
                        .await
                        .context("error publishing kafka schemas for sink")?;

                    let options = AvroSchemaOptions {
                        is_debezium: matches!(envelope, SinkEnvelope::Debezium),
                        ..Default::default()
                    };

                    let schema_generator = AvroSchemaGenerator::new(key_desc, value_desc, options)
                        .expect("avro schema validated");
                    Box::new(AvroEncoder::new(
                        schema_generator,
                        key_schema_id,
                        value_schema_id,
                    ))
                }
                KafkaSinkFormat::Json => Box::new(JsonEncoder::new(
                    key_desc,
                    value_desc,
                    matches!(envelope, SinkEnvelope::Debezium),
                )),
            };

            // !IMPORTANT!
            // Correctness of this operator relies on no fallible operations happening after this
            // point. This is a temporary workaround of build_fallible's bad interaction of owned
            // capabilities and errors.
            // TODO(petrosagg): Make the fallible async operator safe
            *capset = CapabilitySet::new();

            while let Some(event) = input.next().await {
                if let Event::Data(cap, rows) = event {
                    for ((key, value), time, diff) in rows {
                        let key = key.map(|key| encoder.encode_key_unchecked(key));
                        let value = value.map(|value| encoder.encode_value_unchecked(value));
                        output.give(&cap, ((key, value), time, diff)).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        })
    });

    let statuses = errors.map(|error| HealthStatusMessage {
        index: 0,
        update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), None),
        namespace: StatusNamespace::Kafka,
    });

    (stream.as_collection(), statuses, button.press_on_drop())
}
