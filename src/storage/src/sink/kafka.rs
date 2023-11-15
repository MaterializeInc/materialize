// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::{StreamExt, TryFutureExt};
use maplit::btreemap;
use mz_interchange::avro::{AvroEncoder, AvroSchemaGenerator, AvroSchemaOptions};
use mz_interchange::encode::Encode;
use mz_interchange::json::JsonEncoder;
use mz_kafka_util::client::{MzClientContext, TunnelingClientContext};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::task;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_ssh_util::tunnel::SshTunnelStatus;
use mz_storage_client::client::SinkStatisticsUpdate;
use mz_storage_client::sink::{ProgressRecord, PurifiedKafkaSinkConnection};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::{ContextCreationError, DataflowError};
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkAvroFormatState, KafkaSinkConnection, KafkaSinkFormat,
    MetadataFilled, SinkEnvelope, StorageSinkDesc,
};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    AsyncInputHandle, Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use prometheus::core::AtomicU64;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders, OwnedMessage, ToBytes};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use timely::communication::Pull;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::{Broadcast, Concat, Map, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as _};
use timely::{Data, PartialOrder};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::SinkRender;
use crate::sink::KafkaBaseMetrics;
use crate::statistics::{SinkStatisticsMetrics, StorageStatistics};
use crate::storage_state::StorageState;

// 30s is a good maximum backoff for network operations. Long enough to reduce
// load on an upstream system, but short enough that we can respond quickly when
// the upstream system comes back online.
const BACKOFF_CLAMP: Duration = Duration::from_secs(30);

impl<G> SinkRender<G> for KafkaSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
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
    ) -> (Stream<G, HealthStatusMessage>, Option<Rc<dyn Any>>)
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let mut scope = input.scope();

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        // This operator will be responsible for the async initialization of external state before
        // all the other operators can do their work.
        // TODO(petrosagg): move this work to the Controller
        let (purify_stream, purify_errors, purify_token) = purify_connection(
            format!("kafka-{sink_id}-purify-connection"),
            input.scope(),
            &storage_state.connection_context,
            sink_id,
            self.clone(),
        );

        let (encoded_input, encode_token) = encode_collection(
            format!("kafka-{sink_id}-{}-encode", self.format.get_format_name()),
            &input,
            &purify_stream,
            sink.envelope,
        );

        let shared_kafka_context = Rc::new(RefCell::new(None));

        let (produce_errors, produce_token) = produce_to_kafka(
            format!("kafka-{sink_id}"),
            &encoded_input,
            &purify_stream,
            sink_id,
            sink.as_of.clone(),
            sink.with_snapshot,
            storage_state.sink_metrics.kafka.clone(),
            storage_state
                .sink_statistics
                .get(&sink_id)
                .expect("statistics initialized")
                .clone(),
            storage_state.connection_context.clone(),
            Rc::clone(&shared_kafka_context),
            write_frontier,
        );

        let errors = produce_errors.concat(&purify_errors).flat_map(move |error| {
            let ssh_status = shared_kafka_context.borrow().as_ref().map(|context| context.tunnel_status());

            let hint: Option<String> = if let ContextCreationError::Other(error) = &*error {
                match error.downcast_ref::<RDKafkaError>() {
                    Some(kafka_error) => {
                        let is_timeout = matches!(kafka_error.code(), RDKafkaErrorCode::OperationTimedOut);
                        if kafka_error.is_retriable() && is_timeout {
                            let msg = "If you're running a single Kafka broker, ensure that the \
                                configs transaction.state.log.replication.factor, \
                                transaction.state.log.min.isr, and offsets.topic.replication.factor \
                                are set to 1 on the broker";
                            Some(msg.to_string())
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            } else {
                None
            };

            let mut statuses = vec![];

            if let Some(SshTunnelStatus::Errored(e)) = ssh_status {
                statuses.push(HealthStatusMessage {
                    index: 0,
                    update: HealthStatusUpdate::stalled(e, None),
                    namespace: StatusNamespace::Ssh,
                });
            }

            statuses.push(HealthStatusMessage {
                index: 0,
                update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), hint),
                namespace: if matches!(*error, ContextCreationError::Ssh(_)) {
                    StatusNamespace::Ssh
                } else {
                    StatusNamespace::Kafka
                },
            });

            statuses
        });

        let running_update = Some(HealthStatusMessage {
            index: 0,
            update: HealthStatusUpdate::Running,
            namespace: StatusNamespace::Kafka,
        })
        .to_stream(&mut scope);
        let health = errors.concat(&running_update);
        (
            health,
            Some(Rc::new((purify_token, encode_token, produce_token))),
        )
    }
}

fn purify_connection<G>(
    name: String,
    scope: G,
    connection_context: &ConnectionContext,
    sink_id: GlobalId,
    connection: KafkaSinkConnection,
) -> (
    Stream<G, PurifiedKafkaSinkConnection>,
    Stream<G, Rc<ContextCreationError>>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let active_write_worker = (usize::cast_from(sink_id.hashed()) % scope.peers()) == scope.index();
    let mut purify_op = AsyncOperatorBuilder::new(name, scope);
    let (mut output, stream) = purify_op.new_output();
    let connection_context = connection_context.clone();
    let (button, errors) = purify_op.build_fallible(move |caps| {
        Box::pin(async move {
            // We only want to do this work on one worker and let the other know of the result
            if !active_write_worker {
                return Ok(());
            }
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();
            let purified_connection = mz_storage_client::sink::purify_kafka_sink(
                sink_id,
                connection,
                &connection_context,
            )
            .await?;
            output.give(&capset[0], purified_connection).await;
            Ok(())
        })
    });
    // Broadcast the purified information to all the workers
    (stream.broadcast(), errors, button.press_on_drop())
}

/// Per-Kafka sink metrics.
pub struct SinkMetrics {
    messages_sent_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    message_send_errors_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    message_delivery_errors_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    rows_queued: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SinkMetrics {
    fn new(
        base: &KafkaBaseMetrics,
        topic_name: &str,
        sink_id: &str,
        worker_id: &str,
    ) -> SinkMetrics {
        let labels = vec![
            topic_name.to_string(),
            sink_id.to_string(),
            worker_id.to_string(),
        ];
        SinkMetrics {
            messages_sent_counter: base
                .messages_sent_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_send_errors_counter: base
                .message_send_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_delivery_errors_counter: base
                .message_delivery_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            rows_queued: base.rows_queued.get_delete_on_drop_gauge(labels),
        }
    }
}

#[derive(Clone)]
pub struct KafkaSinkSendRetryManager {
    // Because we flush this fully before moving onto the next timestamp, this queue cannot contain
    // more messages than are contained in a single timely timestamp.  It's also unlikely to get
    // beyond the size of the rdkafka internal message queue because that seems to stop accepting
    // new messages when errors are being returned by the delivery callback -- though we should not
    // rely on that fact if/when we eventually (tm) get around to making our queues bounded.
    //
    // If perf becomes an issue, we can do something slightly more complex with a crossbeam channel
    q: VecDeque<OwnedMessage>,
    outstanding_send_count: u64,
}

impl KafkaSinkSendRetryManager {
    fn new() -> Self {
        Self {
            q: VecDeque::new(),
            outstanding_send_count: 0,
        }
    }
    fn record_send(&mut self) {
        self.outstanding_send_count += 1;
    }
    fn record_error(&mut self, msg: OwnedMessage) {
        self.q.push_back(msg);
        self.outstanding_send_count -= 1;
    }
    fn record_success(&mut self) {
        self.outstanding_send_count -= 1;
    }
    fn sends_flushed(&mut self) -> bool {
        self.outstanding_send_count == 0 && self.q.is_empty()
    }
    fn pop_retry(&mut self) -> Option<OwnedMessage> {
        self.q.pop_front()
    }
}

#[derive(Clone)]
pub struct SinkProducerContext {
    metrics: Arc<SinkMetrics>,
    retry_manager: Arc<Mutex<KafkaSinkSendRetryManager>>,
    inner: MzClientContext,
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

#[derive(Clone)]
struct KafkaTxProducer {
    name: String,
    inner: Arc<ThreadedProducer<TunnelingClientContext<SinkProducerContext>>>,
    timeout: Duration,
}

impl KafkaTxProducer {
    fn init_transactions(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("init_transactions:{}", self.name),
            move || self_producer.init_transactions(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn begin_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        task::spawn_blocking(
            || format!("begin_transaction:{}", self.name),
            move || self_producer.begin_transaction(),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn commit_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("commit_transaction:{}", self.name),
            move || self_producer.commit_transaction(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn abort_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("abort_transaction:{}", self.name),
            move || self_producer.abort_transaction(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn flush(&self) -> impl Future<Output = KafkaResult<()>> {
        let self_producer = Arc::clone(&self.inner);
        let self_timeout = self.timeout;
        task::spawn_blocking(
            || format!("flush:{}", self.name),
            move || self_producer.flush(self_timeout),
        )
        .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn send<'a, K, P>(
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

    async fn retry_on_txn_error<'a, F, Fut, T>(&self, f: F) -> Result<T, anyhow::Error>
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

struct KafkaSinkState {
    name: String,
    topic: String,
    metrics: Arc<SinkMetrics>,
    producer: KafkaTxProducer,
    retry_manager: Arc<Mutex<KafkaSinkSendRetryManager>>,

    progress_topic: String,
    progress_key: String,
}

impl KafkaSinkState {
    // Until `try` blocks, we need this for using `fail_point!` correctly.
    async fn new(
        sink_id: GlobalId,
        connection: KafkaSinkConnection,
        sink_name: String,
        worker_id: String,
        metrics: &KafkaBaseMetrics,
        connection_context: &ConnectionContext,
    ) -> Result<Self, ContextCreationError> {
        let metrics = Arc::new(SinkMetrics::new(
            metrics,
            &connection.topic,
            &sink_id.to_string(),
            &worker_id,
        ));

        let retry_manager = Arc::new(Mutex::new(KafkaSinkSendRetryManager::new()));

        let producer_context = SinkProducerContext {
            metrics: Arc::clone(&metrics),
            retry_manager: Arc::clone(&retry_manager),
            inner: MzClientContext::default(),
        };

        #[allow(clippy::redundant_closure_call)]
        let producer = (|| async {
            fail::fail_point!("kafka_sink_creation_error", |_| Err(
                ContextCreationError::Other(anyhow::anyhow!("synthetic error"))
            ));

            connection
                .connection
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
                    },
                )
                .await
        })()
        .await?;

        let producer = KafkaTxProducer {
            name: sink_name.clone(),
            inner: Arc::new(producer),
            timeout: Duration::from_secs(5),
        };

        Ok(KafkaSinkState {
            name: sink_name,
            topic: connection.topic,
            metrics,
            producer,
            retry_manager,
            progress_topic: match connection.consistency_config {
                KafkaConsistencyConfig::Progress { topic } => topic,
            },
            progress_key: mz_storage_client::sink::ProgressKey::new(sink_id),
        })
    }

    async fn send<'a, K, P>(&self, mut record: BaseRecord<'a, K, P>) -> Result<(), anyhow::Error>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let tries = Retry::default()
            .max_tries(usize::MAX)
            .clamp_backoff(Duration::from_secs(60 * 10))
            .into_retry_stream();
        tokio::pin!(tries);
        loop {
            tries.next().await.expect("infinite stream");
            match self.producer.send(record) {
                Ok(()) => {
                    self.metrics.messages_sent_counter.inc();
                    self.retry_manager.lock().await.record_send();
                    return Ok(());
                }
                Err((e, rec)) => {
                    record = *rec;
                    self.metrics.message_send_errors_counter.inc();

                    if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = e {
                        debug!(
                            "unable to produce message in {}: rdkafka queue full; will retry",
                            self.name
                        );
                        continue;
                    } else {
                        // We've received an error that is not transient
                        bail!(format!(
                            "fatal error while producing message in {}: {e}",
                            self.name
                        ));
                    }
                }
            }
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        self.flush_inner().await;
        while !{
            let mut guard = self.retry_manager.lock().await;
            guard.sends_flushed()
        } {
            while let Some(msg) = {
                let mut guard = self.retry_manager.lock().await;
                guard.pop_retry()
            } {
                let mut transformed_msg = BaseRecord::to(msg.topic());
                transformed_msg = match msg.key() {
                    Some(k) => transformed_msg.key(k),
                    None => transformed_msg,
                };
                transformed_msg = match msg.payload() {
                    Some(p) => transformed_msg.payload(p),
                    None => transformed_msg,
                };
                self.send(transformed_msg).await?;
            }
            self.flush_inner().await;
        }
        Ok(())
    }

    async fn flush_inner(&self) {
        Retry::default()
            .max_tries(usize::MAX)
            .clamp_backoff(BACKOFF_CLAMP)
            .retry_async(|_| self.producer.flush())
            .await
            .expect("Infinite retry cannot fail");
    }

    async fn send_progress_record(&self, upper: Antichain<Timestamp>) -> Result<(), anyhow::Error> {
        let encoded = serde_json::to_vec(&ProgressRecord {
            frontier: upper.into(),
        })
        .expect("serialization to vec cannot fail");
        let record = BaseRecord::to(&self.progress_topic)
            .payload(&encoded)
            .key(&self.progress_key);
        self.send(record).await?;
        Ok(())
    }
}

/// Produces/sends a stream of encoded rows (as `Vec<u8>`) to Kafka.
///
/// This operator exchanges all updates to a single worker by hashing on the given sink `id`.
///
/// Updates are only sent to Kafka once the input frontier has passed their `time`. Updates are
/// sent in ascending timestamp order.
fn produce_to_kafka<G>(
    name: String,
    input: &Collection<G, (Option<Vec<u8>>, Option<Vec<u8>>), Diff>,
    purify_stream: &Stream<G, PurifiedKafkaSinkConnection>,
    sink_id: GlobalId,
    as_of: Antichain<Timestamp>,
    with_snapshot: bool,
    metrics: KafkaBaseMetrics,
    sink_statistics: StorageStatistics<SinkStatisticsUpdate, SinkStatisticsMetrics>,
    connection_context: ConnectionContext,
    shared_context: Rc<RefCell<Option<Arc<TunnelingClientContext<SinkProducerContext>>>>>,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> (Stream<G, Rc<ContextCreationError>>, PressOnDropButton)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = input.scope();
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new(name.clone(), input.inner.scope());

    // We want exactly one worker to send all the data to the sink topic.
    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    let mut input = builder.new_input(&input.inner, Exchange::new(move |_| hashed_id));
    let purify_input = builder.new_input(purify_stream, Pipeline);

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                write_frontier.borrow_mut().clear();
                return Ok(());
            }

            let PurifiedKafkaSinkConnection {
                connection,
                resume_upper,
            } = recv_one(purify_input).await;
            assert!(
                *resume_upper == [Timestamp::minimum()]
                    || PartialOrder::less_equal(&as_of, &resume_upper),
                "{}: input compacted past resume upper: as_of {}, resume_upper: {}",
                name,
                as_of.pretty(),
                resume_upper.pretty(),
            );

            let s = KafkaSinkState::new(
                sink_id,
                connection,
                name,
                worker_id.to_string(),
                &metrics,
                &connection_context,
            )
            .await?;
            *shared_context.borrow_mut() = Some(Arc::clone(s.producer.inner.context()));

            s.producer
                .retry_on_txn_error(|p| p.init_transactions())
                .await?;

            info!(
                "{}: as_of: {}, resume upper: {}",
                s.name,
                as_of.pretty(),
                resume_upper.pretty()
            );

            let mut lower = resume_upper.clone();
            let mut pending_batches = vec![];
            while let Some(event) = input.next_mut().await {
                match event {
                    Event::Data(_cap, batch) => {
                        // Retain updates that are..
                        batch.retain(|(_, time, diff)| {
                            // ..not a no-op record
                            *diff != 0
                            // ..beyond the lower frontier
                            && lower.less_equal(time)
                            // ..strictly beyond the as of if a snapshot was requested
                            && (with_snapshot || as_of.less_than(time))
                        });
                        s.metrics.rows_queued.add(u64::cast_from(batch.len()));
                        pending_batches.push(std::mem::take(batch));
                    }
                    Event::Progress(upper) => {
                        // There is nothing to do until the frontier goes beyond our resume upper
                        // and we definitely do not want to commit progress records with regressed
                        // frontiers.
                        if !PartialOrder::less_equal(&resume_upper, &upper) {
                            continue;
                        }

                        let mut ready_updates = pending_batches
                            .iter_mut()
                            .flat_map(|batch| {
                                batch.drain_filter_swapping(|(_, t, _)| !upper.less_equal(t))
                            })
                            .collect::<Vec<_>>();

                        // Sort updates by time
                        ready_updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

                        info!(
                            "{}: beginning transaction until {} with {} rows",
                            s.name,
                            upper.pretty(),
                            ready_updates.len()
                        );

                        s.producer
                            .retry_on_txn_error(|p| p.begin_transaction())
                            .await?;

                        let count_for_stats = u64::cast_from(ready_updates.len());
                        let mut total_size_for_stats = 0;

                        for ((key, value), time, diff) in ready_updates {
                            // apply_sink_envelope guarantees that diff == 1
                            assert_eq!(diff, 1, "invalid sink stream");

                            let mut size_for_stats = 0;
                            let record = BaseRecord::to(&s.topic);
                            let record = match value.as_ref() {
                                Some(r) => {
                                    size_for_stats += u64::cast_from(r.len());
                                    record.payload(r)
                                }
                                None => record,
                            };
                            let record = match key.as_ref() {
                                Some(r) => {
                                    size_for_stats += u64::cast_from(r.len());
                                    record.key(r)
                                }
                                None => record,
                            };

                            let ts_bytes = time.to_string().into_bytes();
                            let record = record.headers(OwnedHeaders::new().insert(Header {
                                key: "materialize-timestamp",
                                value: Some(&ts_bytes),
                            }));

                            total_size_for_stats += size_for_stats;

                            s.send(record).await?;
                            sink_statistics.inc_messages_staged_by(1);
                            sink_statistics.inc_bytes_staged_by(size_for_stats);
                            s.metrics.rows_queued.dec();
                        }

                        // Flush to make sure that errored messages have been properly retried before
                        // sending progress records and commit transactions.
                        s.flush().await?;

                        // We don't count this record as part of the message count in user-facing
                        // statistics.
                        s.send_progress_record(upper.clone()).await?;

                        info!("{}: committing transaction for {}", s.name, upper.pretty());
                        s.producer
                            .retry_on_txn_error(|p| p.commit_transaction())
                            .await?;

                        sink_statistics.inc_messages_committed_by(count_for_stats);
                        sink_statistics.inc_bytes_committed_by(total_size_for_stats);

                        s.flush().await?;

                        pending_batches.retain(|batch| !batch.is_empty());
                        lower.clone_from(&upper);
                        *write_frontier.borrow_mut() = upper;

                        // We want debug_assert but also to print out if we would have failed the assertion in release mode
                        let in_flight_count = s.producer.inner.in_flight_count();
                        let sends_flushed = s.retry_manager.lock().await.sends_flushed();
                        if cfg!(debug_assertions) {
                            assert_eq!(in_flight_count, 0);
                            assert!(sends_flushed);
                        } else {
                            if in_flight_count != 0 {
                                error!("Producer has {:?} messages in flight", in_flight_count);
                            }
                            if !sends_flushed {
                                error!("Retry manager has not flushed sends");
                            }
                        }
                    }
                }
            }
            Ok(())
        })
    });

    (errors, button.press_on_drop())
}

/// Encodes a stream of `(Option<Row>, Option<Row>)` updates using the specified encoder.
///
/// Input [`Row`] updates must me compatible with the given implementor of [`Encode`].
fn encode_collection<G: Scope>(
    name: String,
    input: &Collection<G, (Option<Row>, Option<Row>), Diff>,
    purify_stream: &Stream<G, PurifiedKafkaSinkConnection>,
    envelope: SinkEnvelope,
) -> (
    Collection<G, (Option<Vec<u8>>, Option<Vec<u8>>), Diff>,
    PressOnDropButton,
) {
    let mut builder = AsyncOperatorBuilder::new(name, input.inner.scope());

    let mut input = builder.new_input(&input.inner, Pipeline);
    let purify_input = builder.new_input(purify_stream, Pipeline);
    let (mut output, stream) = builder.new_output();

    let button = builder.build(move |_caps| async move {
        let PurifiedKafkaSinkConnection {
            connection,
            resume_upper: _,
        } = recv_one(purify_input).await;

        let key_desc = connection
            .key_desc_and_indices
            .as_ref()
            .map(|(desc, _indices)| desc.clone());
        let value_desc = connection.value_desc;

        let encoder: Box<dyn Encode> = match connection.format {
            KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::Published {
                key_schema_id,
                value_schema_id,
            }) => {
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
            KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::UnpublishedMaybe { .. }) => {
                unreachable!("must have communicated with CSR")
            }
            KafkaSinkFormat::Json => Box::new(JsonEncoder::new(
                key_desc,
                value_desc,
                matches!(envelope, SinkEnvelope::Debezium),
            )),
        };

        while let Some(event) = input.next_mut().await {
            if let Event::Data(cap, rows) = event {
                for ((key, value), time, diff) in rows.drain(..) {
                    let key = key.map(|key| encoder.encode_key_unchecked(key));
                    let value = value.map(|value| encoder.encode_value_unchecked(value));
                    output.give(&cap, ((key, value), time, diff)).await;
                }
            }
        }
    });

    (stream.as_collection(), button.press_on_drop())
}

/// A helper to receive a single value from a timely input.
async fn recv_one<T, P, D>(mut purify_input: AsyncInputHandle<T, Vec<D>, P>) -> D
where
    T: timely::progress::Timestamp,
    P: Pull<Bundle<T, D>>,
    D: Data,
{
    loop {
        match purify_input.next_mut().await {
            Some(Event::Data(_time, data)) => match data.pop() {
                Some(value) => break value,
                None => {}
            },
            Some(Event::Progress(_)) => {}
            None => panic!("input completed without data"),
        }
    }
}
