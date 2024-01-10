// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use differential_dataflow::{Collection, Hashable};
use futures::{StreamExt, TryFutureExt};
use maplit::btreemap;
use mz_interchange::avro::{AvroEncoder, AvroSchemaGenerator, AvroSchemaOptions};
use mz_interchange::encode::Encode;
use mz_interchange::json::JsonEncoder;
use mz_kafka_util::client::{MzClientContext, TunnelingClientContext};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::task;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_ssh_util::tunnel::SshTunnelStatus;
use mz_storage_client::sink::progress_key::ProgressKey;
use mz_storage_client::sink::ProgressRecord;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{ContextCreationError, ContextCreationErrorExt, DataflowError};
use mz_storage_types::sinks::{
    KafkaSinkConnection, KafkaSinkFormat, MetadataFilled, SinkAsOf, SinkEnvelope, StorageSinkDesc,
};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders, OwnedMessage, ToBytes};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::Concat;
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::metrics::sink::SinkMetrics;
use crate::metrics::StorageMetrics;
use crate::render::sinks::SinkRender;
use crate::statistics::SinkStatistics;
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
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.
        _err_collection: Collection<G, DataflowError, Diff>,
    ) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>)
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // TODO: this is a brittle way to indicate the worker that will write to the sink
        // because it relies on us continuing to hash on the sink_id, with the same hash
        // function, and for the Exchange pact to continue to distribute by modulo number
        // of workers.
        let peers = sinked_collection.inner.scope().peers();
        let worker_index = sinked_collection.inner.scope().index();
        let active_write_worker = (usize::cast_from(sink_id.hashed()) % peers) == worker_index;

        // Only the active_write_worker will ever produce data so all other workers have
        // an empty frontier.  It's necessary to insert all of these into `storage_state.
        // sink_write_frontier` below so we properly clear out default frontiers of
        // non-active workers.
        let shared_frontier = Rc::new(RefCell::new(if active_write_worker {
            Antichain::from_elem(Timestamp::minimum())
        } else {
            Antichain::new()
        }));

        let (health, tokens) = kafka(
            sinked_collection,
            sink_id,
            self.clone(),
            sink.envelope,
            sink.as_of.clone(),
            Rc::clone(&shared_frontier),
            &storage_state.metrics,
            storage_state
                .sink_statistics
                .get(&sink_id)
                .expect("statistics initialized")
                .clone(),
            &storage_state.storage_configuration,
        );

        storage_state
            .sink_write_frontiers
            .insert(sink_id, shared_frontier);

        (health, tokens)
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

struct KafkaTxProducerConfig<'a> {
    name: String,
    connection: &'a KafkaSinkConnection,
    storage_configuration: &'a StorageConfiguration,
    producer_context: SinkProducerContext,
    sink_id: GlobalId,
}

#[derive(Clone)]
struct KafkaTxProducer {
    name: String,
    inner: Arc<ThreadedProducer<TunnelingClientContext<SinkProducerContext>>>,
    timeout: Duration,
}

impl KafkaTxProducer {
    async fn new<'a>(
        KafkaTxProducerConfig {
            name,
            connection,
            storage_configuration,
            producer_context,
            sink_id,
        }: KafkaTxProducerConfig<'a>,
    ) -> Result<KafkaTxProducer, ContextCreationError> {
        let client_id = connection.client_id(&storage_configuration.connection_context, sink_id);
        let transactional_id =
            connection.transactional_id(&storage_configuration.connection_context, sink_id);

        let producer = connection
            .connection
            .create_with_context(
                storage_configuration,
                producer_context,
                &btreemap! {
                    // Allow Kafka monitoring tools to identify this producer.
                    "client.id" => client_id,
                    // Ensure that messages are sinked in order and without
                    // duplicates. Note that this only applies to a single
                    // instance of a producer - in the case of restarts, all
                    // bets are off and full exactly once support is required.
                    "enable.idempotence" => "true".into(),
                    // Use the compression type requested by the user.
                    "compression.type" => connection.compression_type.to_librdkafka_option().into(),
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
                    // Use the transactional ID requested by the user.
                    "transactional.id" => transactional_id,
                    // Use the default transaction timeout. (At time of writing: 60s.)
                    // The Kafka sink may have long-running transactions, since it expects
                    // to be able to write all data at the same timestamp in a single
                    // transaction... including the initial snapshot.
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
            .await
            .check_ssh_status(producer.inner.client().context())?;

        Ok(producer)
    }

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

struct HealthOutputHandle {
    health_cap: timely::dataflow::operators::Capability<Timestamp>,
    // TODO(guswynn): We probably don't need this Mutex, but removing it
    // is a large refactor of this entire module.
    handle: Mutex<
        mz_timely_util::builder_async::AsyncOutputHandle<
            mz_repr::Timestamp,
            Vec<HealthStatusMessage>,
            TeeCore<mz_repr::Timestamp, Vec<HealthStatusMessage>>,
        >,
    >,
}

struct KafkaSinkState {
    name: String,
    topic: String,
    metrics: Arc<SinkMetrics>,
    producer: KafkaTxProducer,
    pending_rows: BTreeMap<Timestamp, Vec<EncodedRow>>,
    ready_rows: VecDeque<(Timestamp, Vec<EncodedRow>)>,
    retry_manager: Arc<Mutex<KafkaSinkSendRetryManager>>,

    progress_topic: String,
    progress_key: ProgressKey,

    healthchecker: HealthOutputHandle,
    gate_ts: Rc<Cell<Option<Timestamp>>>,

    /// Timestamp of the latest progress record that was written out to Kafka.
    latest_progress_ts: Timestamp,

    /// Write frontier of this sink.
    ///
    /// The write frontier potentially blocks compaction of timestamp bindings
    /// in upstream sources. The latest written progress record is used when
    /// restarting the sink to gate updates with a lower timestamp. We advance
    /// the write frontier in lockstep with writing out progress records. This
    /// ensures that we don't write updates more than once, ensuring
    /// exactly-once guarantees.
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
}

impl KafkaSinkState {
    // Until `try` blocks, we need this for using `fail_point!` correctly.
    async fn new(
        sink_id: GlobalId,
        connection: KafkaSinkConnection,
        sink_name: String,
        worker_id: usize,
        write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
        metrics: &StorageMetrics,
        storage_configuration: &StorageConfiguration,
        gate_ts: Rc<Cell<Option<Timestamp>>>,
        healthchecker: HealthOutputHandle,
    ) -> (Self, Option<Option<Timestamp>>) {
        let metrics = Arc::new(metrics.get_sink_metrics(&connection.topic, sink_id, worker_id));

        let retry_manager = Arc::new(Mutex::new(KafkaSinkSendRetryManager::new()));

        let producer_context = SinkProducerContext {
            metrics: Arc::clone(&metrics),
            retry_manager: Arc::clone(&retry_manager),
            inner: MzClientContext::default(),
        };

        let producer = halt_on_err(
            &healthchecker,
            #[allow(clippy::redundant_closure_call)]
            (|| async {
                fail::fail_point!("kafka_sink_creation_error", |_| Err(
                    ContextCreationError::Other(anyhow::anyhow!("synthetic error"))
                ));

                KafkaTxProducer::new(KafkaTxProducerConfig {
                    name: sink_name.clone(),
                    connection: &connection,
                    storage_configuration,
                    producer_context,
                    sink_id,
                })
                .await
            })()
            .await,
            None,
        )
        .await;

        // Note that where this lies in the rendering cycle means that we might
        // create the collateral topics each time the sink is rendered, e.g. if
        // the broker's admin deleted the progress and data topics. For more
        // details, see `mz_storage_client::sink::build_kafka`.
        let latest_ts = halt_on_err(
            &healthchecker,
            mz_storage_client::sink::build_kafka(sink_id, &connection, storage_configuration).await,
            None,
        )
        .await;

        let progress_topic = connection
            .progress_topic(&storage_configuration.connection_context)
            .into_owned();
        (
            KafkaSinkState {
                name: sink_name,
                topic: connection.topic,
                metrics,
                producer,
                pending_rows: BTreeMap::new(),
                ready_rows: VecDeque::new(),
                retry_manager,
                progress_topic,
                progress_key: ProgressKey::new(sink_id),
                healthchecker,
                gate_ts,
                latest_progress_ts: Timestamp::minimum(),
                write_frontier,
            },
            latest_ts,
        )
    }

    async fn send<'a, K, P>(&self, mut record: BaseRecord<'a, K, P>)
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
                    return;
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
                        self.halt_on_err(Err(anyhow!(format!(
                            "fatal error while producing message in {}: {e}",
                            self.name
                        ))))
                        .await
                    }
                }
            }
        }
    }

    async fn flush(&self) {
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
                self.send(transformed_msg).await;
            }
            self.flush_inner().await;
        }
    }

    async fn flush_inner(&self) {
        Retry::default()
            .max_tries(usize::MAX)
            .clamp_backoff(BACKOFF_CLAMP)
            .retry_async(|_| self.producer.flush())
            .await
            .expect("Infinite retry cannot fail");
    }

    async fn send_progress_record(&self, transaction_id: Option<Timestamp>) {
        let encoded = serde_json::to_vec(&ProgressRecord {
            timestamp: transaction_id,
        })
        .expect("serialization to vec cannot fail");
        let record = BaseRecord::to(&self.progress_topic)
            .payload(&encoded)
            .key(&self.progress_key);
        self.send(record).await
    }

    /// Asserts that the write frontier has not yet advanced beyond `t`.
    fn assert_progress(&self, ts: &Timestamp) {
        assert!(self.write_frontier.borrow().less_equal(ts));
    }

    /// Updates the latest progress update timestamp to `latest_update_ts` if it
    /// is greater than the currently maintained timestamp.
    ///
    /// This does not emit a progress update and should be used when the sink
    /// emits a progress update that is not based on updates of the input
    /// frontier.
    ///
    /// See [`maybe_emit_progress`](Self::maybe_emit_progress).
    fn maybe_update_progress(&mut self, latest_update_ts: &Timestamp) {
        if *latest_update_ts > self.latest_progress_ts {
            self.latest_progress_ts = *latest_update_ts;
        }
    }

    /// Updates the latest progress update timestamp based on the given
    /// input frontier and pending rows.
    ///
    /// This will emit a progress record to the progress topic if the frontier
    /// advanced and advance the maintained write frontier, which will in turn
    /// unblock compaction of timestamp bindings in sources.
    ///
    /// *NOTE*: Progress records will only be emitted when
    /// `KafkaSinkConnection.consistency` points to a progress topic. The
    /// write frontier will be advanced regardless.
    async fn maybe_emit_progress(
        &mut self,
        mut input_frontier: Antichain<Timestamp>,
        as_of: &SinkAsOf<Timestamp>,
    ) -> bool {
        input_frontier.extend(self.pending_rows.keys().min().cloned());
        let min_frontier = input_frontier;

        // If we emit a progress record before the as_of, we open ourselves to the possibility that
        // we restart the sink with a gate timestamp behind the ASOF.  If that happens, we're unable
        // to tell the difference between some records we've already written out and those that we
        // still need to write out. (This is because asking for records with a given ASOF will fast
        // forward all records at or before to the requested ASOF.)
        //
        // N.B. This condition is _not_ symmetric with the assert on the gate_ts on sink startup
        // because we subtract 1 below to determine what progress value to actually write out.
        if !PartialOrder::less_than(&as_of.frontier, &min_frontier) {
            return false;
        }

        let mut progress_emitted = false;

        if let Some(min_frontier) = min_frontier.into_option() {
            // A frontier of `t` means we still might receive updates with `t`. The progress
            // frontier we emit `f` indicates that all future values will be greater than `f`.
            //
            // The progress notion of frontier does not mesh with the notion of frontier in timely.
            // This is confusing -- but still preferrable.  Were we to define the "progress
            // frontier" to be the earliest time we could still write more data, we would need to
            // write out a progress record of `t + 1` when we write out data.  Furthermore, were we
            // to ever move to a notion of multi-dimensional time, it is more natural (and easier
            // to keep the system correct) to record the time of the data we write to the progress
            // topic.
            let min_frontier = min_frontier.saturating_sub(1);

            if min_frontier > self.latest_progress_ts {
                // record the write frontier in the progress topic.
                self.halt_on_err(
                    self.producer
                        .retry_on_txn_error(|p| p.begin_transaction())
                        .await,
                )
                .await;

                debug!(
                    "{}: sending progress for gate ts: {:?}",
                    &self.name, min_frontier
                );
                self.send_progress_record(Some(min_frontier)).await;

                self.halt_on_err(
                    self.producer
                        .retry_on_txn_error(|p| p.commit_transaction())
                        .await,
                )
                .await;

                progress_emitted = true;
                self.latest_progress_ts = min_frontier;
            }

            let mut write_frontier = self.write_frontier.borrow_mut();

            // make sure we don't regress
            debug!(
                "{}: downgrading write frontier to: {:?}",
                &self.name, min_frontier
            );
            assert!(write_frontier.less_equal(&min_frontier));
            write_frontier.clear();
            write_frontier.insert(min_frontier);
        } else {
            // record the write frontier in the progress topic.
            self.halt_on_err(
                self.producer
                    .retry_on_txn_error(|p| p.begin_transaction())
                    .await,
            )
            .await;

            debug!("{}: sending progress for empty frontier", &self.name);
            self.send_progress_record(None).await;

            self.halt_on_err(
                self.producer
                    .retry_on_txn_error(|p| p.commit_transaction())
                    .await,
            )
            .await;

            // If there's no longer an input frontier, we will no longer receive any data forever and, therefore, will
            // never output more data
            info!("{}: advancing write frontier to empty", &self.name);
            self.write_frontier.borrow_mut().clear();
        }

        progress_emitted
    }

    async fn update_status(&self, status: HealthStatusUpdate, namespace: StatusNamespace) {
        update_status(&self.healthchecker, status, namespace).await;
    }

    /// Report a stalled HealthStatusUpdate and then halt with the same message.
    pub async fn halt_on_err<T>(&self, result: Result<T, anyhow::Error>) -> T {
        // We may not get an up-to-date ssh status here, but on restart we will.
        let ssh_status = self.producer.inner.client().context().tunnel_status();

        halt_on_err(
            &self.healthchecker,
            result.map_err(ContextCreationError::Other),
            Some(ssh_status),
        )
        .await
    }
}

async fn update_status(
    healthchecker: &HealthOutputHandle,
    status: HealthStatusUpdate,
    namespace: StatusNamespace,
) {
    healthchecker
        .handle
        .lock()
        .await
        .give(
            &healthchecker.health_cap,
            HealthStatusMessage {
                // sinks only have 1 logical object.
                index: 0,
                namespace,
                update: status,
            },
        )
        .await;
}

async fn halt_on_err<T>(
    healthchecker: &HealthOutputHandle,
    // We use a `ContextCreationError` for convenience here because it can be an ssh error during
    // `create_with_context` calls, or an `anyhow::Error` for transient failures during operation.
    // In the future we could probably also use the `KafkaError` variant as well.
    result: Result<T, ContextCreationError>,
    // The status of the ssh tunnel, if it already exists.
    ssh_status: Option<SshTunnelStatus>,
) -> T {
    match result {
        Ok(t) => t,
        Err(error) => {
            let kafka_error = match error {
                ContextCreationError::KafkaError(KafkaError::Transaction(ref e)) => Some(e),
                ContextCreationError::Other(ref err) => err.downcast_ref::<RDKafkaError>(),
                ContextCreationError::KafkaError(_) | ContextCreationError::Ssh(_) => None,
            };
            let hint = match kafka_error {
                Some(e) => {
                    if e.is_retriable() && e.code() == RDKafkaErrorCode::OperationTimedOut {
                        let hint =
                            "If you're running a single Kafka broker, ensure that the configs \
                        transaction.state.log.replication.factor, transaction.state.log.min.isr, \
                        and offsets.topic.replication.factor are set to 1 on the broker";
                        Some(hint.to_owned())
                    } else {
                        None
                    }
                }
                None => None,
            };

            // Update the ssh status. This could be overridden below, but this ensures the
            // user gets a useful error if the kafka error is a result of this status.
            if let Some(SshTunnelStatus::Errored(e)) = ssh_status {
                update_status(
                    healthchecker,
                    HealthStatusUpdate::stalled(e, None),
                    StatusNamespace::Ssh,
                )
                .await;
            }

            update_status(
                healthchecker,
                HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), hint),
                if matches!(error, ContextCreationError::Ssh(_)) {
                    StatusNamespace::Ssh
                } else {
                    StatusNamespace::Kafka
                },
            )
            .await;

            // Make sure to never return, preventing the sink from writing
            // out anything it might regret in the future.
            future::pending().await
        }
    }
}

#[derive(Debug)]
struct EncodedRow {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    count: usize,
}

fn kafka<G>(
    collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    id: GlobalId,
    connection: KafkaSinkConnection,
    envelope: SinkEnvelope,
    as_of: SinkAsOf,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
    metrics: &StorageMetrics,
    sink_statistics: SinkStatistics,
    storage_configuration: &StorageConfiguration,
) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!("kafka-{}", id);

    let stream = &collection.inner;

    let shared_gate_ts = Rc::new(Cell::new(None));

    let (encoded_stream, encode_health_stream, encode_token) = encode_stream(
        stream,
        id,
        as_of.clone(),
        Rc::clone(&shared_gate_ts),
        &name,
        connection.clone(),
        storage_configuration.clone(),
        envelope,
    );

    let (produce_health_stream, produce_token) = produce_to_kafka(
        encoded_stream,
        id,
        name,
        connection,
        as_of,
        shared_gate_ts,
        write_frontier,
        metrics,
        sink_statistics,
        storage_configuration.clone(),
    );

    (
        encode_health_stream.concat(&produce_health_stream),
        vec![encode_token, produce_token],
    )
}

/// Produces/sends a stream of encoded rows (as `Vec<u8>`) to Kafka.
///
/// This operator exchanges all updates to a single worker by hashing on the given sink `id`.
///
/// Updates are only sent to Kafka once the input frontier has passed their `time`. Updates are
/// sent in ascending timestamp order. The order of updates at the same timestamp will not be changed.
/// However, it is important to keep in mind that this operator exchanges updates so if the input
/// stream is sharded updates will likely arrive at this operator in some non-deterministic order.
///
/// Updates that are not beyond the given [`SinkAsOf`] and/or the `gate_ts` in
/// [`KafkaSinkConnection`] will be discarded without producing them.
fn produce_to_kafka<G>(
    stream: Stream<G, ((Option<Vec<u8>>, Option<Vec<u8>>), Timestamp, Diff)>,
    id: GlobalId,
    name: String,
    connection: KafkaSinkConnection,
    as_of: SinkAsOf,
    shared_gate_ts: Rc<Cell<Option<Timestamp>>>,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
    metrics: &StorageMetrics,
    sink_statistics: SinkStatistics,
    storage_configuration: StorageConfiguration,
) -> (Stream<G, HealthStatusMessage>, PressOnDropButton)
where
    G: Scope<Timestamp = Timestamp>,
{
    let metrics = metrics.clone();
    let worker_id = stream.scope().index();
    let worker_count = stream.scope().peers();
    let mut builder = AsyncOperatorBuilder::new(name.clone(), stream.scope());

    // keep the latest progress updates, if any, in order to update
    // our internal state after the send loop
    let mut progress_update = None;

    // We want exactly one worker to send all the data to the sink topic.
    let hashed_id = id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % worker_count == worker_id;

    let mut input = builder.new_input(&stream, Exchange::new(move |_| hashed_id));

    // The frontier of this output is never inspected, so we can just use a
    // normal output, even if its frontier is connected to the input.
    //
    // n.b. each operator needs to have its own `HealthOutputHandle` and they
    // cannot be shared between operators.
    let (health_output, health_stream) = builder.new_output();

    let button = builder.build(move |caps| async move {
        let [health_cap]: [_; 1] = caps.try_into().unwrap();

        if !is_active_worker {
            return;
        }

        let (mut s, latest_ts) = KafkaSinkState::new(
            id,
            connection,
            name,
            worker_id,
            write_frontier,
            &metrics,
            &storage_configuration,
            Rc::clone(&shared_gate_ts),
            HealthOutputHandle {
                health_cap,
                handle: Mutex::new(health_output),
            },
        )
        .await;

        info!(
            "{}: initial as_of: {:?}, latest progress record: {:?}",
            s.name, as_of.frontier, latest_ts
        );

        s.update_status(HealthStatusUpdate::running(), StatusNamespace::Kafka)
            .await;

        let latest_ts = match latest_ts {
            Some(Some(latest_ts)) => Some(latest_ts),
            Some(None) => {
                info!("{}: sink shutting down", s.name);
                return;
            }
            None => None,
        };
        shared_gate_ts.set(latest_ts);

        if let Some(gate) = latest_ts {
            assert!(
                PartialOrder::less_equal(&as_of.frontier, &Antichain::from_elem(gate)),
                "{}: some element of the Sink as_of frontier is too \
                    far advanced for our output-gating timestamp: \
                    as_of {:?}, gate_ts: {:?}",
                s.name,
                as_of.frontier,
                gate
            );
            s.maybe_update_progress(&gate);
        }

        while let Some(event) = input.next_mut().await {
            match event {
                Event::Data(_, rows) => {
                    // Queue all pending rows waiting to be sent to kafka
                    assert!(is_active_worker);
                    for ((key, value), time, diff) in rows.drain(..) {
                        let should_emit = if as_of.strict {
                            as_of.frontier.less_than(&time)
                        } else {
                            as_of.frontier.less_equal(&time)
                        };

                        let previously_published = Some(time) <= s.gate_ts.get();

                        if !should_emit || previously_published {
                            // Skip stale data for already published timestamps
                            continue;
                        }

                        if diff == 0 {
                            // Explicitly refuse to send no-op records
                            continue;
                        };
                        let count =
                            usize::try_from(diff).expect("can't sink negative multiplicities");

                        let rows = s.pending_rows.entry(time).or_default();
                        rows.push(EncodedRow { key, value, count });
                        s.metrics.rows_queued.inc();
                    }
                }
                Event::Progress(frontier) => {
                    // Move any newly closed timestamps from pending to ready
                    let mut closed_ts: Vec<Timestamp> = s
                        .pending_rows
                        .iter()
                        .filter(|(ts, _)| !frontier.less_equal(*ts))
                        .map(|(&ts, _)| ts)
                        .collect();
                    closed_ts.sort_unstable();
                    closed_ts.into_iter().for_each(|ts| {
                        let rows = s.pending_rows.remove(&ts).unwrap();
                        s.ready_rows.push_back((ts, rows));
                    });

                    while let Some((ts, rows)) = s.ready_rows.front() {
                        assert!(is_active_worker);

                        info!(
                            "{}: beginning transaction for {:?} with {:?} rows",
                            id,
                            ts,
                            rows.len()
                        );
                        s.halt_on_err(
                            s.producer
                                .retry_on_txn_error(|p| p.begin_transaction())
                                .await,
                        )
                        .await;

                        let mut repeat_counter = 0;

                        let count_for_stats = u64::cast_from(rows.len());
                        let mut total_size_for_stats = 0;
                        for encoded_row in rows {
                            let record = BaseRecord::to(&s.topic);
                            let record = match encoded_row.value.as_ref() {
                                Some(r) => record.payload(r),
                                None => record,
                            };
                            let record = match encoded_row.key.as_ref() {
                                Some(r) => record.key(r),
                                None => record,
                            };

                            let ts_bytes = ts.to_string().into_bytes();
                            let record = record.headers(OwnedHeaders::new().insert(Header {
                                key: "materialize-timestamp",
                                value: Some(&ts_bytes),
                            }));

                            let size_for_stats =
                                u64::cast_from(record.payload.as_ref().map_or(0, |p| p.len()))
                                    + u64::cast_from(record.key.as_ref().map_or(0, |k| k.len()));
                            total_size_for_stats += size_for_stats;

                            s.send(record).await;
                            sink_statistics.inc_messages_staged_by(1);
                            sink_statistics.inc_bytes_staged_by(size_for_stats);

                            // advance to the next repetition of this row, or the next row if all
                            // repetitions are exhausted
                            repeat_counter += 1;
                            if repeat_counter == encoded_row.count {
                                repeat_counter = 0;
                                s.metrics.rows_queued.dec();
                            }
                        }

                        // Flush to make sure that errored messages have been properly retried before
                        // sending progress records and commit transactions.
                        s.flush().await;

                        // We don't count this record as part of the message count in user-facing
                        // statistics.
                        s.send_progress_record(Some(*ts)).await;

                        info!("{}: committing transaction for {:?}", id, ts);
                        s.halt_on_err(
                            s.producer
                                .retry_on_txn_error(|p| p.commit_transaction())
                                .await,
                        )
                        .await;
                        sink_statistics.inc_messages_committed_by(count_for_stats);
                        sink_statistics.inc_bytes_committed_by(total_size_for_stats);

                        s.flush().await;

                        // sanity check for the continuous updating
                        // of the write frontier below
                        s.assert_progress(ts);
                        progress_update.replace(ts.clone());

                        s.ready_rows.pop_front();
                    }

                    // Update our state based on any progress we may have sent.  This
                    // call is required for us to periodically write progress updates
                    // even without new data coming in.
                    if let Some(ts) = progress_update.take() {
                        s.maybe_update_progress(&ts);
                    }

                    // If we don't have ready rows, our write frontier equals the minimum
                    // of the input frontier and any stashed timestamps.
                    // While we still have ready rows that we're emitting, hold the write
                    // frontier at the previous time.
                    //
                    // Only one worker receives all the updates and we don't want the
                    // other workers to also emit progress.
                    if is_active_worker {
                        let progress_emitted =
                            s.maybe_emit_progress(frontier.clone(), &as_of).await;
                        if progress_emitted {
                            // Don't flush if we know there were no records emitted.
                            // It has a noticeable negative performance impact.
                            s.flush().await;
                        }
                    }

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
    });

    (health_stream, button.press_on_drop())
}

/// Encodes a stream of `(Option<Row>, Option<Row>)` updates using the specified encoder.
///
/// This operator will only encode `fuel` number of updates per invocation. If necessary, it will
/// stash updates and use an [`timely::scheduling::Activator`] to re-schedule future invocations.
///
/// Input [`Row`] updates must me compatible with the given implementor of [`Encode`].
///
/// Updates that are not beyond the given [`SinkAsOf`] and/or the `gate_ts` will be discarded
/// without encoding them.
///
/// Input updates do not have to be partitioned and/or sorted. This operator will not exchange
/// data. Updates with lower timestamps will be processed before updates with higher timestamps
/// if they arrive in order. However, this is not a guarantee, as this operator does not wait
/// for the frontier to signal completeness. It is an optimization for downstream operators
/// that behave suboptimal when receiving updates that are too far in the future with respect
/// to the current frontier. The order of updates that arrive at the same timestamp will not be
/// changed.
fn encode_stream<G>(
    input_stream: &Stream<G, ((Option<Row>, Option<Row>), Timestamp, Diff)>,
    sink_id: GlobalId,
    as_of: SinkAsOf,
    shared_gate_ts: Rc<Cell<Option<Timestamp>>>,
    name_prefix: &str,
    connection: KafkaSinkConnection,
    storage_configuration: StorageConfiguration,
    envelope: SinkEnvelope,
) -> (
    Stream<G, ((Option<Vec<u8>>, Option<Vec<u8>>), Timestamp, Diff)>,
    Stream<G, HealthStatusMessage>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!(
        "{}-{}_encode",
        name_prefix,
        connection.format.get_format_name()
    );
    let worker_id = input_stream.scope().index();
    let worker_count = input_stream.scope().peers();
    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % worker_count == worker_id;

    let mut builder = AsyncOperatorBuilder::new(name.clone(), input_stream.scope());

    let mut input = builder.new_input(input_stream, Exchange::new(move |_| hashed_id));
    let (mut output, stream) = builder.new_output();

    // The frontier of this output is never inspected, so we can just use a normal output,
    // even if its frontier is connected to the input.
    let (health_output, health_stream) = builder.new_output();

    let button = builder.build(move |caps| async move {
        let [output_cap, health_cap]: [_; 2] = caps.try_into().unwrap();
        drop(output_cap);

        if !is_active_worker {
            return;
        }

        let healthchecker = HealthOutputHandle {
            health_cap,
            handle: Mutex::new(health_output),
        };

        let key_desc = connection
            .key_desc_and_indices
            .as_ref()
            .map(|(desc, _indices)| desc.clone());
        let value_desc = connection.value_desc.clone();

        let encoder: Box<dyn Encode> = match connection.format {
            KafkaSinkFormat::Avro {
                key_schema,
                value_schema,
                csr_connection,
            } => {
                // Ensure that schemas are registered with the schema registry.
                // While this looks somewhat innocuous, this step is why this
                // must be an async operator.
                //
                // Also note that where this lies in the rendering cycle means
                // that we will publish the schemas each time the sink is
                // rendered.
                let (key_schema_id, value_schema_id) = halt_on_err(
                    &healthchecker,
                    async {
                        let ccsr = csr_connection.connect(&storage_configuration).await?;
                        let ids = mz_storage_client::sink::publish_kafka_schemas(
                            &ccsr,
                            &connection.topic,
                            key_schema.as_deref(),
                            Some(mz_ccsr::SchemaType::Avro),
                            &value_schema,
                            mz_ccsr::SchemaType::Avro,
                        )
                        .await
                        .context("error publishing kafka schemas for sink")?;
                        Ok(ids)
                    }
                    .await,
                    None,
                )
                .await;

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

        while let Some(event) = input.next_mut().await {
            if let Event::Data(cap, rows) = event {
                for ((key, value), time, diff) in rows.drain(..) {
                    let should_emit = if as_of.strict {
                        as_of.frontier.less_than(&time)
                    } else {
                        as_of.frontier.less_equal(&time)
                    };
                    let ts_gated = Some(time) <= shared_gate_ts.get();

                    if !should_emit || ts_gated {
                        // Skip stale data for already published timestamps
                        continue;
                    }

                    let key = key.map(|key| encoder.encode_key_unchecked(key));
                    let value = value.map(|value| encoder.encode_value_unchecked(value));

                    output.give(&cap, ((key, value), time, diff)).await;
                }
            }
        }
    });

    (stream, health_stream, button.press_on_drop())
}
