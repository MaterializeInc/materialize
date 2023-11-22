// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use differential_dataflow::{Collection, Hashable};
use futures::StreamExt;
use maplit::btreemap;
use mz_interchange::avro::{AvroEncoder, AvroSchemaGenerator, AvroSchemaOptions};
use mz_interchange::encode::Encode;
use mz_interchange::json::JsonEncoder;
use mz_kafka_util::client::{MzClientContext, TunnelingClientContext};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::retry::Retry;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_ssh_util::tunnel::SshTunnelStatus;
use mz_storage_client::client::SinkStatisticsUpdate;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::{ContextCreationError, DataflowError};
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkAvroFormatState, KafkaSinkConnection, KafkaSinkFormat,
    MetadataFilled, SinkAsOf, SinkEnvelope, StorageSinkDesc,
};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use prometheus::core::AtomicU64;
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::{KafkaError, RDKafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders, ToBytes};
use rdkafka::producer::{BaseRecord, Producer};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::Concat;
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::SinkRender;
use crate::sink::KafkaBaseMetrics;
use crate::statistics::{SinkStatisticsMetrics, StorageStatistics};
use crate::storage_state::StorageState;

use self::util::KafkaSinkSendRetryManager;

mod util;

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
    ) -> (Stream<G, HealthStatusMessage>, Option<Rc<dyn Any>>)
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

        let (health, token) = kafka(
            sinked_collection,
            sink_id,
            self.clone(),
            sink.envelope,
            sink.as_of.clone(),
            Rc::clone(&shared_frontier),
            storage_state.sink_metrics.kafka.clone(),
            storage_state
                .sink_statistics
                .get(&sink_id)
                .expect("statistics initialized")
                .clone(),
            storage_state.connection_context.clone(),
        );

        storage_state
            .sink_write_frontiers
            .insert(sink_id, shared_frontier);

        (health, Some(token))
    }
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
    producer: util::KafkaTxProducer,
    pending_rows: BTreeMap<Timestamp, Vec<EncodedRow>>,
    ready_rows: VecDeque<(Timestamp, Vec<EncodedRow>)>,
    retry_manager: Arc<Mutex<KafkaSinkSendRetryManager>>,

    progress_topic: String,
    progress_key: String,
    progress_client: Option<Arc<BaseConsumer<TunnelingClientContext<MzClientContext>>>>,

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
        worker_id: String,
        write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
        metrics: &KafkaBaseMetrics,
        connection_context: &ConnectionContext,
        gate_ts: Rc<Cell<Option<Timestamp>>>,
        healthchecker: HealthOutputHandle,
    ) -> Self {
        let metrics = Arc::new(SinkMetrics::new(
            metrics,
            &connection.topic,
            &sink_id.to_string(),
            &worker_id,
        ));

        let retry_manager = Arc::new(Mutex::new(KafkaSinkSendRetryManager::new()));

        let producer_context = util::SinkProducerContext {
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

                util::KafkaTxProducer::new(util::KafkaTxProducerConfig {
                    name: sink_name.clone(),
                    connection: &connection.connection,
                    connection_context,
                    producer_context,
                    sink_id,
                    worker_id,
                })
                .await
            })()
            .await,
            None,
        )
        .await;

        let progress_client = halt_on_err(
            &healthchecker,
            connection
                .connection
                .create_with_context(
                    connection_context,
                    MzClientContext::default(),
                    &btreemap! {
                        "group.id" => util::SinkGroupId::new(sink_id),
                        "isolation.level" => "read_committed".into(),
                        "enable.auto.commit" => "false".into(),
                        "auto.offset.reset" => "earliest".into(),
                        "enable.partition.eof" => "true".into(),
                    },
                )
                .await,
            None,
        )
        .await;

        KafkaSinkState {
            name: sink_name,
            topic: connection.topic,
            metrics,
            producer,
            pending_rows: BTreeMap::new(),
            ready_rows: VecDeque::new(),
            retry_manager,
            progress_topic: match connection.consistency_config {
                KafkaConsistencyConfig::Progress { topic } => topic,
            },
            progress_key: util::ProgressKey::new(sink_id),
            progress_client: Some(Arc::new(progress_client)),
            healthchecker,
            gate_ts,
            latest_progress_ts: Timestamp::minimum(),
            write_frontier,
        }
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
            .clamp_backoff(util::BACKOFF_CLAMP)
            .retry_async(|_| self.producer.flush())
            .await
            .expect("Infinite retry cannot fail");
    }

    async fn send_progress_record(&self, transaction_id: Timestamp) {
        let encoded = serde_json::to_vec(&util::ProgressRecord {
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
                self.send_progress_record(min_frontier).await;

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
            let hint: Option<String> = if let ContextCreationError::Other(error) = &error {
                error
                    .downcast_ref::<RDKafkaError>()
                    .and_then(|kafka_error| {
                        if kafka_error.is_retriable()
                            && kafka_error.code() == RDKafkaErrorCode::OperationTimedOut
                        {
                            Some(
                                "If you're running a single Kafka broker, ensure \
                                    that the configs transaction.state.log.replication.factor, \
                                    transaction.state.log.min.isr, and \
                                    offsets.topic.replication.factor are set to 1 on the broker"
                                    .to_string(),
                            )
                        } else {
                            None
                        }
                    })
            } else {
                None
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
    metrics: KafkaBaseMetrics,
    sink_statistics: StorageStatistics<SinkStatisticsUpdate, SinkStatisticsMetrics>,
    connection_context: ConnectionContext,
) -> (Stream<G, HealthStatusMessage>, Rc<dyn Any>)
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
        connection_context.clone(),
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
        connection_context,
    );

    (
        encode_health_stream.concat(&produce_health_stream),
        Rc::new((encode_token, produce_token)),
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
    metrics: KafkaBaseMetrics,
    sink_statistics: StorageStatistics<SinkStatisticsUpdate, SinkStatisticsMetrics>,
    connection_context: ConnectionContext,
) -> (Stream<G, HealthStatusMessage>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
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

        let healthchecker = HealthOutputHandle {
            health_cap,
            handle: Mutex::new(health_output),
        };

        // Note that where this lies in the rendering cycle means that we might
        // create the collateral topics each time the sink is rendered, e.g. if
        // the broker's admin deleted the progress and data topics. For more
        // details, see `mz_storage_client::sink::build_kafka`.
        let latest_ts = halt_on_err(
            &healthchecker,
            util::build_kafka(id, &connection, &connection_context).await,
            None,
        )
        .await;

        let mut s = KafkaSinkState::new(
            id,
            connection,
            name,
            worker_id.to_string(),
            write_frontier,
            &metrics,
            &connection_context,
            Rc::clone(&shared_gate_ts),
            healthchecker,
        )
        .await;

        s.halt_on_err(
            s.producer
                .retry_on_txn_error(|p| p.init_transactions())
                .await,
        )
        .await;

        info!(
            "{}: initial as_of: {:?}, latest progress record: {:?}",
            s.name, as_of.frontier, latest_ts
        );
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

        s.update_status(HealthStatusUpdate::running(), StatusNamespace::Kafka)
            .await;

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
                            "Beginning transaction for {:?} with {:?} rows",
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
                        s.send_progress_record(*ts).await;

                        info!("Committing transaction for {:?}", ts,);
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

    (health_stream, Rc::new(button.press_on_drop()))
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
    mut connection: KafkaSinkConnection,
    connection_context: ConnectionContext,
    envelope: SinkEnvelope,
) -> (
    Stream<G, ((Option<Vec<u8>>, Option<Vec<u8>>), Timestamp, Diff)>,
    Stream<G, HealthStatusMessage>,
    Rc<dyn Any>,
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

        // Ensure that Kafka collateral exists. While this looks somewhat
        // innocuous, this step is why this must be an async operator.
        let healthchecker = HealthOutputHandle {
            health_cap,
            handle: Mutex::new(health_output),
        };

        let _ = halt_on_err(
            &healthchecker,
            util::publish_schemas(
                &connection_context,
                &mut connection.format,
                &connection.topic,
            )
            .await,
            None,
        )
        .await;

        let key_desc = connection
            .key_desc_and_indices
            .as_ref()
            .map(|(desc, _indices)| desc.clone());
        let value_desc = connection.value_desc.clone();

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

    (stream, health_stream, Rc::new(button.press_on_drop()))
}
