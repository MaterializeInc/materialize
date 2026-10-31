// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile-time checks that the public API accepts the call shapes Materialize
//! uses with rust-rdkafka.
//!
//! The functions below mirror call sites in Materialize (contexts, source,
//! sink, admin, testdrive, kgen). Most are never called; compiling them is the
//! test. Keep them in sync with the facade's rdkafka-compatibility contract.

#![allow(dead_code)]

use std::error::Error;
use std::io;
use std::iter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use crate::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigResource, ConfigSource,
    NewPartitions, NewTopic, OwnedResourceSpecifier, ResourceSpecifier, TopicReplication,
};
use crate::client::{Client, DefaultClientContext, OAuthToken};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::consumer::base_consumer::PartitionQueue;
use crate::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
    StreamConsumer,
};
use crate::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use crate::message::{BorrowedMessage, Header, Headers, OwnedHeaders, OwnedMessage, ToBytes};
use crate::producer::{
    BaseRecord, DefaultProducerContext, DeliveryResult, FutureProducer, FutureRecord, Producer,
    ProducerContext, ThreadedProducer,
};
use crate::types::RDKafkaRespErr;
use crate::util::Timeout;
use crate::{ClientContext, Message, Offset, Statistics, TopicPartitionList};

/// Like `MzClientContext`.
#[derive(Default)]
struct MzLikeContext {
    statistics_tx: Option<tokio::sync::watch::Sender<Statistics>>,
}

impl ClientContext for MzLikeContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        use RDKafkaLogLevel::*;
        if matches!(level, Emerg | Alert | Critical | Error) || fac == "FAIL" {
            let _ = log_message;
        }
    }

    fn stats(&self, statistics: Statistics) {
        if let Some(tx) = &self.statistics_tx {
            tx.send_replace(statistics);
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        let _ = format!("error: {}: {}", error, reason);
    }
}

impl ConsumerContext for MzLikeContext {}

impl ProducerContext for MzLikeContext {
    type DeliveryOpaque = <DefaultProducerContext as ProducerContext>::DeliveryOpaque;
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        DefaultProducerContext.delivery(delivery_result, delivery_opaque);
    }
}

/// Like `TunnelingClientContext`: blocks on another runtime in the blocking
/// callbacks and forwards everything else.
struct TunnelingLikeContext<C> {
    inner: C,
    runtime: tokio::runtime::Handle,
}

impl<C> ClientContext for TunnelingLikeContext<C>
where
    C: ClientContext,
{
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        let token = self.runtime.block_on(async { "token".to_string() });
        Ok(OAuthToken {
            token,
            lifetime_ms: 0,
            principal_name: "".to_string(),
        })
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, io::Error> {
        let addrs = self.runtime.block_on(async { Vec::new() });
        self.inner.log(
            RDKafkaLogLevel::Error,
            "FAIL",
            &format!("Broker address resolved to a private IP: {host}:{port}"),
        );
        Ok(addrs)
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }

    fn stats(&self, statistics: Statistics) {
        self.inner.stats(statistics)
    }
}

impl<C> ConsumerContext for TunnelingLikeContext<C>
where
    C: ConsumerContext,
{
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        self.inner.pre_rebalance(rebalance)
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        self.inner.post_rebalance(rebalance)
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        self.inner.commit_callback(result, offsets)
    }
}

impl<C> ProducerContext for TunnelingLikeContext<C>
where
    C: ProducerContext,
{
    type DeliveryOpaque = C::DeliveryOpaque;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        self.inner.delivery(delivery_result, delivery_opaque)
    }
}

type Ctx = TunnelingLikeContext<MzLikeContext>;

fn assert_send_sync<T: Send + Sync>() {}
fn assert_send<T: Send>(_: &T) {}

#[mz_ore::test]
fn auto_traits() {
    assert_send_sync::<BaseConsumer<Ctx>>();
    assert_send_sync::<Arc<BaseConsumer<Ctx>>>();
    assert_send_sync::<PartitionQueue<Ctx>>();
    assert_send_sync::<StreamConsumer>();
    assert_send_sync::<ThreadedProducer<Ctx>>();
    assert_send_sync::<FutureProducer<MzLikeContext>>();
    assert_send_sync::<AdminClient<Ctx>>();
    assert_send_sync::<Client<Ctx>>();
    assert_send_sync::<BorrowedMessage<'static>>();
    assert_send_sync::<OwnedMessage>();
    assert_send_sync::<OwnedHeaders>();
    assert_send_sync::<TopicPartitionList>();
    assert_send_sync::<KafkaError>();
    assert_send_sync::<Statistics>();
    assert_send_sync::<ClientConfig>();
    assert_send_sync::<crate::metadata::Metadata>();
    assert_send_sync::<AdminOptions>();
}

/// `mz_kafka_util::client::create_new_client_config`.
fn create_new_client_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.set("debug", "all");
    config.set("socket.keepalive.enable", "true");
    config.set(
        "socket.timeout.ms",
        Duration::from_secs(60).as_millis().to_string(),
    );
    config.set("socket.connection.setup.timeout.ms", "30000");
    config
}

/// `mz_kafka_util::client::{create, create_with_context}`.
fn create<T: FromClientConfig>(config: &ClientConfig) -> KafkaResult<T> {
    config.create()
}

fn create_with_context<C, T>(config: &ClientConfig, context: C) -> KafkaResult<T>
where
    C: ClientContext,
    T: FromClientConfigAndContext<C>,
{
    config.create_with_context(context)
}

/// `mz_kafka_util::client::get_partitions`.
fn get_partitions<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, String> {
    let meta = client
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| e.to_string())?;
    // NOTE: `RDKafkaErrorCode::from` is the identity here because
    // `RDKafkaRespErr` is an alias; Materialize's code still calls it.
    #[allow(clippy::useless_conversion)]
    fn check_err(err: Option<RDKafkaRespErr>) -> Result<(), String> {
        match err.map(RDKafkaErrorCode::from) {
            Some(RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::UnknownTopicOrPartition) => {
                Err("topic does not exist".into())
            }
            Some(code) => Err(code.to_string()),
            None => Ok(()),
        }
    }
    let meta_topic = &meta.topics()[0];
    check_err(meta_topic.error())?;
    let _ = meta_topic.name();
    let mut ids = Vec::new();
    for partition_meta in meta_topic.partitions() {
        check_err(partition_meta.error())?;
        ids.push(partition_meta.id());
        let _ = partition_meta.replicas().len();
    }
    let _ = meta.brokers()[0].id();
    Ok(ids)
}

/// The Kafka source reader.
fn source_reader(
    consumer: Arc<BaseConsumer<Ctx>>,
    topic: String,
    notify: Arc<dyn Fn() + Send + Sync>,
) -> KafkaResult<()> {
    while let Some(result) = consumer.poll(Duration::from_secs(0)) {
        if let Err(e) = result {
            let _ = format!("kafka error when polling consumer for source: {e}");
        }
    }

    // Partition assignment: full re-assign, then re-split every queue.
    let mut tpl = consumer.assignment().unwrap();
    let existing: Vec<_> = tpl
        .elements_for_topic(&topic)
        .iter()
        .map(|e| (e.topic().to_string(), e.partition(), e.offset()))
        .collect();
    let _ = existing;
    tpl.add_partition_offset(&topic, 3, Offset::Offset(10))?;
    consumer.assign(&tpl)?;
    let mut queues = Vec::new();
    for pid in 0..4 {
        let mut queue = consumer.split_partition_queue(&topic, pid).expect("queue");
        let notify = Arc::clone(&notify);
        queue.set_nonempty_callback(move || notify());
        queues.push(queue);
    }
    let _ctx: &Arc<Ctx> = consumer.context();

    for queue in &queues {
        for _ in 0..10_000 {
            match queue.poll(Duration::from_millis(0)) {
                Some(Ok(msg)) => construct_source_message(&msg),
                Some(Err(e)) => {
                    let _ = format!("error consuming from source: {e}");
                }
                None => break,
            }
        }
    }

    // Progress.
    let positions = consumer.position().unwrap();
    for elem in positions.elements_for_topic(&topic) {
        match elem.offset() {
            Offset::Offset(n) => {
                let _ = (elem.partition(), n);
            }
            Offset::Invalid => {}
            o => panic!("unexpected offset {o:?}"),
        }
    }

    // Offset commit, from a blocking task.
    let mut commit = TopicPartitionList::new();
    commit.add_partition_offset(&topic, 0, Offset::Offset(5))?;
    let committer = Arc::clone(&consumer);
    let _handle = mz_ore::task::spawn_blocking(
        || "commit",
        move || committer.commit(&commit, CommitMode::Sync),
    );

    // Watermarks.
    let partitions = get_partitions(consumer.client(), &topic, Duration::from_secs(10))
        .map_err(|_| KafkaError::Canceled)?;
    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for pid in partitions {
        tpl.add_partition_offset(&topic, pid, Offset::Beginning)?;
        tpl.add_partition_offset(&topic, pid, Offset::End)?;
    }
    let offsets = consumer.offsets_for_times(tpl, Duration::from_secs(10))?;
    for entry in offsets.elements() {
        match entry.offset() {
            Offset::Offset(offset) => {
                let _ = (entry.partition(), offset);
            }
            offset => panic!("unexpected {offset:?}"),
        }
    }
    let (_low, _high) = consumer.fetch_watermarks(&topic, 0, Duration::from_secs(10))?;
    let _metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
    Ok(())
}

fn construct_source_message(msg: &BorrowedMessage<'_>) {
    let _partition: i32 = msg.partition();
    let _offset: i64 = msg.offset();
    let _ts: Option<i64> = msg.timestamp().to_millis();
    let _key: Option<&[u8]> = msg.key();
    let _payload: Option<&[u8]> = msg.payload();
    let _topic: &str = msg.topic();
    if let Some(headers) = msg.headers() {
        let _last = headers
            .iter()
            .filter(|header| header.key == "k")
            .last()
            .map(|Header { key, value }| (key.to_string(), value.map(|v| v.to_vec())));
        let _count = headers.count();
    }
    let _owned: OwnedMessage = msg.detach();
}

/// The Kafka sink's transactional producer.
struct ProgressKey(String);

impl ToBytes for ProgressKey {
    fn to_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

fn sink(
    config: &ClientConfig,
    runtime: tokio::runtime::Handle,
    topic: &str,
    progress_topic: &str,
) -> KafkaResult<()> {
    let (tx, mut rx) = tokio::sync::watch::channel(Statistics::default());
    let ctx = TunnelingLikeContext {
        inner: MzLikeContext {
            statistics_tx: Some(tx),
        },
        runtime,
    };
    let producer: ThreadedProducer<Ctx> = create_with_context(config, ctx)?;
    let clone = producer.clone();
    let _handle = mz_ore::task::spawn_blocking(
        || "init",
        move || clone.init_transactions(Duration::from_secs(60)),
    );
    producer.begin_transaction()?;

    let key = vec![1u8];
    let value = vec![2u8];
    let ts = 5u64.to_le_bytes();
    let mut headers = OwnedHeaders::new().insert(Header {
        key: "materialize-timestamp",
        value: Some(&ts[..]),
    });
    let user_value = vec![3u8];
    headers = headers.insert(Header {
        key: "user",
        value: Some(&user_value),
    });
    let record = BaseRecord {
        topic,
        key: Some(&key),
        payload: Some(&value),
        headers: Some(headers),
        partition: Some(7 % 3),
        timestamp: None,
        delivery_opaque: (),
    };
    producer.send(record).map_err(|(e, _)| e)?;

    let progress_key = ProgressKey("mz-sink-1".into());
    let json = b"{}".to_vec();
    let progress = BaseRecord::to(progress_topic)
        .payload(&json)
        .key(&progress_key);
    producer.send(progress).map_err(|(e, _)| e)?;

    match producer.commit_transaction(Duration::from_secs(600)) {
        Ok(()) => {}
        Err(KafkaError::Transaction(err)) if err.txn_requires_abort() => {
            producer.abort_transaction(Duration::from_secs(60))?;
        }
        Err(KafkaError::Transaction(ref e))
            if e.is_retriable() && e.code() == RDKafkaErrorCode::OperationTimedOut => {}
        Err(e) => return Err(e),
    }
    let _ctx: &Arc<Ctx> = producer.context();

    let meta = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(10))?;
    let _count = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len());

    // Statistics.
    let stats = rx.borrow_and_update().clone();
    let _ = (
        stats.msg_cnt,
        stats.msg_size,
        stats.txmsgs,
        stats.txmsg_bytes,
        stats.tx,
    );
    let _ = stats.tx_bytes;
    let mut outbuf_cnt = 0;
    let mut connects = 0;
    for broker in stats.brokers.values() {
        outbuf_cnt += broker.outbuf_cnt + broker.outbuf_msg_cnt + broker.waitresp_cnt;
        outbuf_cnt += broker.waitresp_msg_cnt;
        let _ = (broker.txerrs, broker.txretries, broker.req_timeouts);
        connects += broker.connects.unwrap_or(0) + broker.disconnects.unwrap_or(0);
    }
    let _ = (outbuf_cnt, connects);
    Ok(())
}

/// The Kafka sink's progress search.
fn progress_search(
    consumer: &BaseConsumer<Ctx>,
    topic: &str,
    progress_key: &ProgressKey,
) -> Result<(), String> {
    let mut tps = TopicPartitionList::new();
    tps.add_partition(topic, 0);
    tps.set_partition_offset(topic, 0, Offset::Offset(10))
        .map_err(|e| e.to_string())?;
    consumer.assign(&tps).map_err(|e| e.to_string())?;
    let position = consumer
        .position()
        .map_err(|e| e.to_string())?
        .find_partition(topic, 0)
        .ok_or("missing")?
        .offset();
    match position {
        Offset::Offset(_) | Offset::Invalid => {}
        _ => return Err("unexpected".into()),
    }
    loop {
        match consumer.poll(Duration::from_secs(600)) {
            Some(Ok(message)) => {
                if message.key() != Some(progress_key.to_bytes()) {
                    continue;
                }
                let _ = message.payload();
            }
            Some(Err(KafkaError::PartitionEOF(_))) => continue,
            Some(Err(e)) => return Err(format!("failed to fetch progress message {e}")),
            None => return Ok(()),
        }
    }
}

/// `mz_kafka_util::admin` and `mz_storage_client::sink`.
async fn admin(client: &AdminClient<Ctx>, topic: &str) -> KafkaResult<()> {
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));

    let metadata = client
        .inner()
        .fetch_metadata(None, Some(Duration::from_secs(10)))?;
    let broker_id = metadata.brokers()[0].id();
    let (_low, _high) = client
        .inner()
        .fetch_watermarks(topic, 0, Duration::from_secs(10))?;

    let resources = [ResourceSpecifier::Broker(broker_id)];
    let fut = client.describe_configs(&resources, &opts);
    assert_send(&fut);
    for result in fut.await? {
        let ConfigResource { specifier, entries } = result.map_err(KafkaError::AdminOp)?;
        if let OwnedResourceSpecifier::Topic(name) = specifier {
            let _ = name;
        }
        for ConfigEntry {
            name,
            value,
            source,
            ..
        } in entries
        {
            let _ = (name, value, source != ConfigSource::DynamicTopic);
        }
    }

    let new_topic = NewTopic::new(topic, -1, TopicReplication::Fixed(-1))
        .set("cleanup.policy", "compact")
        .set("segment.bytes", "134217728");
    let _ = (new_topic.name, new_topic.num_partitions, &new_topic.config);
    let res = client.create_topics(iter::once(&new_topic), &opts).await?;
    match res.into_iter().next().unwrap() {
        Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
        Err((_, e)) => return Err(KafkaError::AdminOp(e)),
    }
    let _ = client
        .create_topics([&new_topic], &AdminOptions::new())
        .await?;

    let alter = AlterConfig::new(ResourceSpecifier::Topic(topic)).set("retention.ms", "-1");
    for result in client.alter_configs([&alter], &opts).await? {
        if let Err((_, code)) = result {
            return Err(KafkaError::AdminOp(code));
        }
    }

    let topic_names = [topic];
    let delete = client.delete_topics(&topic_names, &opts);
    assert_send(&delete);
    match delete.await?.into_iter().next().unwrap() {
        Ok(_) | Err((_, RDKafkaErrorCode::UnknownTopic)) => {}
        Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
        Err((_, e)) => return Err(KafkaError::AdminOp(e)),
    }

    let partitions = NewPartitions::new(topic, 4);
    for result in client
        .create_partitions(
            &[partitions],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(1))),
        )
        .await?
    {
        if let Err((_topic, code)) = result {
            let _ = code;
        }
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(3))?;
    let res = client.delete_records(&tpl, &opts).await?;
    let _ = res.count();
    res.elements()[0].error()?;
    Ok(())
}

/// testdrive's Kafka actions.
async fn testdrive(config: &ClientConfig, topic: &str) -> KafkaResult<()> {
    let mut config = config.clone();
    config.set("enable.auto.commit", "false");
    let producer: FutureProducer<MzLikeContext> =
        create_with_context(&config, MzLikeContext::default())?;
    let key = vec![1u8];
    let value = vec![2u8];
    let headers = OwnedHeaders::new().insert(Header {
        key: "h",
        value: Some(&[1u8][..]),
    });
    let record = FutureRecord::to(topic)
        .partition(0)
        .key(&key)
        .payload(&value)
        .timestamp(1)
        .headers(headers);
    let fut = producer.send(record, Duration::from_secs(1));
    assert_send(&fut);
    let res: Result<(i32, i64), (KafkaError, OwnedMessage)> = fut.await;
    let _ = res;
    let record: FutureRecord<'_, Vec<u8>, Vec<u8>> = FutureRecord::to(topic).payload(&value);
    if let Ok(delivery) = producer.send_result(record) {
        let _ = delivery.await;
    }
    let _ = producer
        .client()
        .fetch_metadata(None, Some(Duration::from_secs(1)))?;

    let consumer: StreamConsumer = create(&config)?;
    consumer.subscribe(&[topic])?;
    let next = consumer.stream().next().await;
    match next {
        Some(Ok(message)) => {
            consumer.store_offset_from_message(&message)?;
            if let Some(headers) = message.headers() {
                for header in headers.iter() {
                    let _ = (header.key, header.value);
                }
            }
        }
        Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition))) => {}
        Some(Err(e)) => return Err(e),
        None => {}
    }
    consumer.commit_consumer_state(CommitMode::Sync)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    let committed = consumer.committed_offsets(tpl, Duration::from_secs(10))?;
    let _ = committed.elements_for_topic(topic)[0].offset() == Offset::Offset(1);
    let _ = consumer.recv().await;

    let admin: AdminClient<DefaultClientContext> = create(&config)?;
    let _ = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(1))?
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect::<Vec<_>>();
    Ok(())
}

/// kgen.
fn kgen(config: &ClientConfig, topic: &str) {
    let producer: ThreadedProducer<MzLikeContext> =
        create_with_context(config, MzLikeContext::default()).unwrap();
    let buf = vec![0u8; 10];
    let mut rec = BaseRecord::to(topic).key(&buf).payload(&buf);
    rec = rec.partition(1);
    loop {
        match producer.send(rec) {
            Ok(()) => break,
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => rec = r,
            Err((e, _)) => panic!("{e}"),
        }
    }
    producer.flush(Timeout::Never).unwrap();
}

/// environmentd tests, the validation consumer and version reporting.
async fn env_tests() {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap();
    let t = NewTopic::new("foo", 1, TopicReplication::Fixed(1));
    match admin.create_topics([&t], &AdminOptions::new()).await {
        Ok(results) => match results.into_iter().next().unwrap() {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((_, e)) => panic!("{e}"),
        },
        Err(e) => panic!("{e}"),
    }
    let consumer: BaseConsumer<DefaultConsumerContext> =
        create(&create_new_client_config()).unwrap();
    let _ = consumer.fetch_metadata(None, Duration::from_secs(1));
    let _ = format!("librdkafka v{}", crate::version());
}

#[mz_ore::test]
fn creation_reports_config_errors() {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "localhost:9092")
        .set("foo", "bar");
    let err = create::<BaseConsumer>(&config).err().unwrap();
    assert_eq!(
        err.to_string(),
        "Client config error: No such configuration property: \"foo\" foo bar"
    );
    assert!(create::<StreamConsumer>(&config).is_err());
    assert!(create::<ThreadedProducer<DefaultProducerContext>>(&config).is_err());
    assert!(create::<FutureProducer>(&config).is_err());

    let mut config = ClientConfig::new();
    config
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "GSSAPI");
    let err = create_with_context::<_, AdminClient<_>>(&config, MzLikeContext::default())
        .err()
        .unwrap();
    assert!(
        err.to_string()
            .starts_with("Client creation error: No provider for SASL mechanism GSSAPI"),
        "{err}"
    );
}

#[mz_ore::test]
fn statistics_shape() {
    let stats = Statistics::default();
    let json = serde_json::to_string(&stats).unwrap();
    let stats: Statistics = serde_json::from_str(&json).unwrap();
    if let Some(topic) = stats.topics.get("t") {
        for (id, partition) in &topic.partitions {
            let _: (i32, i64) = (*id, partition.hi_offset);
        }
    }
    assert_eq!(crate::version(), "0.0.0");
}
