// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Future producer.
//!
//! A [`FutureProducer`] returns a future for every record, which resolves when
//! the delivery is acknowledged or fails.

use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures::channel::oneshot;

use crate::client::{Client, ClientContext, DefaultClientContext, OAuthToken};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use crate::internal::runtime;
use crate::message::{Message, OwnedHeaders, OwnedMessage, Timestamp, ToBytes};
use crate::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use crate::statistics::Statistics;
use crate::util::{IntoOpaque, Timeout};

/// A record for the future producer.
///
/// Like [`BaseRecord`], but specific to the [`FutureProducer`]. The only
/// difference is that the [`FutureRecord`] doesn't provide custom delivery
/// opaque object.
#[derive(Debug)]
pub struct FutureRecord<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> {
    /// Required destination topic.
    pub topic: &'a str,
    /// Optional destination partition.
    pub partition: Option<i32>,
    /// Optional payload.
    pub payload: Option<&'a P>,
    /// Optional key.
    pub key: Option<&'a K>,
    /// Optional timestamp.
    pub timestamp: Option<i64>,
    /// Optional message headers.
    pub headers: Option<OwnedHeaders>,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> FutureRecord<'a, K, P> {
    /// Creates a new record with the specified topic name.
    pub fn to(topic: &'a str) -> FutureRecord<'a, K, P> {
        FutureRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
        }
    }

    fn from_base_record<D: IntoOpaque>(
        base_record: BaseRecord<'a, K, P, D>,
    ) -> FutureRecord<'a, K, P> {
        FutureRecord {
            topic: base_record.topic,
            partition: base_record.partition,
            key: base_record.key,
            payload: base_record.payload,
            timestamp: base_record.timestamp,
            headers: base_record.headers,
        }
    }

    /// Sets the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> FutureRecord<'a, K, P> {
        self.partition = Some(partition);
        self
    }

    /// Sets the destination payload of the record.
    pub fn payload(mut self, payload: &'a P) -> FutureRecord<'a, K, P> {
        self.payload = Some(payload);
        self
    }

    /// Sets the destination key of the record.
    pub fn key(mut self, key: &'a K) -> FutureRecord<'a, K, P> {
        self.key = Some(key);
        self
    }

    /// Sets the destination timestamp of the record.
    pub fn timestamp(mut self, timestamp: i64) -> FutureRecord<'a, K, P> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> FutureRecord<'a, K, P> {
        self.headers = Some(headers);
        self
    }

    fn into_base_record<D: IntoOpaque>(self, delivery_opaque: D) -> BaseRecord<'a, K, P, D> {
        BaseRecord {
            topic: self.topic,
            partition: self.partition,
            key: self.key,
            payload: self.payload,
            timestamp: self.timestamp,
            headers: self.headers,
            delivery_opaque,
        }
    }
}

/// The [`ProducerContext`] used by the [`FutureProducer`].
///
/// This context will use a [`Future`] as its `DeliveryOpaque` and will
/// complete the future when the message is delivered (or failed to). All other
/// callbacks are forwarded to the wrapped context.
#[derive(Clone)]
pub struct FutureProducerContext<C: ClientContext + 'static> {
    wrapped_context: C,
}

/// Represents the result of message production as performed from the
/// `FutureProducer`.
///
/// If message delivery was successful, `OwnedDeliveryResult` will return the
/// partition and offset of the message. If the message failed to be delivered
/// an error will be returned, together with an owned copy of the original
/// message.
pub type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

impl<C: ClientContext + 'static> ClientContext for FutureProducerContext<C> {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.wrapped_context.stats(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.wrapped_context.error(error, reason);
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, io::Error> {
        self.wrapped_context.resolve_broker_addr(host, port)
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.wrapped_context
            .generate_oauth_token(oauthbearer_config)
    }
}

impl<C: ClientContext + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryOpaque = Box<oneshot::Sender<OwnedDeliveryResult>>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        tx: Box<oneshot::Sender<OwnedDeliveryResult>>,
    ) {
        let owned_delivery_result = match delivery_result {
            Ok(message) => Ok((message.partition(), message.offset())),
            Err((error, message)) => Err((error.clone(), message.detach())),
        };
        // The receiver may be gone if the caller dropped the future.
        let _ = tx.send(owned_delivery_result);
    }
}

/// A producer that returns a [`Future`] for every message being produced.
///
/// Since message production is asynchronous, the caller cannot immediately
/// know if the delivery of the message was successful or not. The
/// FutureProducer provides this information in a [`Future`], which will be
/// completed once the information becomes available.
///
/// It doesn't need to be polled, and can be cheaply cloned to get a reference
/// to the same underlying producer.
pub struct FutureProducer<C = DefaultClientContext>
where
    C: ClientContext + 'static,
{
    producer: Arc<ThreadedProducer<FutureProducerContext<C>>>,
}

impl<C> Clone for FutureProducer<C>
where
    C: ClientContext + 'static,
{
    fn clone(&self) -> FutureProducer<C> {
        FutureProducer {
            producer: Arc::clone(&self.producer),
        }
    }
}

impl FromClientConfig for FutureProducer<DefaultClientContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer<DefaultClientContext>> {
        FutureProducer::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C> FromClientConfigAndContext<C> for FutureProducer<C>
where
    C: ClientContext + 'static,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<FutureProducer<C>> {
        let future_context = FutureProducerContext {
            wrapped_context: context,
        };
        let threaded_producer = ThreadedProducer::from_config_and_context(config, future_context)?;
        Ok(FutureProducer {
            producer: Arc::new(threaded_producer),
        })
    }
}

/// A [`Future`] wrapping the result of the message production.
///
/// Once completed, the future will contain an `OwnedDeliveryResult` with
/// information on the delivery status of the message. If the producer is
/// dropped before the delivery status is received, the future will instead
/// resolve with [`oneshot::Canceled`].
pub struct DeliveryFuture {
    rx: oneshot::Receiver<OwnedDeliveryResult>,
}

impl Future for DeliveryFuture {
    type Output = Result<OwnedDeliveryResult, oneshot::Canceled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

impl<C> FutureProducer<C>
where
    C: ClientContext + 'static,
{
    /// Sends a message to Kafka, returning the result of the send.
    ///
    /// The `queue_timeout` parameter controls how long to retry for if the
    /// producer queue is full. Set it to `Timeout::Never` to retry forever or
    /// `Timeout::After(Duration::ZERO)` to never block. If the timeout is
    /// reached and the queue is still full, an
    /// [`RDKafkaErrorCode::QueueFull`] error will be reported in the
    /// [`OwnedDeliveryResult`].
    ///
    /// Keep in mind that `queue_timeout` only applies to the first phase of
    /// the send operation. Once the message is queued, the underlying
    /// producer will keep trying to send the message to Kafka until
    /// `message.timeout.ms` expires.
    ///
    /// The returned future works with any executor.
    pub async fn send<K, P, T>(
        &self,
        record: FutureRecord<'_, K, P>,
        queue_timeout: T,
    ) -> OwnedDeliveryResult
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
        T: Into<Timeout>,
    {
        let start_time = Instant::now();
        let queue_timeout = queue_timeout.into();
        let can_retry = || match queue_timeout {
            Timeout::Never => true,
            Timeout::After(t) => start_time.elapsed() < t,
        };

        let (tx, rx) = oneshot::channel();
        let mut base_record = record.into_base_record(Box::new(tx));

        loop {
            match self.producer.send(base_record) {
                Err((e, record))
                    if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        && can_retry() =>
                {
                    base_record = record;
                    runtime::sleep(Duration::from_millis(100)).await;
                }
                Ok(_) => {
                    break rx.await.expect("producer unexpectedly dropped");
                }
                Err((e, record)) => {
                    let owned_message = OwnedMessage::new(
                        record.payload.map(|p| p.to_bytes().to_vec()),
                        record.key.map(|k| k.to_bytes().to_vec()),
                        record.topic.to_owned(),
                        record
                            .timestamp
                            .map_or(Timestamp::NotAvailable, Timestamp::CreateTime),
                        record.partition.unwrap_or(-1),
                        0,
                        record.headers,
                    );
                    break Err((e, owned_message));
                }
            }
        }
    }

    /// Like [`FutureProducer::send`], but if enqueuing fails, an error will be
    /// returned immediately, alongside the [`FutureRecord`] provided.
    pub fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<DeliveryFuture, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let (tx, rx) = oneshot::channel();
        let base_record = record.into_base_record(Box::new(tx));
        self.producer
            .send(base_record)
            .map(|()| DeliveryFuture { rx })
            .map_err(|(e, record)| (e, FutureRecord::from_base_record(record)))
    }
}

impl<C> Producer<FutureProducerContext<C>> for FutureProducer<C>
where
    C: ClientContext + 'static,
{
    fn client(&self) -> &Client<FutureProducerContext<C>> {
        self.producer.client()
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.flush(timeout)
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.producer.begin_transaction()
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.abort_transaction(timeout)
    }
}
