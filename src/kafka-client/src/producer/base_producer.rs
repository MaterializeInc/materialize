// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Low-level producers.

use std::sync::Arc;

use crate::client::Client;
use crate::config::{
    ClientConfig, ClientKind, ClientSettings, FromClientConfig, FromClientConfigAndContext,
};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::producer::ProducerInner;
use crate::message::{OwnedHeaders, ToBytes};
use crate::producer::{DefaultProducerContext, Producer, ProducerContext};
use crate::util::{IntoOpaque, Timeout};

pub use crate::message::DeliveryResult;

/// A record for the [`ThreadedProducer`].
///
/// The `BaseRecord` is a structure that can be used to provide a new record
/// to [`ThreadedProducer::send`]. Since most fields are optional, a
/// `BaseRecord` can be constructed using the builder pattern.
///
/// A `None` key or payload is a Kafka null, distinct from an empty value. A
/// `None` partition selects one with the `partitioner` setting (librdkafka's
/// `consistent_random` by default). A `None` timestamp is the current time.
#[derive(Debug)]
pub struct BaseRecord<'a, K: ToBytes + ?Sized = (), P: ToBytes + ?Sized = (), D: IntoOpaque = ()> {
    /// Required destination topic.
    pub topic: &'a str,
    /// Optional destination partition.
    pub partition: Option<i32>,
    /// Optional payload.
    pub payload: Option<&'a P>,
    /// Optional key.
    pub key: Option<&'a K>,
    /// Optional timestamp.
    ///
    /// Note that Kafka represents timestamps as the number of milliseconds
    /// since the Unix epoch.
    pub timestamp: Option<i64>,
    /// Optional message headers.
    pub headers: Option<OwnedHeaders>,
    /// Required delivery opaque (defaults to `()` if not required).
    pub delivery_opaque: D,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized, D: IntoOpaque> BaseRecord<'a, K, P, D> {
    /// Creates a new record with the specified topic name and delivery opaque.
    pub fn with_opaque_to(topic: &'a str, delivery_opaque: D) -> BaseRecord<'a, K, P, D> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque,
        }
    }

    /// Sets the destination partition of the record.
    pub fn partition(mut self, partition: i32) -> BaseRecord<'a, K, P, D> {
        self.partition = Some(partition);
        self
    }

    /// Sets the payload of the record.
    pub fn payload(mut self, payload: &'a P) -> BaseRecord<'a, K, P, D> {
        self.payload = Some(payload);
        self
    }

    /// Sets the key of the record.
    pub fn key(mut self, key: &'a K) -> BaseRecord<'a, K, P, D> {
        self.key = Some(key);
        self
    }

    /// Sets the timestamp of the record.
    ///
    /// Note that Kafka represents timestamps as the number of milliseconds
    /// since the Unix epoch.
    pub fn timestamp(mut self, timestamp: i64) -> BaseRecord<'a, K, P, D> {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the headers of the record.
    pub fn headers(mut self, headers: OwnedHeaders) -> BaseRecord<'a, K, P, D> {
        self.headers = Some(headers);
        self
    }
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> BaseRecord<'a, K, P, ()> {
    /// Creates a new record with the specified topic name.
    pub fn to(topic: &'a str) -> BaseRecord<'a, K, P, ()> {
        BaseRecord {
            topic,
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
            delivery_opaque: (),
        }
    }
}

/// A producer whose delivery reports are processed in the background.
///
/// Clones share the same underlying producer. Dropping the last clone does
/// not flush: queued messages are discarded and an open transaction is left
/// for the broker to time out, as in rust-rdkafka.
pub struct ThreadedProducer<C>
where
    C: ProducerContext + 'static,
{
    client: Arc<Client<C>>,
    inner: Arc<ProducerInner<C>>,
}

impl<C: ProducerContext + 'static> Clone for ThreadedProducer<C> {
    fn clone(&self) -> Self {
        ThreadedProducer {
            client: Arc::clone(&self.client),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl FromClientConfig for ThreadedProducer<DefaultProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<ThreadedProducer<DefaultProducerContext>> {
        ThreadedProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C> FromClientConfigAndContext<C> for ThreadedProducer<C>
where
    C: ProducerContext + 'static,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<ThreadedProducer<C>> {
        let settings = ClientSettings::from_config(config, ClientKind::Producer)?;
        let client = Client::new(settings, context)?;
        ThreadedProducer::from_client(client)
    }
}

impl<C> ThreadedProducer<C>
where
    C: ProducerContext + 'static,
{
    /// Creates the producer engine on top of a client.
    pub(crate) fn from_client(client: Client<C>) -> KafkaResult<ThreadedProducer<C>> {
        let inner = ProducerInner::new(Arc::clone(client.cluster()), Arc::clone(client.context()));
        Ok(ThreadedProducer {
            client: Arc::new(client),
            inner,
        })
    }

    /// The producer engine.
    pub(crate) fn inner(&self) -> &Arc<ProducerInner<C>> {
        &self.inner
    }

    /// Sends a message to Kafka.
    ///
    /// Message fields such as key, payload, partition, timestamp etc. are
    /// provided to this method via a [`BaseRecord`]. If the message is
    /// correctly enqueued in the producer's memory buffer, the method will take
    /// ownership of the record and return immediately; in case of failure to
    /// enqueue, the original record is returned, alongside an error code. If
    /// the message fails to be produced after being enqueued in the buffer,
    /// the [`ProducerContext::delivery`] method will be called asynchronously,
    /// with the provided [`ProducerContext::DeliveryOpaque`].
    ///
    /// Never blocks. Fails with `MessageProduction(QueueFull)` when the queue
    /// limits are reached, `MessageProduction(MessageSizeTooLarge)` for
    /// records over `message.max.bytes`, `MessageProduction(UnknownPartition)`
    /// for an explicit partition the topic does not have,
    /// `MessageProduction(State)` outside a transaction of a transactional
    /// producer, and `MessageProduction(Fatal)` after a fatal error.
    pub fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.inner.send(record)
    }
}

impl<C> Producer<C> for ThreadedProducer<C>
where
    C: ProducerContext + 'static,
{
    fn client(&self) -> &Client<C> {
        &self.client
    }

    fn in_flight_count(&self) -> i32 {
        self.inner.in_flight_count()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.inner.flush(timeout.into())
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.inner.init_transactions(timeout.into())
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.inner.begin_transaction()
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.inner.commit_transaction(timeout.into())
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.inner.abort_transaction(timeout.into())
    }
}
