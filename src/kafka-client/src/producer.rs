// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka producers.
//!
//! [`ThreadedProducer`] enqueues records without blocking and reports their
//! delivery to [`ProducerContext::delivery`]. [`FutureProducer`] wraps it and
//! returns a future per record. Unlike rust-rdkafka, neither needs a polling
//! thread: delivery callbacks run on the client's I/O threads.

use std::sync::Arc;

use crate::client::{Client, ClientContext};
use crate::error::KafkaResult;
use crate::util::{IntoOpaque, Timeout};

pub mod base_producer;
pub mod future_producer;

#[doc(inline)]
pub use self::base_producer::{BaseRecord, DeliveryResult, ThreadedProducer};
#[doc(inline)]
pub use self::future_producer::{DeliveryFuture, FutureProducer, FutureRecord};

/// Producer-specific context.
///
/// This user-defined object can be used to provide custom callbacks for
/// producer events. Refer to the list of methods to check which callbacks can
/// be specified.
///
/// In particular, it can be used to specify the `delivery` callback that will
/// be called when the acknowledgement for a delivered message is received.
pub trait ProducerContext: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque;

    /// This method will be called once the message has been delivered (or
    /// failed to). The `DeliveryOpaque` will be the one provided by the user
    /// when calling send.
    ///
    /// Called inline from the client's I/O tasks; must not block.
    fn delivery(&self, delivery_result: &DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque);
}

/// An inert producer context that can be used when customizations are not
/// required.
#[derive(Clone)]
pub struct DefaultProducerContext;

impl ClientContext for DefaultProducerContext {}

impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

/// Common trait for all producers.
///
/// The transaction methods and `flush` block the calling thread. They may be
/// called from any thread except the client's own I/O threads, including tokio
/// worker threads, which they block.
pub trait Producer<C = DefaultProducerContext>
where
    C: ProducerContext + 'static,
{
    /// Returns the [`Client`] underlying this producer.
    fn client(&self) -> &Client<C>;

    /// Returns a reference to the [`ProducerContext`] used to create this
    /// producer.
    fn context(&self) -> &Arc<C> {
        self.client().context()
    }

    /// Returns the number of messages that are either waiting to be sent or
    /// are sent but are waiting to be acknowledged.
    fn in_flight_count(&self) -> i32;

    /// Flushes any pending messages.
    ///
    /// This method should be called before termination to ensure delivery of
    /// all enqueued messages. Fails with `Flush(OperationTimedOut)` if the
    /// timeout expires first.
    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Initialize transactions for the producer instance.
    ///
    /// This function ensures any transactions initiated by previous instances
    /// of the producer with the same `transactional.id` are completed. If the
    /// previous instance failed with a transaction in progress the previous
    /// transaction will be aborted.
    ///
    /// This function needs to be called before any other transactional or
    /// produce functions are called when the `transactional.id` is
    /// configured.
    ///
    /// Errors are [`KafkaError::Transaction`](crate::error::KafkaError::Transaction),
    /// classified with `is_retriable`, `txn_requires_abort` and `is_fatal`.
    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Begins a new transaction.
    ///
    /// This function should be called exactly once before each new
    /// transaction. It is local and does not contact the cluster.
    fn begin_transaction(&self) -> KafkaResult<()>;

    /// Commits the current transaction.
    ///
    /// Any outstanding messages will be flushed (delivered) before actually
    /// committing the transaction.
    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;

    /// Aborts the current transaction.
    ///
    /// Any outstanding messages will be purged and fail with
    /// `PurgeInflight` or `PurgeQueue`.
    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()>;
}
