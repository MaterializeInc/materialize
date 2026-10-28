// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Low-level consumers.

use std::sync::Arc;

use crate::client::Client;
use crate::config::{
    ClientConfig, ClientKind, ClientSettings, FromClientConfig, FromClientConfigAndContext,
};
use crate::consumer::{CommitMode, Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::KafkaResult;
use crate::internal::consumer::{ConsumerInner, PartitionQueueInner};
use crate::internal::hooks::ConsumerHooks;
use crate::message::BorrowedMessage;
use crate::message::Message;
use crate::metadata::Metadata;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::Timeout;

/// A low-level consumer that requires manual polling.
///
/// The consumer is `Send + Sync` and is meant to be shared through an `Arc`,
/// which [`BaseConsumer::split_partition_queue`] requires.
pub struct BaseConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext + 'static,
{
    client: Client<C>,
    inner: Arc<ConsumerInner>,
}

impl FromClientConfig for BaseConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer> {
        BaseConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `BaseConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<BaseConsumer<C>> {
        let settings = ClientSettings::from_config(config, ClientKind::Consumer)?;
        let client = Client::new(settings, context)?;
        BaseConsumer::from_client(client)
    }
}

impl<C> BaseConsumer<C>
where
    C: ConsumerContext,
{
    /// Creates the consumer engine on top of a client.
    pub(crate) fn from_client(client: Client<C>) -> KafkaResult<BaseConsumer<C>> {
        let hooks: Arc<dyn ConsumerHooks> = Arc::<C>::clone(client.context());
        let inner = ConsumerInner::new(Arc::clone(client.cluster()), hooks);
        Ok(BaseConsumer { client, inner })
    }

    /// The consumer engine.
    pub(crate) fn inner(&self) -> &Arc<ConsumerInner> {
        &self.inner
    }

    /// Polls the consumer for new messages.
    ///
    /// It won't block for more than the specified timeout. Use zero
    /// `Duration` for non-blocking call. Returns `None` if the timeout
    /// expires before a message, an error or a consumer event is available.
    ///
    /// Returns messages and per-partition errors of partitions that are not
    /// split off with [`split_partition_queue`](Self::split_partition_queue).
    /// Partition ends are reported as [`KafkaError::PartitionEOF`] when
    /// `enable.partition.eof` is set.
    ///
    /// Also delivers pending [`ClientContext::error`] and
    /// [`ClientContext::stats`] callbacks on the calling thread, before
    /// returning and while waiting; they are not delivered otherwise.
    ///
    /// [`ClientContext::error`]: crate::ClientContext::error
    /// [`ClientContext::stats`]: crate::ClientContext::stats
    /// [`KafkaError::PartitionEOF`]: crate::error::KafkaError::PartitionEOF
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        self.inner.poll(timeout.into())
    }

    /// Splits messages for the specified partition into their own queue.
    ///
    /// If the `topic` or `partition` is invalid, returns `None`.
    ///
    /// After calling this method, newly-fetched messages for the specified
    /// partition will be returned via [`PartitionQueue::poll`] rather than
    /// [`BaseConsumer::poll`]. Note that there may be buffered messages for
    /// the specified partition that will continue to be returned by
    /// `BaseConsumer::poll`. For best results, call `split_partition_queue`
    /// before the first call to `BaseConsumer::poll`.
    ///
    /// The queue is tied to the current assignment: create it again after
    /// every [`Consumer::assign`].
    pub fn split_partition_queue(
        self: &Arc<Self>,
        topic: &str,
        partition: i32,
    ) -> Option<PartitionQueue<C>> {
        let queue = self.inner.split_partition_queue(topic, partition)?;
        Some(PartitionQueue {
            consumer: Arc::clone(self),
            queue,
        })
    }
}

impl<C> Consumer<C> for BaseConsumer<C>
where
    C: ConsumerContext,
{
    fn client(&self) -> &Client<C> {
        &self.client
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.inner.subscribe(topics)
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.inner.assign(assignment)
    }

    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        self.inner.commit(topic_partition_list, mode)
    }

    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        self.inner.commit_consumer_state(mode)
    }

    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()> {
        self.inner
            .store_offset(message.topic(), message.partition(), message.offset() + 1)
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        self.inner.assignment()
    }

    fn committed_offsets<T: Into<Timeout>>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        self.inner.committed_offsets(tpl, timeout.into())
    }

    fn offsets_for_times<T: Into<Timeout>>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList> {
        self.inner.offsets_for_times(timestamps, timeout.into())
    }

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.inner.position()
    }

    fn fetch_metadata<T: Into<Timeout>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        self.client.fetch_metadata(topic, timeout)
    }

    fn fetch_watermarks<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        self.client.fetch_watermarks(topic, partition, timeout)
    }
}

/// A message queue for a single partition.
pub struct PartitionQueue<C>
where
    C: ConsumerContext + 'static,
{
    consumer: Arc<BaseConsumer<C>>,
    queue: PartitionQueueInner,
}

impl<C> PartitionQueue<C>
where
    C: ConsumerContext,
{
    /// Polls the partition for new messages.
    ///
    /// The `timeout` parameter controls how long to block if no messages are
    /// available; a zero timeout never blocks.
    ///
    /// Control records and aborted transactional records are never returned,
    /// but polling past them advances [`Consumer::position`].
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        self.queue.poll(self.consumer.inner(), timeout.into())
    }

    /// Sets a callback that will be invoked whenever the queue becomes
    /// nonempty.
    ///
    /// The callback is edge-triggered and runs on the client's I/O threads; it
    /// must not block.
    pub fn set_nonempty_callback<F>(&mut self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.queue.set_nonempty_callback(Arc::new(f));
    }
}

impl<C> Drop for BaseConsumer<C>
where
    C: ConsumerContext + 'static,
{
    fn drop(&mut self) {
        // Leave the group before the client shuts down its connections.
        self.inner.close();
    }
}
