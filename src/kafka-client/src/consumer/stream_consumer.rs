// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! High-level consumers with a [`Stream`] interface.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};

use crate::client::Client;
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext,
};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::consumer::ConsumerInner;
use crate::message::BorrowedMessage;
use crate::metadata::Metadata;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::Timeout;

/// A stream of messages from a [`StreamConsumer`].
///
/// See the documentation of [`StreamConsumer::stream`] for details.
pub struct MessageStream<'a> {
    inner: &'a ConsumerInner,
}

impl<'a> Stream for MessageStream<'a> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.inner;
        if let Some(result) = inner.poll(Timeout::After(Duration::ZERO)) {
            return Poll::Ready(Some(result));
        }
        // Register, then poll again, so that data arriving in between is not
        // missed.
        inner.register_waker(cx.waker());
        match inner.poll(Timeout::After(Duration::ZERO)) {
            Some(result) => Poll::Ready(Some(result)),
            None => Poll::Pending,
        }
    }
}

/// A high-level consumer with a [`Stream`] interface.
///
/// This consumer doesn't need to be polled explicitly. Extracting an item from
/// the stream returned by [`stream`](StreamConsumer::stream) will
/// implicitly poll the underlying consumer. Unlike rust-rdkafka's, it is not
/// generic over an async runtime and works with any executor.
pub struct StreamConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext + 'static,
{
    base: Arc<BaseConsumer<C>>,
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Creates a new `StreamConsumer` starting from a [`ClientConfig`].
impl<C> FromClientConfigAndContext<C> for StreamConsumer<C>
where
    C: ConsumerContext + 'static,
{
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<Self> {
        let base = BaseConsumer::from_config_and_context(config, context)?;
        // librdkafka has no consumer queue without a group.
        if !base.inner().has_group() {
            return Err(KafkaError::ClientCreation(
                "librdkafka failed to create consumer queue".into(),
            ));
        }
        Ok(StreamConsumer {
            base: Arc::new(base),
        })
    }
}

impl<C> StreamConsumer<C>
where
    C: ConsumerContext + 'static,
{
    /// Constructs a stream that yields messages from this consumer.
    ///
    /// It is legal to have multiple live message streams for the same
    /// consumer, and to move those message streams across threads. Note,
    /// however, that the message streams share the same underlying state. A
    /// message received by the consumer will be delivered to only one of the
    /// live message streams. If you seek the underlying consumer, all message
    /// streams created from the consumer will begin to draw messages from the
    /// new position of the consumer.
    ///
    /// The stream never terminates.
    pub fn stream(&self) -> MessageStream<'_> {
        MessageStream {
            inner: self.base.inner(),
        }
    }

    /// Receives the next message from the stream.
    ///
    /// This method will block until the next message is available or an
    /// error occurs. It is legal to call `recv` from multiple threads
    /// simultaneously.
    pub async fn recv(&self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.stream()
            .next()
            .await
            .expect("kafka streams never terminate")
    }
}

impl<C> Consumer<C> for StreamConsumer<C>
where
    C: ConsumerContext,
{
    fn client(&self) -> &Client<C> {
        self.base.client()
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.base.subscribe(topics)
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.base.assign(assignment)
    }

    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        self.base.commit(topic_partition_list, mode)
    }

    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        self.base.commit_consumer_state(mode)
    }

    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()> {
        self.base.store_offset_from_message(message)
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        self.base.assignment()
    }

    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
    {
        self.base.committed_offsets(tpl, timeout)
    }

    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.offsets_for_times(timestamps, timeout)
    }

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.base.position()
    }

    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.fetch_metadata(topic, timeout)
    }

    fn fetch_watermarks<T>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.base.fetch_watermarks(topic, partition, timeout)
    }
}
