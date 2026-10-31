// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka consumers.

use std::sync::Arc;

use crate::client::{Client, ClientContext};
use crate::error::KafkaResult;
use crate::message::BorrowedMessage;
use crate::metadata::Metadata;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::Timeout;

pub mod base_consumer;
pub mod stream_consumer;

#[doc(inline)]
pub use self::base_consumer::BaseConsumer;
#[doc(inline)]
pub use self::stream_consumer::{MessageStream, StreamConsumer};

/// Rebalance information.
#[derive(Clone, Debug)]
pub enum Rebalance<'a> {
    /// A new partition assignment is received.
    Assign(&'a TopicPartitionList),
    /// A new partition revocation is received.
    Revoke(&'a TopicPartitionList),
    /// Unexpected error from Kafka.
    Error(String),
}

/// Consumer-specific context.
///
/// This user-defined object can be used to provide custom callbacks for
/// consumer events. Refer to the list of methods to check which callbacks can
/// be specified. [`ClientContext`] documents the threads that call them.
///
/// NOTE: Unlike rust-rdkafka, there is no `rebalance` method taking the native
/// client and no `main_queue_min_poll_interval`: the client applies
/// assignments itself and calls [`pre_rebalance`](Self::pre_rebalance) and
/// [`post_rebalance`](Self::post_rebalance) around them.
pub trait ConsumerContext: ClientContext + Sized {
    /// Pre-rebalance callback. This method will run before the rebalance and
    /// should terminate its execution quickly.
    fn pre_rebalance(&self, _rebalance: &Rebalance<'_>) {}

    /// Post-rebalance callback. This method will run after the rebalance and
    /// should terminate its execution quickly.
    fn post_rebalance(&self, _rebalance: &Rebalance<'_>) {}

    /// Post commit callback. This method will run after a group of offsets was
    /// committed to the offset store.
    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {}
}

/// An inert [`ConsumerContext`] that can be used when no customizations are
/// needed.
#[derive(Clone, Debug, Default)]
pub struct DefaultConsumerContext;

impl ClientContext for DefaultConsumerContext {}
impl ConsumerContext for DefaultConsumerContext {}

/// Specifies whether a commit should be performed synchronously or
/// asynchronously.
///
/// A commit is performed via [`Consumer::commit`] or one of its variants.
///
/// Regardless of the `CommitMode`, the commit request is sent by the client's
/// I/O tasks. The difference between [`CommitMode::Sync`] and
/// [`CommitMode::Async`] is in whether the caller waits for the Kafka broker to
/// respond that it finished handling the commit request.
#[derive(Clone, Copy, Debug)]
pub enum CommitMode {
    /// In `Sync` mode, the caller blocks until the Kafka broker finishes
    /// processing the commit request.
    Sync = 0,
    /// In `Async` mode, the caller enqueues the commit request in a local
    /// work queue and returns immediately.
    Async = 1,
}

/// Common trait for all consumers.
///
/// Blocking methods (`commit` in `Sync` mode, `committed_offsets`,
/// `offsets_for_times`, `fetch_metadata`, `fetch_watermarks`) may be called
/// from any thread except the client's own I/O threads, including tokio worker
/// threads, which they block.
pub trait Consumer<C = DefaultConsumerContext>
where
    C: ConsumerContext + 'static,
{
    /// Returns the [`Client`] underlying this consumer.
    fn client(&self) -> &Client<C>;

    /// Returns a reference to the [`ConsumerContext`] used to create this
    /// consumer.
    fn context(&self) -> &Arc<C> {
        self.client().context()
    }

    /// Subscribes the consumer to a list of topics, joining the consumer
    /// group given by `group.id`.
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;

    /// Manually assigns topics and partitions to the consumer, replacing the
    /// previous assignment.
    ///
    /// Each element's offset is the start position: `Offset(n)` starts at
    /// `n`, `Beginning`/`End`/`OffsetTail` are resolved with ListOffsets, and
    /// `Stored`/`Invalid` use the committed offset or `auto.offset.reset`.
    /// Partitions that stay assigned restart from the given offsets. Split
    /// partition queues must be created again after every call.
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()>;

    /// Commits the offsets in `topic_partition_list` to the group given by
    /// `group.id`.
    ///
    /// Works without group membership (with generation -1). In `Sync` mode,
    /// blocks until the coordinator answers, without a timeout.
    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()>;

    /// Commits the current consumer state, i.e. the offsets in the offset
    /// store.
    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()>;

    /// Stores offset to be used on the next (auto)commit. When using this
    /// `enable.auto.offset.store` should be set to `false` in the config.
    fn store_offset_from_message(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()>;

    /// Returns the current partition assignment.
    fn assignment(&self) -> KafkaResult<TopicPartitionList>;

    /// Retrieves the committed offsets for topics and partitions.
    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>;

    /// Looks up the offsets for the specified partitions by timestamp.
    ///
    /// Each element's offset is the timestamp to look up. As in librdkafka,
    /// the logical offsets [`Offset::Beginning`](crate::Offset::Beginning)
    /// (-2) and [`Offset::End`](crate::Offset::End) (-1) are passed through
    /// as ListOffsets timestamps, yielding the earliest and the latest
    /// offsets. A timestamp with no later message yields `Offset::End`.
    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Retrieves the current positions (offsets) for topics and partitions:
    /// for every assigned partition, the offset after the last message or
    /// control record returned to the application, or `Offset::Invalid` if
    /// none was returned since it was assigned.
    fn position(&self) -> KafkaResult<TopicPartitionList>;

    /// Returns the metadata information for the specified topic, or for all
    /// topics in the cluster if no topic is specified.
    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized;

    /// Returns the low and high watermarks for a specific topic and partition.
    fn fetch_watermarks<T>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)>
    where
        T: Into<Timeout>,
        Self: Sized;
}
