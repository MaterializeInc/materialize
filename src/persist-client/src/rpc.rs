// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use futures_util::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::Endpoint;
use tonic::{Extensions, Request};
use tracing::{error, info, warn};

use mz_persist::location::VersionedData;
use mz_proto::RustType;

use crate::cache::PersistClientCache;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::proto_pub_sub_message::Message;
use crate::internal::service::{
    PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe, ProtoUnsubscribe,
};
use crate::ShardId;

/// WIP
#[async_trait]
pub trait PersistPubSub {
    /// Receive handles with which to push and subscribe to diffs.
    async fn connect(
        addr: String,
        caller_id: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error>;
}

/// The send-side client to Persist PubSub.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push(&self, shard_id: &ShardId, diff: &VersionedData);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already subscribed shards.
    fn subscribe(&self, shard: &ShardId);

    /// Unsubscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already unsubscribed shards.
    fn unsubscribe(&self, shard: &ShardId);
}

/// The receive-side client to Persist PubSub.
pub trait PubSubReceiver: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

impl<T> PubSubReceiver for T where T: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

#[derive(Debug)]
pub struct PersistPubSubServer {
    service: PersistService,
}

impl PersistPubSubServer {
    pub fn new(cache: &PersistClientCache) -> Self {
        let service = PersistService::new(Arc::clone(&cache.state_cache));
        PersistPubSubServer { service }
    }

    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self.service))
            .serve(listen_addr)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct PubSubSenderClient {
    requests: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
}

#[async_trait]
impl PubSubSender for PubSubSenderClient {
    fn push(&self, shard_id: &ShardId, diff: &VersionedData) {
        let seqno = diff.seqno.clone();
        let diff = ProtoPushDiff {
            shard_id: shard_id.into_proto(),
            seqno: diff.seqno.into_proto(),
            diff: diff.data.clone(),
        };
        // WIP: counters
        match self.requests.send(ProtoPubSubMessage {
            message: Some(Message::PushDiff(diff)),
        }) {
            Ok(_) => {
                info!("pushed ({}, {})", shard_id, seqno);
            }
            Err(err) => {
                error!("{}", err);
            }
        }
    }

    fn subscribe(&self, shard: &ShardId) {
        match self.requests.send(ProtoPubSubMessage {
            message: Some(Message::Subscribe(ProtoSubscribe {
                shard: shard.to_string(),
            })),
        }) {
            // WIP: counters
            Ok(_) => {
                info!("subscribed to {}", shard);
            }
            Err(err) => {
                error!("error subscribing to {}: {}", shard, err);
            }
        }
    }

    fn unsubscribe(&self, shard: &ShardId) {
        match self.requests.send(ProtoPubSubMessage {
            message: Some(Message::Unsubscribe(ProtoUnsubscribe {
                shard: shard.to_string(),
            })),
        }) {
            // WIP: counters
            Ok(_) => {
                info!("unsubscribed from {}", shard);
            }
            Err(err) => {
                error!("error unsubscribing from {}: {}", shard, err);
            }
        }
    }
}

pub const PERSIST_PUBSUB_CALLER_KEY: &str = "persist-pubsub-caller-id";

/// A [PersistPubSub] implementation backed by gRPC.
#[derive(Debug)]
pub struct PersistPubSubClient;

#[async_trait]
impl PersistPubSub for PersistPubSubClient {
    async fn connect(
        addr: String,
        caller_id: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error> {
        // WIP: connect with retries and a timeout
        let mut client = ProtoPersistPubSubClient::connect(addr.clone()).await?;
        info!("Created pubsub client to: {:?}", addr);
        // WIP not unbounded.
        let (requests, responses) = tokio::sync::mpsc::unbounded_channel();
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY),
            AsciiMetadataValue::try_from(caller_id)
                .unwrap_or_else(|_| AsciiMetadataValue::from_static("unknown")),
        );
        let pubsub_request = Request::from_parts(
            metadata,
            Extensions::default(),
            UnboundedReceiverStream::new(responses),
        );
        let responses = client.pub_sub(pubsub_request).await?;

        let sender = PubSubSenderClient { requests };
        let receiver = responses
            .into_inner()
            .filter_map(|res| async move {
                match res {
                    Ok(message) => Some(message),
                    Err(err) => {
                        // WIP: metric coverage here
                        warn!("pubsub client received err: {:?}", err);
                        None
                    }
                }
            })
            .boxed();

        Ok((Arc::new(sender), Box::new(receiver)))
    }
}
