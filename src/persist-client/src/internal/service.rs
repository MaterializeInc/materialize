// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP
#![allow(clippy::clone_on_ref_ptr, clippy::disallowed_methods)] // Generated code does this

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::AsciiMetadataKey;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, info_span, warn};

use mz_persist::location::VersionedData;
use mz_proto::ProtoType;

use crate::cache::StateCache;
use crate::internal::service::proto_pub_sub_message::Message;
use crate::rpc::PERSIST_PUBSUB_CALLER_KEY;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.service.rs"
));

#[derive(Debug)]
pub struct PersistService {
    /// Assigns a unique ID to each incoming connection.
    connection_id_counter: AtomicUsize,
    /// Maintains a mapping of `ShardId --> [ConnectionId -> Tx]`.
    shard_subscribers: Arc<
        RwLock<
            BTreeMap<String, BTreeMap<usize, UnboundedSender<Result<ProtoPubSubMessage, Status>>>>,
        >,
    >,
    state_cache: Arc<StateCache>,
}

impl PersistService {
    pub fn new(state_cache: Arc<StateCache>) -> Self {
        PersistService {
            shard_subscribers: Default::default(),
            connection_id_counter: AtomicUsize::new(0),
            state_cache,
        }
    }
}

#[async_trait]
impl proto_persist_pub_sub_server::ProtoPersistPubSub for PersistService {
    type PubSubStream = Pin<Box<dyn Stream<Item = Result<ProtoPubSubMessage, Status>> + Send>>;

    async fn pub_sub(
        &self,
        req: Request<Streaming<ProtoPubSubMessage>>,
    ) -> Result<Response<Self::PubSubStream>, Status> {
        let root_span = info_span!("persist::push::server");
        let _guard = root_span.enter();
        let caller_id = req
            .metadata()
            .get(AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY))
            .map(|key| key.to_str().ok())
            .flatten()
            .map(|key| key.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        info!("incoming push from: {:?}", caller_id);

        let mut in_stream = req.into_inner();

        // WIP not unbounded
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);

        let subscribers = Arc::clone(&self.shard_subscribers);
        let state_cache = Arc::clone(&self.state_cache);
        // this spawn here to cleanup after connection error / disconnect, otherwise the stream
        // would not be polled after the connection drops. in our case, we want to clear the
        // connection and its subscriptions from our shared state when it drops.
        tokio::spawn(async move {
            let root_span = info_span!("persist::push::server_conn");
            let _guard = root_span.enter();
            let mut current_subscriptions = BTreeSet::new();

            while let Some(result) = in_stream.next().await {
                let req = match result {
                    Ok(req) => req,
                    Err(err) => {
                        warn!("pubsub connection err: {}", err);
                        break;
                    }
                };

                match req.message {
                    None => {}
                    Some(proto_pub_sub_message::Message::PushDiff(req)) => {
                        let diff = VersionedData {
                            seqno: req.seqno.into_rust().expect("WIP"),
                            data: req.diff.clone(),
                        };

                        {
                            let subscribers = subscribers.read().expect("lock poisoned");
                            match subscribers.get(&req.shard_id) {
                                None => {}
                                Some(subscribed_connections) => {
                                    for (subscribed_conn_id, tx) in subscribed_connections {
                                        // skip sending the diff back to the original sender
                                        if *subscribed_conn_id == connection_id {
                                            continue;
                                        }
                                        info!(
                                            "server forwarding req to {} conns {} {} {}",
                                            caller_id,
                                            &req.shard_id,
                                            diff.seqno,
                                            diff.data.len()
                                        );
                                        let req = ProtoPubSubMessage {
                                            message: Some(Message::PushDiff(req.clone())),
                                        };
                                        let _ = tx.send(Ok(req));
                                    }
                                }
                            }
                        }

                        // also apply it locally
                        // WIP: just eat the cost and use ShardId everywhere?
                        let shard_id = req.shard_id.parse().expect("WIP");
                        state_cache.push_diff(&shard_id, diff);
                    }
                    Some(proto_pub_sub_message::Message::Subscribe(diff)) => {
                        info!("conn {} adding subscription to {}", caller_id, diff.shard);
                        let mut subscribed_shards = subscribers.write().expect("lock poisoned");
                        subscribed_shards
                            .entry(diff.shard.clone())
                            .or_default()
                            .insert(connection_id, tx.clone());
                        current_subscriptions.insert(diff.shard);
                    }
                    Some(proto_pub_sub_message::Message::Unsubscribe(diff)) => {
                        info!(
                            "conn {} removing subscription from {}",
                            caller_id, diff.shard
                        );
                        let mut subscribed_shards = subscribers.write().expect("lock poisoned");
                        if let Some(subscribed_connections) = subscribed_shards.get_mut(&diff.shard)
                        {
                            subscribed_connections.remove(&connection_id);
                        }
                        current_subscriptions.remove(&diff.shard);
                    }
                }
            }

            {
                let mut subscribers = subscribers.write().expect("lock poisoned");
                for shard_id in current_subscriptions {
                    if let Some(subscribed_connections) = subscribers.get_mut(&shard_id) {
                        subscribed_connections.remove(&connection_id);
                    }
                }
            }

            info!("push stream to {} ended", caller_id);
        });

        let out_stream: Self::PubSubStream = Box::pin(UnboundedReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}
