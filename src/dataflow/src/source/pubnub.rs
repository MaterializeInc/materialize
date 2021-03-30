// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::StreamExt;
use pubnub_hyper::core::data::message::Type;
use pubnub_hyper::{Builder, DefaultRuntime, DefaultTransport};
use tokio::sync::mpsc::{self, Receiver};

use crate::source::{
    DataEncoding, ExternalSourceConnector, Logger, MzOffset, NextMessage, PartitionId,
    SourceInstanceId, SourceMessage, SourceReader, SyncActivator,
};

/// Information required to sync data from PubNub
pub struct PubNubSourceReader {
    receiver: Receiver<SourceMessage<Vec<u8>>>,
    activator: Arc<SyncActivator>,
}

impl SourceReader<Vec<u8>> for PubNubSourceReader {
    fn new(
        _name: String,
        _source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _: DataEncoding,
        _: Option<Logger>,
    ) -> Result<(PubNubSourceReader, Option<PartitionId>), anyhow::Error> {
        let pubnub_conn = match connector {
            ExternalSourceConnector::PubNub(pubnub_conn) => pubnub_conn,
            _ => panic!("Unsupported ExternalSourceConnector for PubNubSourceReader"),
        };
        log::debug!("creating PubNubSourceReader worker_id={}", worker_id);

        let (tx, rx) = mpsc::channel(1024);

        let transport = DefaultTransport::new()
            // we don't need a publish key for subscribing
            .publish_key("")
            .subscribe_key(&pubnub_conn.subscribe_key)
            .build()
            .map_err(anyhow::Error::msg)?;

        let channel = pubnub_conn
            .channel
            .try_into()
            .or_else(|c| Err(anyhow!("invalid channel name: {}", c)))?;

        let mut pubnub = Builder::new()
            .transport(transport)
            .runtime(DefaultRuntime)
            .build();

        tokio::spawn(async move {
            let stream = pubnub.subscribe(channel).await;
            tokio::pin!(stream);

            while let Some(msg) = stream.next().await {
                if msg.message_type == Type::Publish {
                    let record = msg.json.dump().into_bytes();
                    let offset = MzOffset {
                        offset: msg.timetoken.t as i64,
                    };

                    let message = SourceMessage {
                        partition: PartitionId::File,
                        offset: offset,
                        upstream_time_millis: None,
                        key: None,
                        payload: Some(record),
                    };
                    if let Err(_) = tx.send(message).await {
                        // Source is no longer active
                        return;
                    }
                }
            }
        });

        Ok((
            PubNubSourceReader {
                receiver: rx,
                activator: Arc::new(consumer_activator),
            },
            Some(PartitionId::File),
        ))
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Vec<u8>>, anyhow::Error> {
        let waker = futures::task::waker_ref(&self.activator);
        let mut context = Context::from_waker(&waker);

        match self.receiver.poll_recv(&mut context) {
            Poll::Ready(Some(message)) => Ok(NextMessage::Ready(message)),
            Poll::Ready(None) => Ok(NextMessage::Finished),
            Poll::Pending => Ok(NextMessage::Pending),
        }
    }
}
