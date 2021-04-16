// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use pubnub_hyper::core::data::{channel, message::Type};
use pubnub_hyper::{Builder, DefaultRuntime, DefaultTransport};

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::PubNubSourceConnector;
use repr::{Datum, Row};

/// Information required to sync data from PubNub
pub struct PubNubSourceReader {
    connector: PubNubSourceConnector,
}

impl PubNubSourceReader {
    /// Constructs a new instance
    pub fn new(connector: PubNubSourceConnector) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl SimpleSource for PubNubSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        let transport = DefaultTransport::new()
            // we don't need a publish key for subscribing
            .publish_key("")
            .subscribe_key(&self.connector.subscribe_key)
            .build()
            .map_err(SourceError::FileIO)?;

        let mut pubnub = Builder::new()
            .transport(transport)
            .runtime(DefaultRuntime)
            .build();

        let channel = self.connector.channel;
        let channel: channel::Name = channel.parse().or_else(|_| {
            Err(SourceError::FileIO(format!(
                "invalid pubnub channel: {}",
                channel
            )))
        })?;

        loop {
            let stream = pubnub.subscribe(channel.clone()).await;
            tokio::pin!(stream);

            while let Some(msg) = stream.next().await {
                if msg.message_type == Type::Publish {
                    let s = msg.json.dump();

                    let row = Row::pack_slice(&[Datum::String(&s)]);

                    timestamper
                        .insert(row)
                        .await
                        .map_err(|e| SourceError::FileIO(e.to_string()))?;
                }
            }

            log::info!(
                "pubnub channel {:?} disconnected. reconnecting",
                channel.to_string()
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
