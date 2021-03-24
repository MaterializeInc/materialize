// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use futures::StreamExt;
use pubnub_hyper::core::data::message::Type;
use pubnub_hyper::{Builder, DefaultRuntime, DefaultTransport};

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::PubNubSourceConnector;
use repr::{Datum, RowPacker};

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

        let channel = self.connector.channel.parse().unwrap();

        let stream = pubnub.subscribe(channel).await;
        tokio::pin!(stream);

        let mut packer = RowPacker::new();

        while let Some(msg) = stream.next().await {
            if msg.message_type == Type::Publish {
                let s = msg.json.dump();

                let datum: Datum = (&*s).into();
                packer.push(datum);
                let row = packer.finish_and_reuse();

                timestamper
                    .insert(row)
                    .await
                    .map_err(|e| SourceError::FileIO(e.to_string()))?;
            }
        }

        Ok(())
    }
}
