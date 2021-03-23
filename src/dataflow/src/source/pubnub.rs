use async_trait::async_trait;
use futures::StreamExt;
use pubnub_hyper::core::data::message::Type;
use pubnub_hyper::{Builder, DefaultRuntime, DefaultTransport};

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::PubnubSourceConnector;
use repr::{Datum, RowPacker};

/// Information required to sync data from Pubnub
pub struct PubnubSourceReader {
    connector: PubnubSourceConnector,
}

impl PubnubSourceReader {
    /// Constructs a new instance
    pub fn new(connector: PubnubSourceConnector) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl SimpleSource for PubnubSourceReader {
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
