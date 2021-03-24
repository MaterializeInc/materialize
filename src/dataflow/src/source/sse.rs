// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::io::{Error, ErrorKind};
use std::time::Duration;

use async_trait::async_trait;
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use reqwest::Client;

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::SseSourceConnector;
use repr::RowPacker;

/// Information required to sync data from Sse
pub struct SseSourceReader {
    connector: SseSourceConnector,
    reconnection_time: Duration,
    last_event_id: String,
}

impl SseSourceReader {
    /// Constructs a new instance
    pub fn new(connector: SseSourceConnector) -> Self {
        Self {
            connector,
            reconnection_time: Duration::from_secs(5),
            last_event_id: String::new(),
        }
    }

    async fn try_produce(&mut self, timestamper: &Timestamper) -> Result<(), anyhow::Error> {
        let mut packer = RowPacker::new();

        let client = Client::new();

        let request = client.get(&self.connector.url);
        let request = if self.last_event_id.is_empty() {
            request
        } else {
            request.header("Last-Event-ID", &self.last_event_id)
        };

        let response = request.send().await?;

        let lines = response
            .bytes_stream()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
            .into_async_read()
            .lines();
        tokio::pin!(lines);

        // word for word implementation of the pseudocode defined in:
        // https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
        let mut event_type = String::new();
        let mut data = String::new();
        let mut last_event_id = String::new();
        while let Some(Ok(line)) = lines.next().await {
            if line.is_empty() {
                self.last_event_id = last_event_id.clone();

                if data.is_empty() {
                    event_type = String::new();
                    continue;
                }
                if data.ends_with('\n') {
                    data.pop();
                }

                // Replace with empty strings
                let event_type = std::mem::take(&mut event_type);
                let data = std::mem::take(&mut data);

                packer.push((&*last_event_id).into());
                packer.push((&*event_type).into());
                packer.push((&*data).into());

                let row = packer.finish_and_reuse();

                timestamper.insert(row).await?;
            } else {
                if line.starts_with(':') {
                    continue;
                }

                let (field, value) = if let Some(idx) = line.find(':') {
                    let (field, value) = line.split_at(idx);
                    let mut value = &value[1..];
                    if value.starts_with(' ') {
                        value = &value[1..];
                    }
                    (field, value)
                } else {
                    (&*line, "")
                };

                match field {
                    "event" => event_type = field.to_string(),
                    "data" => {
                        let _ = writeln!(data, "{}", value);
                    }
                    "id" => {
                        if !value.contains('\0') {
                            last_event_id = value.to_string();
                        }
                    }
                    "retry" => {
                        if let Ok(ms) = value.parse::<u64>() {
                            self.reconnection_time = Duration::from_millis(ms);
                        }
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleSource for SseSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        loop {
            let _ = self.try_produce(timestamper).await;
            tokio::time::sleep(self.reconnection_time).await;
        }
    }
}
