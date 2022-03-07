// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::Utc;
use mz_ore::retry::Retry;
use serde::Serialize;
use url::Url;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    /// Labels to be added to the ingested lines.
    labels: HashMap<String, String>,
    /// The lines to be ingested.
    lines: Vec<String>,
}

pub fn build_ingest(cmd: BuiltinCommand) -> Result<IngestAction, anyhow::Error> {
    let labels = cmd.args.into_iter().collect();
    Ok(IngestAction {
        labels,
        lines: cmd.input,
    })
}

#[async_trait]
impl Action for IngestAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        state
            .loki_client
            .push_logs(&self.labels, &self.lines)
            .await?;
        Ok(ControlFlow::Continue)
    }
}

pub struct Client {
    url: Url,
    client: reqwest::Client,
    timeout: Duration,
}

impl Client {
    pub(super) fn new(url: Url, timeout: Duration) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
            timeout,
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    async fn push_logs(
        &self,
        labels: &HashMap<String, String>,
        lines: &[String],
    ) -> Result<(), anyhow::Error> {
        let ts = Utc::now().timestamp_nanos().to_string();
        let ts_str = ts.as_str();
        let values: Vec<_> = lines
            .iter()
            .map(move |line| [ts_str, line.as_str()])
            .collect();
        let body = LokiApiV1Push {
            streams: vec![Stream {
                stream: &labels,
                values: &values,
            }],
        };
        let mut url = self.url.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow!("invalid Loki URL"))?
            .pop_if_empty()
            .push("loki")
            .push("api")
            .push("v1")
            .push("push");
        Retry::default()
            .max_duration(self.timeout)
            .retry_async(|_| async {
                self.client
                    .post(url.clone())
                    .json(&body)
                    .send()
                    .await
                    .context("connecting to Loki")?
                    .error_for_status()
                    .context("Loki HTTP error")
            })
            .await
            .context("sending Loki records")
            // Drop the response, we don't care about it.
            .map(|_| ())
    }
}

#[derive(Debug, Serialize)]
struct LokiApiV1Push<'a> {
    streams: Vec<Stream<'a>>,
}

#[derive(Debug, Serialize)]
struct Stream<'a> {
    stream: &'a HashMap<String, String>,
    values: &'a [[&'a str; 2]],
}
