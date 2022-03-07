use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::future::TryFutureExt;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::IntervalStream;
use tracing::warn;

use mz_dataflow_types::SourceErrorDetails;
use mz_expr::{GlobalId, SourceInstanceId};
use mz_repr::{Datum, Row};

use crate::source::{SimpleSource, SourceError, Timestamper};

pub struct LokiSourceReader {
    source_id: SourceInstanceId,
    conn_info: ConnectionInfo,
    batch_window: Duration,
    query: String,
}

#[derive(Clone)]
struct ConnectionInfo {
    user: String,
    pw: String,
    endpoint: String,
}

impl LokiSourceReader {
    pub fn new(
        source_id: SourceInstanceId,
        user: String,
        pw: String,
        endpoint: String,
        query: String,
    ) -> LokiSourceReader {
        Self {
            source_id: source_id,
            conn_info: ConnectionInfo { user, pw, endpoint },
            batch_window: Duration::from_secs(60),
            query: query,
        }
    }

    async fn query(
        conn_info: ConnectionInfo,
        start: u128,
        end: u128,
        query: String,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::new();
        client
            .get(format!("{}/loki/api/v1/query_range", conn_info.endpoint))
            .basic_auth(conn_info.user, Some(conn_info.pw))
            .query(&[("query", query)])
            .query(&[("start", format!("{}", start))])
            .query(&[("end", format!("{}", end))])
            .query(&[("direction", "forward")])
            .send()
            .await
    }

    fn new_stream(
        query: String,
        batch_window: Duration,
        conn_info: ConnectionInfo,
    ) -> impl stream::Stream<Item = Result<QueryResult, reqwest::Error>> {
        let polls = IntervalStream::new(tokio::time::interval(batch_window));

        let conn_info = conn_info;
        polls.then(move |_tick| {
            let start = SystemTime::now() - batch_window;
            let end = SystemTime::now();

            Self::query(
                conn_info.clone(),
                start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                end.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                query.clone(),
            )
            .and_then(|resp| resp.json::<QueryResult>())
        })
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        let stream = Self::new_stream(self.query, self.batch_window, self.conn_info);
        tokio::pin!(stream);

        while let Some(entry) = stream.next().await {
            match entry {
                Ok(result) => {
                    let Data::Streams(streams) = result.data;

                    let lines: Vec<String> = streams
                        .iter()
                        .flat_map(|s| s.values.iter().map(|v| v.line.clone()))
                        .collect();

                    for line in lines {
                        let row = Row::pack_slice(&[Datum::String(&line)]);

                        timestamper.insert(row).await.map_err(|e| SourceError {
                            source_id: self.source_id.clone(),
                            error: SourceErrorDetails::FileIO(e.to_string()),
                        })?;
                    }
                }
                Err(e) => warn!("Loki error: {:?}", e),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct QueryResult {
    status: String,
    data: Data,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", content = "result")]
enum Data {
    #[serde(rename = "streams")]
    Streams(Vec<Stream>),
}

#[derive(Debug, Deserialize)]
struct Stream {
    #[serde(rename = "stream")]
    labels: HashMap<String, String>,
    values: Vec<LogEntry>,
}

#[derive(Debug, Deserialize)]
struct LogEntry {
    ts: String,
    line: String,
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn connect() {
        let user = "5442";
        let pw = "";
        let endpoint = "https://logs-prod-us-central1.grafana.net";
        let uid = SourceInstanceId {
            source_id: GlobalId::Explain,
            dataflow_id: 1,
        };

        let loki = LokiSourceReader::new(
            uid,
            user.to_string(),
            pw.to_string(),
            endpoint.to_string(),
            "{job=\"systemd-journal\"}".to_owned(),
        );

        let fut = loki.new_stream().take(5);
        fut.for_each(|data| async move {
            println!("{:?}", data);
        })
        .await;
    }
}
