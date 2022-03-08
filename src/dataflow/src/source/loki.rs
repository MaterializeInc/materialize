use std::{
    collections::HashMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::future::TryFutureExt;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::IntervalStream;
use tracing::warn;

use mz_dataflow_types::SourceErrorDetails;
use mz_expr::SourceInstanceId;
use mz_repr::{Datum, Row};

use crate::source::{SimpleSource, SourceError, Timestamper};

pub struct LokiSourceReader {
    source_id: SourceInstanceId,
    conn_info: LokiConnectionInfo,
    batch_window: Duration,
    query: String,
    client: reqwest::Client,
}

#[derive(Clone)]
pub struct LokiConnectionInfo {
    user: String,
    pw: String,
    endpoint: String,
}

impl LokiConnectionInfo {
    /// Loads connection information form the environment. Checks for `LOKI_ADDR`, `LOKI_USERNAME` and `LOKI_PASSWORD`.
    pub fn from_env() -> LokiConnectionInfo {
        let mut c = LokiConnectionInfo {
            user: "".to_string(),
            pw: "".to_string(),
            endpoint: "".to_string(),
        };

        if let Ok(user) = env::var("LOKI_USERNAME") {
            c.user = user;
        }

        if let Ok(password) = env::var("LOKI_PASSWORD") {
            c.pw = password;
        }

        if let Ok(address) = env::var("LOKI_ADDR") {
            c.endpoint = address;
        }

        return c;
    }

    pub fn with_user(self, user: Option<String>) -> LokiConnectionInfo {
        if let Some(user) = user {
            let mut c = self.clone();
            c.user = user;
            return c;
        } else {
            return self;
        }
    }

    pub fn with_password(self, password: Option<String>) -> LokiConnectionInfo {
        if let Some(password) = password {
            let mut c = self.clone();
            c.pw = password;
            return c;
        } else {
            return self;
        }
    }

    pub fn with_endpont(self, address: Option<String>) -> LokiConnectionInfo {
        if let Some(address) = address {
            let mut c = self.clone();
            c.endpoint = address;
            return c;
        } else {
            return self;
        }
    }
}

impl LokiSourceReader {
    pub fn new(
        source_id: SourceInstanceId,
        conn_info: LokiConnectionInfo,
        query: String,
    ) -> LokiSourceReader {
        Self {
            source_id,
            conn_info,
            batch_window: Duration::from_secs(60),
            query: query,
            client: reqwest::Client::new(),
        }
    }

    async fn query(
        &self,
        conn_info: LokiConnectionInfo,
        start: u128,
        end: u128,
        query: String,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .get(format!("{}/loki/api/v1/query_range", conn_info.endpoint))
            .basic_auth(conn_info.user, Some(conn_info.pw))
            .query(&[("query", query)])
            .query(&[("start", format!("{}", start))])
            .query(&[("end", format!("{}", end))])
            .query(&[("direction", "forward")])
            .send()
            .await
    }

    fn new_stream<'a>(
        &'a self,
    ) -> impl stream::Stream<Item = Result<QueryResult, reqwest::Error>> + 'a {
        let polls = IntervalStream::new(tokio::time::interval(self.batch_window));

        let conn_info = self.conn_info.clone();
        polls.then(move |_tick| {
            let end = SystemTime::now();
            let start = end - self.batch_window;

            self.query(
                conn_info.clone(),
                start
                    .duration_since(UNIX_EPOCH)
                    .expect("Start must be after epoch.")
                    .as_nanos(),
                end.duration_since(UNIX_EPOCH)
                    .expect("End must be after epoch.")
                    .as_nanos(),
                self.query.clone(),
            )
            .and_then(|resp| resp.json::<QueryResult>())
        })
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        let stream = self.new_stream();
        tokio::pin!(stream);

        while let Some(entry) = stream.next().await {
            match entry {
                Ok(result) => {
                    let Data::Streams(streams) = result.data;

                    #[derive(Debug, Serialize)]
                    struct LokiRow<'a> {
                        line: &'a str,
                        labels: &'a HashMap<String, String>,
                    }

                    let lines: Vec<String> = streams
                        .iter()
                        .flat_map(|s| {
                            s.values.iter().map(|v| {
                                serde_json::to_string(&LokiRow {
                                    line: &v.line,
                                    labels: &s.labels,
                                })
                                .unwrap()
                            })
                        })
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
    use mz_expr::GlobalId;

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
            LokiConnectionInfo {
                user: user.to_string(),
                pw: pw.to_string(),
                endpoint: endpoint.to_string(),
            },
            "{job=\"systemd-journal\"}".to_owned(),
        );

        let fut = loki.new_stream().take(5);
        fut.for_each(|data| async move {
            println!("{:?}", data);
        })
        .await;
    }
}
