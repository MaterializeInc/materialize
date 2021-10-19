use std::{collections::HashMap, time::{Duration, UNIX_EPOCH, SystemTime}};

use async_trait::async_trait;
use futures::future::{TryFutureExt};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::IntervalStream;

use crate::source::{SimpleSource, SourceError, Timestamper};

pub struct LokiSourceReader {
    conn_info: ConnectionInfo,
    batch_window: Duration,
}

#[derive(Clone)]
struct ConnectionInfo {
    user: String,
    pw: String,
    endpoint: String,
}

impl LokiSourceReader {
    pub fn new(user: String, pw: String, endpoint: String) -> LokiSourceReader {
        Self {
            conn_info: ConnectionInfo { user, pw, endpoint },
            batch_window: Duration::from_secs(60),
        }
    }

    async fn query(conn_info: ConnectionInfo, start: u128, end: u128) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::new();
            client.get(format!("{}/loki/api/v1/query_range", conn_info.endpoint))
                .basic_auth(conn_info.user, Some(conn_info.pw))
                .query(&[("query", "{job=\"systemd-journal\"}")])
                .query(&[("start", format!("{}", start))])
                .query(&[("end", format!("{}", end))])
                .query(&[("direction", "forward")])
                .send()
                .await
    }

    fn new_stream(self) -> impl stream::Stream<Item = Result<QueryResult, reqwest::Error>> {
        let polls = IntervalStream::new(tokio::time::interval(self.batch_window));

        let batch_window = self.batch_window;
        let conn_info = self.conn_info.clone();
        polls.then(move |_tick| {
            let start = SystemTime::now() - batch_window;
            let end = SystemTime::now();

            Self::query(
                conn_info.clone(),
                start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                end.duration_since(UNIX_EPOCH).unwrap().as_nanos())
                .and_then(|resp| { resp.json::<QueryResult>() })
        })
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {

    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        let stream = self.new_stream();
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
        let loki = LokiSourceReader::new(user.to_string(), pw.to_string(), endpoint.to_string());

        let fut = loki.new_stream().take(5);
        fut.for_each(|data| async move {
            println!("{:?}", data);
        }).await;
    }
}
