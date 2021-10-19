use std::{collections::HashMap, time::{Duration, UNIX_EPOCH, SystemTime}};

use async_trait::async_trait;
use futures::future;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::IntervalStream;

use crate::source::{SimpleSource, SourceError, Timestamper};

pub struct LokiSourceReader {
    user: String,
    pw: String,
    endpoint: String,
    batch_window: Duration,
}

impl LokiSourceReader {
    pub fn new(user: String, pw: String, endpoint: String) -> LokiSourceReader {
        Self {
            user,
            pw,
            endpoint,
            batch_window: Duration::from_secs(60),
        }
    }

    async fn query(self, start: u128, end: u128) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::new();
            client.get(format!("{}/loki/api/v1/query_range", self.endpoint))
                .basic_auth(self.user, Some(self.pw))
                .query(&[("query", "{job=\"systemd-journal\"}")])
                .query(&[("start", format!("{}", start))])
                .query(&[("end", format!("{}", end))])
                .query(&[("direction", "forward")])
                .send()
                .await
    }

    fn new_stream(self) -> impl stream::Stream<Item = Result<QueryResult, reqwest::Error>> {
        let polls = IntervalStream::new(tokio::time::interval(self.batch_window));

        polls.then(|_tick| async {
            let start = SystemTime::now() - self.batch_window;
            let end = SystemTime::now();

            let resp = self.query(
                start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                end.duration_since(UNIX_EPOCH).unwrap().as_nanos()).await?;

            resp.json::<QueryResult>()
        })
    }
}

#[async_trait]
impl SimpleSource for LokiSourceReader {

    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
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

        let start = SystemTime::now() - Duration::from_secs(60 * 60);

        async fn query(user: &str, pw: &str, endpoint: &str, start: u128, end: u128) -> reqwest::Response {
            let client = reqwest::Client::new();
            client.get(format!("{}/loki/api/v1/query_range", endpoint))
                .basic_auth(user, Some(pw))
                .query(&[("query", "{job=\"systemd-journal\"}")])
                .query(&[("start", format!("{}", start))])
                .query(&[("end", format!("{}", end))])
                .query(&[("direction", "forward")])
                .send()
                .await.unwrap()
        }

        let resp = query(
            user, pw, endpoint,
            start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
            (start.duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(60 * 5)).as_nanos()).await;
        println!("Status: {}", resp.status());
        //println!("Body: {}", resp.text().await.unwrap());

        //let parsed = resp.json::<QueryResult>().await.unwrap();
        //let Data::Streams(s) = parsed.data;
        //println!("{}: {}", s[0].values[0].ts, s[0].values[0].line);

        let window = Duration::from_secs(60);
        let polls = IntervalStream::new(tokio::time::interval(window));
        let fut = polls.take(5).then(|_tick| async move {
            let start = SystemTime::now() - window;
            let end = SystemTime::now();

            let resp = query(
                user, pw, endpoint,
                start.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                end.duration_since(UNIX_EPOCH).unwrap().as_nanos()).await;

            let parsed = resp.json::<QueryResult>().await.unwrap();
            parsed.data
        });

        fut.for_each(|data| async move {
            println!("{:?}", data);
        }).await;
    }
}
