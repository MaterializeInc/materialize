use futures::future;
use futures::stream::StreamExt;
use serde::Deserialize;
use std::{collections::HashMap, time::{Duration, UNIX_EPOCH, SystemTime}};
use tokio_stream::wrappers::IntervalStream;

pub struct LokiSourceReader {

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
