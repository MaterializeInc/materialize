use serde::Deserialize;
use std::time::{Duration, UNIX_EPOCH, SystemTime};

pub struct LokiSourceReader {

}

#[derive(Deserialize)]
struct QueryResult {
    status: String,
    data: Data,
}

#[derive(Deserialize)]
#[serde(tag = "resultType", content = "result")]
enum Data {
    #[serde(rename = "streams")]
    Streams(Vec<Stream>),
}

#[derive(Deserialize)]
struct Stream {
    values: Vec<LogEntry>,
}

#[derive(Deserialize)]
struct LogEntry {
    ts: String,
    line: String,
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn connect() {

        let start = SystemTime::now() - Duration::new(60 * 60 * 6, 0);

        let client = reqwest::Client::new();
        let resp = client.get(format!("{}/loki/api/v1/query_range", endpoint))
            .basic_auth(user, Some(pw))
            .query(&[("query", "{job=\"systemd-journal\"}")])
            .query(&[("start", format!("{}", start.duration_since(UNIX_EPOCH).unwrap().as_nanos()))])
            .query(&[("direction", "forward")])
            .send()
            .await.unwrap();

        println!("Status: {}", resp.status());
        //println!("Body: {}", resp.text().await.unwrap());

        let parsed = resp.json::<QueryResult>().await.unwrap();
        let Data::Streams(s) = parsed.data;
        println!("{}: {}", s[0].values[0].ts, s[0].values[0].line);
    }
}
