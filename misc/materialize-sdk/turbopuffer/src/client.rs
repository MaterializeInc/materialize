// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The parts of turbopuffer's HTTP API the sink uses.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use materialize_sdk::{SinkError, WriteResult};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

/// Where and how to reach turbopuffer.
#[derive(Clone)]
pub struct Connection {
    base_url: String,
    api_key: String,
    request_timeout: Duration,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("base_url", &self.base_url)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// The API in `region`, such as `gcp-us-central1`, with `api_key`.
    pub fn new(region: &str, api_key: impl Into<String>) -> Connection {
        Connection {
            base_url: format!("https://{region}.turbopuffer.com"),
            api_key: api_key.into(),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Sends requests to `base_url` instead of the region's, such as through
    /// a proxy.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Connection {
        self.base_url = base_url.into();
        self
    }

    /// How long one request may take before its response counts as lost. 30
    /// seconds by default.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Connection {
        self.request_timeout = timeout;
        self
    }
}

/// Why a request failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The request did not apply.
    Refused { status: u16, message: String },
    /// turbopuffer asked for fewer requests. The request did not apply.
    Throttled { retry_after: Option<Duration> },
    /// The response was lost or the server failed, so a write may have
    /// applied, or may still apply.
    Lost { message: String },
}

impl Failure {
    /// The error a failed read or a write that did not apply returns.
    pub(crate) fn into_error(self) -> SinkError {
        match self {
            // 401 and 403 can pass once the key or its permissions change.
            Failure::Refused { status, message } => SinkError::Target {
                message: format!("turbopuffer refused the request ({status}): {message}"),
                permanent: matches!(status, 400 | 404 | 413 | 422),
            },
            Failure::Throttled { retry_after } => SinkError::Throttled { retry_after },
            Failure::Lost { message } => SinkError::Target {
                message,
                permanent: false,
            },
        }
    }

    /// What a write that failed this way came to.
    pub(crate) fn into_write_result(self) -> WriteResult {
        match self {
            Failure::Lost { .. } => WriteResult::Unknown,
            failure => WriteResult::Failed(failure.into_error()),
        }
    }
}

/// A response whose shape the client does not know, which it treats as a
/// failure instead of guessing.
/// The unindexed bytes `metadata` reports. A status without a count is taken
/// as not indexed yet: waiting keeps readers on the incarnation they use,
/// where a failure would end the run and start the snapshot, and its
/// embedding, again.
fn unindexed(namespace: &str, metadata: &Value) -> u64 {
    let index = |field: &str| metadata.get("index").and_then(|index| index.get(field));
    match index("status").and_then(Value::as_str) {
        Some("up-to-date") => 0,
        Some("updating") => index("unindexed_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX),
        other => {
            tracing::warn!(namespace, status = ?other, "an unknown index status");
            u64::MAX
        }
    }
}

fn unexpected(what: &str, detail: &str) -> Failure {
    Failure::Lost {
        message: format!("an unexpected turbopuffer {what} response: {detail}"),
    }
}

/// `text` as a URL query value.
fn encode(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// The counts a write reports. A count is zero when turbopuffer leaves it
/// out.
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct WriteResponse {
    #[serde(default)]
    pub(crate) rows_upserted: u64,
    #[serde(default)]
    pub(crate) rows_patched: u64,
    /// The ids a patch applied to, when the request asked for them.
    #[serde(default)]
    pub(crate) patched_ids: Vec<Value>,
    #[serde(default)]
    pub(crate) performance: Performance,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct Performance {
    /// The tokens turbopuffer embedded, which it bills, also for rows whose
    /// condition skipped them.
    #[serde(default)]
    pub(crate) embedding_tokens: Option<u64>,
}

/// A turbopuffer client.
#[derive(Clone)]
pub(crate) struct Client {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    #[cfg(test)]
    pub(crate) faults: std::sync::Arc<std::sync::Mutex<Faults>>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

/// Failures a test injects into writes, in the order of the writes. Writes
/// after the plan run out pass.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct Faults {
    pub(crate) plan: std::collections::VecDeque<Fault>,
    /// How many writes were sent to turbopuffer.
    pub(crate) sent: usize,
    /// Ids whose rows turbopuffer refuses with a 400, as it would a row
    /// that breaks a limit the sink does not check.
    pub(crate) refused_ids: std::collections::BTreeSet<String>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    Pass,
    /// The write applies, and its response is lost.
    Lose,
    /// The write is never sent, and reports a lost response.
    Drop,
    /// The write reports a lost response at once and lands after the delay,
    /// as a request stuck in the network does.
    Late(Duration),
}

impl Client {
    pub(crate) fn new(connection: &Connection) -> Result<Client, SinkError> {
        let http = reqwest::Client::builder()
            .timeout(connection.request_timeout)
            .build()
            .map_err(|error| SinkError::Target {
                message: format!("cannot build the HTTP client: {error}"),
                permanent: true,
            })?;
        Ok(Client {
            http,
            base_url: connection.base_url.trim_end_matches('/').to_string(),
            api_key: connection.api_key.clone(),
            #[cfg(test)]
            faults: Default::default(),
        })
    }

    /// Writes the JSON `body` to `namespace`, creating the namespace if it
    /// does not exist.
    pub(crate) async fn write(
        &self,
        namespace: &str,
        body: String,
    ) -> Result<WriteResponse, Failure> {
        #[cfg(test)]
        if let Some(id) = self.refused_id(&body) {
            return Err(Failure::Refused {
                status: 400,
                message: format!("the test refuses the row {id}"),
            });
        }
        #[cfg(test)]
        let fault = self.next_fault();
        #[cfg(test)]
        if fault == Fault::Drop {
            return Err(Failure::Lost {
                message: "dropped by the test".into(),
            });
        }
        let path = format!("/v2/namespaces/{namespace}");
        #[cfg(test)]
        if let Fault::Late(delay) = fault {
            let client = self.clone();
            // A standalone SDK cannot use the repository's spawn wrappers.
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = client.request(Method::POST, &path, Some(body)).await;
            });
            return Err(Failure::Lost {
                message: "delayed by the test".into(),
            });
        }
        let response = self.request(Method::POST, &path, Some(body)).await?;
        #[cfg(test)]
        if fault == Fault::Lose {
            return Err(Failure::Lost {
                message: "lost by the test".into(),
            });
        }
        let response = response.ok_or_else(|| Failure::Refused {
            status: 404,
            message: format!("turbopuffer found no namespace {namespace} to write to"),
        })?;
        serde_json::from_value(response).map_err(|error| Failure::Lost {
            message: format!("an unexpected write response: {error}"),
        })
    }

    /// The rows a query of `namespace` returns, or `None` if the namespace
    /// does not exist.
    pub(crate) async fn query(
        &self,
        namespace: &str,
        body: &Value,
    ) -> Result<Option<Vec<Value>>, Failure> {
        let Some(mut response) = self.query_response(namespace, body).await? else {
            return Ok(None);
        };
        match response.get_mut("rows").map(Value::take) {
            Some(Value::Array(rows)) => Ok(Some(rows)),
            other => Err(unexpected("query", &format!("rows are {other:?}"))),
        }
    }

    /// The response to a query of `namespace`, or `None` if the namespace
    /// does not exist.
    pub(crate) async fn query_response(
        &self,
        namespace: &str,
        body: &Value,
    ) -> Result<Option<Value>, Failure> {
        let path = format!("/v2/namespaces/{namespace}/query");
        self.request(Method::POST, &path, Some(body.to_string()))
            .await
    }

    /// The bytes written to `namespace` that turbopuffer has not indexed
    /// yet, or `None` if the namespace does not exist.
    pub(crate) async fn unindexed_bytes(&self, namespace: &str) -> Result<Option<u64>, Failure> {
        let Some(metadata) = self.metadata(namespace).await? else {
            return Ok(None);
        };
        Ok(Some(unindexed(namespace, &metadata)))
    }

    /// The model and dimensions of each attribute `namespace` embeds, or
    /// `None` if the namespace does not exist.
    pub(crate) async fn embeddings(
        &self,
        namespace: &str,
    ) -> Result<Option<BTreeMap<String, (String, u32)>>, Failure> {
        let Some(metadata) = self.metadata(namespace).await? else {
            return Ok(None);
        };
        let schema = metadata
            .get("schema")
            .and_then(Value::as_object)
            .ok_or_else(|| unexpected("metadata", "no schema"))?;
        let mut embeddings = BTreeMap::new();
        for (column, attribute) in schema {
            let Some(embed) = attribute.get("embed").filter(|embed| !embed.is_null()) else {
                continue;
            };
            let model = embed.get("model").and_then(Value::as_str);
            // The vector's type, such as `[1024]f16`, holds the dimensions.
            let dims = embed
                .get("attribute")
                .and_then(Value::as_str)
                .and_then(|vector| schema.get(vector)?.get("type")?.as_str())
                .and_then(|kind| kind.strip_prefix('[')?.split(']').next()?.parse().ok());
            match (model, dims) {
                (Some(model), Some(dims)) => {
                    embeddings.insert(column.clone(), (model.to_string(), dims));
                }
                _ => {
                    return Err(unexpected(
                        "metadata",
                        &format!("the embedding of {column}"),
                    ));
                }
            }
        }
        Ok(Some(embeddings))
    }

    async fn metadata(&self, namespace: &str) -> Result<Option<Value>, Failure> {
        let path = format!("/v1/namespaces/{namespace}/metadata");
        self.request(Method::GET, &path, None).await
    }

    /// The names of the namespaces that start with `prefix`.
    pub(crate) async fn namespaces(&self, prefix: &str) -> Result<Vec<String>, Failure> {
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut path = format!("/v1/namespaces?prefix={}&page_size=1000", encode(prefix));
            if let Some(cursor) = &cursor {
                path.push_str(&format!("&cursor={}", encode(cursor)));
            }
            let page = self
                .request(Method::GET, &path, None)
                .await?
                .ok_or_else(|| unexpected("namespace list", "not found"))?;
            let ids = page
                .get("namespaces")
                .and_then(Value::as_array)
                .ok_or_else(|| unexpected("namespace list", "no namespaces"))?;
            for namespace in ids {
                let id = namespace.get("id").and_then(Value::as_str);
                names.push(
                    id.ok_or_else(|| unexpected("namespace list", "a namespace without an id"))?
                        .to_string(),
                );
            }
            match page.get("next_cursor").and_then(Value::as_str) {
                Some(next) => cursor = Some(next.to_string()),
                None => return Ok(names),
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn delete_namespace(&self, namespace: &str) -> Result<(), Failure> {
        let path = format!("/v2/namespaces/{namespace}");
        self.request(Method::DELETE, &path, None).await.map(|_| ())
    }

    /// Sends a request and returns its response body, or `None` for a 404.
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<String>,
    ) -> Result<Option<Value>, Failure> {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(&self.api_key);
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request.send().await.map_err(|error| Failure::Lost {
            message: format!("turbopuffer did not respond: {error}"),
        })?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .map(Duration::from_secs);
        let text = response.text().await.map_err(|error| Failure::Lost {
            message: format!("turbopuffer's response was cut off: {error}"),
        })?;
        if status.is_success() {
            let body = if text.is_empty() {
                json!({})
            } else {
                serde_json::from_str(&text).map_err(|error| Failure::Lost {
                    message: format!("turbopuffer's response is not JSON: {error}"),
                })?
            };
            return Ok(Some(body));
        }
        let message = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|body| {
                body.get("error")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(text);
        match status {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::TOO_MANY_REQUESTS => Err(Failure::Throttled { retry_after }),
            status if status.is_client_error() => Err(Failure::Refused {
                status: status.as_u16(),
                message,
            }),
            status => Err(Failure::Lost {
                message: format!("turbopuffer failed ({status}): {message}"),
            }),
        }
    }

    #[cfg(test)]
    fn refused_id(&self, body: &str) -> Option<String> {
        let faults = self.faults.lock().unwrap();
        faults
            .refused_ids
            .iter()
            .find(|id| body.contains(&format!("\"id\":\"{id}\"")))
            .cloned()
    }

    #[cfg(test)]
    fn next_fault(&self) -> Fault {
        let mut faults = self.faults.lock().unwrap();
        let fault = faults.plan.pop_front().unwrap_or(Fault::Pass);
        if fault != Fault::Drop {
            faults.sent += 1;
        }
        fault
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_index_status_without_a_count_waits() {
        let status = |index: Value| unindexed("ns", &json!({ "index": index }));
        assert_eq!(status(json!({"status": "up-to-date"})), 0);
        assert_eq!(
            status(json!({"status": "updating", "unindexed_bytes": 7})),
            7
        );
        assert_eq!(status(json!({"status": "updating"})), u64::MAX);
        assert_eq!(status(json!({"status": "rebuilding"})), u64::MAX);
        assert_eq!(unindexed("ns", &json!({})), u64::MAX);
    }

    #[test]
    fn failures_keep_what_they_say_about_the_write() {
        let refused = |status| Failure::Refused {
            status,
            message: "no".into(),
        };
        let permanent = |failure: Failure| match failure.into_error() {
            SinkError::Target { permanent, .. } => permanent,
            other => panic!("expected a target error, got {other:?}"),
        };
        for status in [400, 404, 413, 422] {
            assert!(permanent(refused(status)), "{status}");
        }
        for status in [401, 403, 409] {
            assert!(!permanent(refused(status)), "{status}");
        }
        let lost = Failure::Lost {
            message: "gone".into(),
        };
        assert!(matches!(
            lost.clone().into_write_result(),
            WriteResult::Unknown
        ));
        assert!(!permanent(lost));
        let throttled = Failure::Throttled {
            retry_after: Some(Duration::from_secs(2)),
        };
        assert!(matches!(
            throttled.into_write_result(),
            WriteResult::Failed(SinkError::Throttled {
                retry_after: Some(_)
            })
        ));
    }

    #[test]
    fn query_values_are_percent_encoded() {
        assert_eq!(encode("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(encode("MDk0+Zg/=="), "MDk0%2BZg%2F%3D%3D");
    }
}
