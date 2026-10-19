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

//! The reader helper.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use crate::checkpoint::{DOCUMENT_ID, LAYOUT};
use crate::client::{Client, Connection, Failure};
use crate::sink::{check_name, checkpoint_namespace, data_namespace};

/// Queries what a [`crate::TurbopufferSink`] wrote, in the incarnation its
/// readers use, without its tombstones.
///
/// The sink switches readers to a new incarnation when a snapshot completes,
/// so a reader looks the incarnation up before each query, or caches it for
/// at most [`Reader::with_cache`]. Readers never read the checkpoint text.
/// Clones share the connection and the cache.
#[derive(Clone, Debug)]
pub struct Reader {
    client: Client,
    name: String,
    max_age: Duration,
    /// The incarnation last looked up, and when the lookup was sent.
    cached: Arc<Mutex<Option<(Instant, u64)>>>,
}

/// Why a read failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The sink has not completed its first snapshot, or was never set up.
    #[error("the sink has no incarnation for readers yet")]
    NotReady,
    /// The query is not a JSON object, or a multi-query's `queries` is not a
    /// list of them.
    #[error("a query is a JSON object, and a multi-query a list of them")]
    InvalidQuery,
    /// turbopuffer asked for fewer requests.
    #[error("turbopuffer is throttling queries")]
    Throttled {
        /// How long turbopuffer asked to wait.
        retry_after: Option<Duration>,
    },
    /// turbopuffer failed or refused the request.
    #[error("{0}")]
    Turbopuffer(String),
}

impl From<Failure> for ReadError {
    fn from(failure: Failure) -> ReadError {
        match failure {
            Failure::Throttled { retry_after } => ReadError::Throttled { retry_after },
            failure => ReadError::Turbopuffer(failure.into_error().to_string()),
        }
    }
}

impl Reader {
    /// A reader of the sink named `name`, as passed to
    /// [`crate::TurbopufferSink::new`].
    pub fn new(connection: &Connection, name: impl Into<String>) -> Result<Reader, ReadError> {
        let name = name.into();
        check_name(&name).map_err(ReadError::Turbopuffer)?;
        let client = Client::new(connection).map_err(|e| ReadError::Turbopuffer(e.to_string()))?;
        Ok(Reader {
            client,
            name,
            max_age: Duration::ZERO,
            cached: Arc::default(),
        })
    }

    /// Reuses the incarnation looked up for up to `max_age`, which saves a
    /// request per query. After a switch, queries keep going to the old
    /// incarnation for up to `max_age`. The sink keeps retired incarnations,
    /// so such a query reads stale documents but does not fail.
    pub fn with_cache(mut self, max_age: Duration) -> Reader {
        self.max_age = max_age;
        self
    }

    /// The namespace readers use now.
    pub async fn namespace(&self) -> Result<String, ReadError> {
        let incarnation = self.incarnation(false).await?;
        Ok(data_namespace(&self.name, incarnation))
    }

    /// Runs `query`, the body of a turbopuffer query or multi-query, in the
    /// namespace readers use, and returns turbopuffer's response, or `None`
    /// if the incarnation has no namespace, as for an empty view. The filters
    /// of the query, or of each query in a multi-query, are extended to leave
    /// tombstones out.
    pub async fn query(&self, query: Value) -> Result<Option<Value>, ReadError> {
        let body = live_only(query)?;
        let incarnation = self.incarnation(false).await?;
        if let Some(response) = self.run(incarnation, &body).await? {
            return Ok(Some(response));
        }
        // turbopuffer creates a namespace only on its first write. A missing
        // namespace is empty, unless readers moved on since the lookup.
        let current = self.incarnation(true).await?;
        if current == incarnation {
            return Ok(None);
        }
        self.run(current, &body).await
    }

    async fn run(&self, incarnation: u64, body: &Value) -> Result<Option<Value>, ReadError> {
        let namespace = data_namespace(&self.name, incarnation);
        Ok(self.client.query_response(&namespace, body).await?)
    }

    /// The incarnation readers use, from the cache unless `fresh`.
    async fn incarnation(&self, fresh: bool) -> Result<u64, ReadError> {
        let cached = *self.cached.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((at, incarnation)) = cached
            && !fresh
            && at.elapsed() < self.max_age
        {
            return Ok(incarnation);
        }
        let sent = Instant::now();
        let body = json!({
            "filters": ["id", "Eq", DOCUMENT_ID],
            "rank_by": ["id", "asc"],
            "limit": 1,
            "include_attributes": ["active", "layout"],
        });
        let rows = self
            .client
            .query(&checkpoint_namespace(&self.name), &body)
            .await?;
        let Some(document) = rows.as_deref().and_then(<[Value]>::first) else {
            return Err(ReadError::NotReady);
        };
        if document.get("layout").and_then(Value::as_u64) != Some(LAYOUT) {
            return Err(ReadError::Turbopuffer(format!(
                "the checkpoint document of {} has a layout this reader does not know",
                self.name
            )));
        }
        let incarnation = document
            .get("active")
            .and_then(Value::as_u64)
            .ok_or(ReadError::NotReady)?;
        *self.cached.lock().unwrap_or_else(|p| p.into_inner()) = Some((sent, incarnation));
        Ok(incarnation)
    }
}

/// `query` with the filters of each query extended to leave tombstones out.
fn live_only(query: Value) -> Result<Value, ReadError> {
    let Value::Object(mut body) = query else {
        return Err(ReadError::InvalidQuery);
    };
    match body.remove("queries") {
        Some(Value::Array(queries)) => {
            let queries = queries
                .into_iter()
                .map(|query| match query {
                    Value::Object(query) => Ok(Value::Object(with_live_filter(query))),
                    _ => Err(ReadError::InvalidQuery),
                })
                .collect::<Result<Vec<_>, _>>()?;
            body.insert("queries".into(), Value::Array(queries));
            Ok(Value::Object(body))
        }
        Some(_) => Err(ReadError::InvalidQuery),
        None => Ok(Value::Object(with_live_filter(body))),
    }
}

fn with_live_filter(mut query: Map<String, Value>) -> Map<String, Value> {
    let live = json!(["deleted", "Eq", false]);
    let filters = match query.remove("filters") {
        None | Some(Value::Null) => live,
        Some(filters) => json!(["And", [filters, live]]),
    };
    query.insert("filters".into(), filters);
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_query_leaves_tombstones_out() {
        let live = json!(["deleted", "Eq", false]);
        assert_eq!(
            live_only(json!({"rank_by": ["id", "asc"]})).unwrap(),
            json!({"rank_by": ["id", "asc"], "filters": live})
        );
        assert_eq!(
            live_only(json!({"filters": null})).unwrap(),
            json!({"filters": live})
        );
        assert_eq!(
            live_only(json!({"filters": ["views", "Gt", 1]})).unwrap(),
            json!({"filters": ["And", [["views", "Gt", 1], live]]})
        );
        assert_eq!(
            live_only(json!({"queries": [{"limit": 1}, {"filters": ["a", "Eq", 1]}]})).unwrap(),
            json!({"queries": [
                {"limit": 1, "filters": live},
                {"filters": ["And", [["a", "Eq", 1], live]]},
            ]})
        );
        for invalid in [json!([]), json!({"queries": {}}), json!({"queries": [1]})] {
            assert_eq!(
                live_only(invalid.clone()),
                Err(ReadError::InvalidQuery),
                "{invalid}"
            );
        }
    }
}
