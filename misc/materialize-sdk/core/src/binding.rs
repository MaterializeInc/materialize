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

//! A [`Subscription`] whose inputs and outputs are JSON text in the form of
//! the `subscription` conformance vectors' steps.
//!
//! Every language binding wraps this type, so a binding only passes strings
//! across its boundary and runs those vectors through its own class. Inputs
//! refuse unknown keys, so a misspelled option fails instead of being ignored.

use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::batch::Start;
use crate::decode::Row;
use crate::error::{ProtocolError, TimestampTooLarge, UsageError};
use crate::statement::Subscribe;
use crate::subscription::{Fetch, PushError};

/// Why a call failed. Each binding raises its own exception for each variant.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    TimestampTooLarge(#[from] TimestampTooLarge),
    #[error(transparent)]
    Usage(#[from] UsageError),
}

impl Error {
    /// The error's name in the vectors' `{"error": ...}` outputs.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Protocol(_) => "protocol",
            Error::TimestampTooLarge(_) => "timestamp_too_large",
            Error::Usage(_) => "usage",
        }
    }
}

impl From<PushError> for Error {
    fn from(error: PushError) -> Error {
        match error {
            PushError::Protocol(error) => error.into(),
            PushError::TimestampTooLarge(error) => error.into(),
            PushError::Usage(error) => error.into(),
        }
    }
}

/// See the module documentation.
#[derive(Debug)]
pub struct Subscription {
    inner: crate::Subscription,
    /// The `FETCH` whose result the subscription waits for.
    outstanding: Option<Fetch>,
}

impl Subscription {
    /// A subscription from `{"subscribe", "start", "max_pending_bytes"?,
    /// "max_timestamp_bytes"?}`.
    pub fn new(options: &str) -> Result<Subscription, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Options {
            subscribe: Subscribe,
            start: Start,
            max_pending_bytes: Option<String>,
            max_timestamp_bytes: Option<String>,
        }
        let options: Options = parse(options)?;
        let pending = options
            .max_pending_bytes
            .as_deref()
            .map(decimal)
            .transpose()?;
        let timestamp = options
            .max_timestamp_bytes
            .as_deref()
            .map(decimal)
            .transpose()?;
        crate::batch::check_limits(pending, timestamp)?;
        let mut inner = crate::Subscription::new(options.subscribe, options.start)?;
        if let Some(limit) = pending {
            inner = inner.with_max_pending_bytes(limit);
        }
        if let Some(limit) = timestamp {
            inner = inner.with_max_timestamp_bytes(limit);
        }
        Ok(Subscription {
            inner,
            outstanding: None,
        })
    }

    /// The `DECLARE` statement that opens the cursor, inside a transaction.
    pub fn declare_sql(&mut self) -> Result<String, Error> {
        Ok(self.inner.declare_sql()?)
    }

    /// The next `FETCH` for `{"max_rows", "timeout_ms"?}`, as
    /// `{"sql", "timeout_ms"}`. The core may drop the timeout to probe for the
    /// end of the stream.
    pub fn fetch(&mut self, input: &str) -> Result<String, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            max_rows: String,
            timeout_ms: Option<String>,
        }
        let input: Input = parse(input)?;
        let timeout = match input.timeout_ms {
            Some(ms) => Some(Duration::from_millis(decimal(&ms)?)),
            None => None,
        };
        let fetch = self.inner.fetch(decimal(&input.max_rows)?, timeout)?;
        let output = json!({
            "sql": fetch.sql(),
            "timeout_ms": fetch.timeout().map(|t| t.as_millis().to_string()),
        });
        self.outstanding = Some(fetch);
        Ok(output.to_string())
    }

    /// Accepts the last `FETCH`'s `{"columns", "rows"}`, every cell in its text
    /// format, and returns `{"events", "finished"}`.
    pub fn push(&mut self, input: &str) -> Result<String, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            columns: Vec<String>,
            rows: Vec<Row>,
        }
        let Input { columns, rows } = parse(input)?;
        let fetch = self
            .outstanding
            .take()
            .ok_or_else(|| UsageError::new("a push follows a fetch"))?;
        let events = self.inner.push(fetch, &columns, rows)?;
        Ok(json!({ "events": events, "finished": self.inner.is_finished() }).to_string())
    }

    /// What `{"sqlstate", "message"}`, an error the server returned, comes to.
    pub fn classify(&self, input: &str) -> Result<String, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            sqlstate: Option<String>,
            message: String,
        }
        let input: Input = parse(input)?;
        let classification = self
            .inner
            .classify(input.sqlstate.as_deref(), &input.message);
        Ok(json!(classification).to_string())
    }

    /// See [`crate::Subscription::is_finished`]. A failed subscription can be
    /// finished too, so a caller checks [`Subscription::is_failed`] before
    /// treating this as the end of the stream.
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// See [`crate::Subscription::is_failed`].
    pub fn is_failed(&self) -> bool {
        self.inner.is_failed()
    }

    /// See [`crate::Subscription::as_of`].
    pub fn as_of(&self) -> Option<u64> {
        self.inner.as_of()
    }
}

fn parse<T: DeserializeOwned>(text: &str) -> Result<T, UsageError> {
    serde_json::from_str(text).map_err(|error| UsageError::new(format!("invalid input: {error}")))
}

/// Parses a decimal string, the JSON form of a 64-bit integer, refusing a
/// sign or leading zeros.
fn decimal<T: FromStr + ToString>(s: &str) -> Result<T, UsageError> {
    match s.parse::<T>() {
        Ok(n) if n.to_string() == s => Ok(n),
        _ => Err(UsageError::new(format!("{s:?} is not a decimal string"))),
    }
}
