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

//! The Node binding of the protocol core, built for WebAssembly, a thin layer
//! over [`materialize_sdk_core::binding`].

use materialize_sdk_core::binding::{self, Error};
use wasm_bindgen::prelude::*;

// The exceptions are JavaScript classes, so that their hierarchy is plain
// JavaScript. `#errors` resolves through the package's `imports` map, so the
// glue and the package share one module and `instanceof` holds.
#[wasm_bindgen(raw_module = "#errors")]
extern "C" {
    type ProtocolError;
    #[wasm_bindgen(constructor)]
    fn new(message: &str) -> ProtocolError;

    type TimestampTooLarge;
    #[wasm_bindgen(constructor)]
    fn new(message: &str) -> TimestampTooLarge;

    type UsageError;
    #[wasm_bindgen(constructor)]
    fn new(message: &str) -> UsageError;
}

/// Runs `function` of the conformance vectors on `input`, both JSON text,
/// and returns the output as JSON text.
#[wasm_bindgen]
pub fn conformance(function: &str, input: &str) -> Result<String, JsValue> {
    let input = serde_json::from_str(input).map_err(|error| UsageError::new(&error.to_string()))?;
    Ok(materialize_sdk_core::conformance::run(function, input).to_string())
}

/// The state of one `SUBSCRIBE` cursor. Each method takes and returns JSON
/// text in the form of the `subscription` conformance vectors' steps.
#[wasm_bindgen]
pub struct Subscription(binding::Subscription);

#[wasm_bindgen]
impl Subscription {
    /// A subscription from `{"subscribe", "start", "max_pending_bytes"?,
    /// "max_timestamp_bytes"?}`.
    #[wasm_bindgen(constructor)]
    pub fn new(options: &str) -> Result<Subscription, JsValue> {
        Ok(Subscription(
            binding::Subscription::new(options).map_err(throw)?,
        ))
    }

    /// The `DECLARE` statement that opens the cursor, inside a transaction.
    #[wasm_bindgen(js_name = declareSql)]
    pub fn declare_sql(&mut self) -> Result<String, JsValue> {
        self.0.declare_sql().map_err(throw)
    }

    /// The next `FETCH` for `{"max_rows", "timeout_ms"?}`, as
    /// `{"sql", "timeout_ms"}`.
    pub fn fetch(&mut self, input: &str) -> Result<String, JsValue> {
        self.0.fetch(input).map_err(throw)
    }

    /// Accepts the last `FETCH`'s `{"columns", "rows"}` and returns
    /// `{"events", "finished"}`.
    pub fn push(&mut self, input: &str) -> Result<String, JsValue> {
        self.0.push(input).map_err(throw)
    }

    /// What `{"sqlstate", "message"}`, an error the server returned, comes to.
    pub fn classify(&self, input: &str) -> Result<String, JsValue> {
        self.0.classify(input).map_err(throw)
    }

    /// Whether the stream has ended, cleanly or not.
    #[wasm_bindgen(getter = isFinished)]
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }

    /// Whether a protocol error ended the stream.
    #[wasm_bindgen(getter = isFailed)]
    pub fn is_failed(&self) -> bool {
        self.0.is_failed()
    }

    /// The stream's `AS OF`, once known.
    #[wasm_bindgen(getter = asOf)]
    pub fn as_of(&self) -> Option<u64> {
        self.0.as_of()
    }
}

fn throw(error: Error) -> JsValue {
    let message = error.to_string();
    match error {
        Error::Protocol(_) => ProtocolError::new(&message).into(),
        Error::TimestampTooLarge(_) => TimestampTooLarge::new(&message).into(),
        Error::Usage(_) => UsageError::new(&message).into(),
    }
}
