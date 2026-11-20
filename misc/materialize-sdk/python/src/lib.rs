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

//! The Python binding of the protocol core, the `materialize_sdk._core`
//! module, a thin layer over [`materialize_sdk_core::binding`].

use materialize_sdk_core::binding::{self, Error};
use pyo3::prelude::*;

// The exceptions are Python classes, so that their hierarchy is plain Python.
pyo3::import_exception!(materialize_sdk._errors, ProtocolError);
pyo3::import_exception!(materialize_sdk._errors, TimestampTooLarge);
pyo3::import_exception!(materialize_sdk._errors, UsageError);

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(conformance, m)?)?;
    m.add_class::<Subscription>()?;
    Ok(())
}

/// Runs `function` of the conformance vectors on `input`, both JSON text,
/// and returns the output as JSON text.
#[pyfunction]
fn conformance(function: &str, input: &str) -> PyResult<String> {
    let input =
        serde_json::from_str(input).map_err(|error| UsageError::new_err(error.to_string()))?;
    Ok(materialize_sdk_core::conformance::run(function, input).to_string())
}

/// The state of one `SUBSCRIBE` cursor. Each method takes and returns JSON
/// text in the form of the `subscription` conformance vectors' steps.
#[pyclass(module = "materialize_sdk._core")]
struct Subscription(binding::Subscription);

#[pymethods]
impl Subscription {
    /// A subscription from `{"subscribe", "start", "max_pending_bytes"?,
    /// "max_timestamp_bytes"?}`.
    #[new]
    fn new(options: &str) -> PyResult<Subscription> {
        Ok(Subscription(
            binding::Subscription::new(options).map_err(raise)?,
        ))
    }

    /// The `DECLARE` statement that opens the cursor, inside a transaction.
    fn declare_sql(&mut self) -> PyResult<String> {
        self.0.declare_sql().map_err(raise)
    }

    /// The next `FETCH` for `{"max_rows", "timeout_ms"?}`, as
    /// `{"sql", "timeout_ms"}`.
    fn fetch(&mut self, input: &str) -> PyResult<String> {
        self.0.fetch(input).map_err(raise)
    }

    /// Accepts the last `FETCH`'s `{"columns", "rows"}` and returns
    /// `{"events", "finished"}`.
    fn push(&mut self, input: &str) -> PyResult<String> {
        self.0.push(input).map_err(raise)
    }

    /// What `{"sqlstate", "message"}`, an error the server returned, comes to.
    fn classify(&self, input: &str) -> PyResult<String> {
        self.0.classify(input).map_err(raise)
    }

    /// Whether the stream has ended, cleanly or not.
    #[getter]
    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }

    /// Whether a protocol error ended the stream.
    #[getter]
    fn is_failed(&self) -> bool {
        self.0.is_failed()
    }

    /// The stream's `AS OF`, once known.
    #[getter]
    fn as_of(&self) -> Option<u64> {
        self.0.as_of()
    }
}

fn raise(error: Error) -> PyErr {
    let message = error.to_string();
    match error {
        Error::Protocol(_) => ProtocolError::new_err(message),
        Error::TimestampTooLarge(_) => TimestampTooLarge::new_err(message),
        Error::Usage(_) => UsageError::new_err(message),
    }
}
