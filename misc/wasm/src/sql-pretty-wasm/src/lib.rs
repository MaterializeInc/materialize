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

// Many things here only compile in the wasm32 target. We don't need to annotate it that way because
// this should only ever be built by wasm-build which specifies that.

use lol_alloc::{FreeListAllocator, LockedAllocator};
use wasm_bindgen::prelude::*;

#[global_allocator]
static ALLOCATOR: LockedAllocator<FreeListAllocator> =
    LockedAllocator::new(FreeListAllocator::new());

/// Pretty prints one SQL query.
///
/// Returns the pretty-printed query at the specified maximum target width. Returns an error if the
/// SQL query is not parseable.
#[wasm_bindgen(js_name = prettyStr)]
pub fn pretty_str(query: &str, width: usize) -> Result<String, JsError> {
    mz_sql_pretty::pretty_str(query, width).map_err(|e| JsError::new(&e.to_string()))
}

/// Pretty prints many SQL queries.
///
/// Returns the list of pretty-printed queries at the specified maximum target width. Returns an
/// error if any SQL query is not parseable.
#[wasm_bindgen(js_name = prettyStrs)]
pub fn pretty_strs(queries: &str, width: usize) -> Result<Vec<String>, JsError> {
    Ok(mz_sql_pretty::pretty_strs(queries, width)
        .map_err(|e| JsError::new(&e.to_string()))?
        .into_iter()
        .collect())
}
