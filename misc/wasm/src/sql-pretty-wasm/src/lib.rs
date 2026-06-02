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

#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &str = r#"
export type PrettyFormatMode = "simple" | "simpleRedacted" | "stable";

export interface PrettyConfig {
    width?: number;
    indent?: number;
    formatMode?: PrettyFormatMode;
}
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "PrettyConfig")]
    pub type JsPrettyConfig;

    #[wasm_bindgen(method, getter, structural)]
    fn width(this: &JsPrettyConfig) -> JsValue;

    #[wasm_bindgen(method, getter, structural)]
    fn indent(this: &JsPrettyConfig) -> JsValue;

    #[wasm_bindgen(method, getter, structural, js_name = formatMode)]
    fn format_mode(this: &JsPrettyConfig) -> JsValue;
}

fn number(value: JsValue, name: &str) -> Result<Option<f64>, JsError> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }

    let value = value
        .as_f64()
        .ok_or_else(|| JsError::new(&format!("{name} must be a number")))?;
    if value.is_finite() && value.fract() == 0.0 {
        Ok(Some(value))
    } else {
        Err(JsError::new(&format!("{name} must be an integer")))
    }
}

fn width(value: JsValue) -> Result<usize, JsError> {
    match number(value, "width")? {
        None => Ok(mz_sql_pretty::DEFAULT_WIDTH),
        Some(width) if width >= 0.0 && width <= usize::MAX as f64 => Ok(width as usize),
        Some(_) => Err(JsError::new("width is out of range")),
    }
}

fn indent(value: JsValue) -> Result<isize, JsError> {
    match number(value, "indent")? {
        None => Ok(mz_sql_pretty::DEFAULT_INDENT),
        Some(indent) if indent >= 0.0 && indent <= isize::MAX as f64 => Ok(indent as isize),
        Some(_) => Err(JsError::new("indent is out of range")),
    }
}

fn format_mode(format_mode: JsValue) -> Result<mz_sql_pretty::FormatMode, JsError> {
    if format_mode.is_undefined() || format_mode.is_null() {
        return Ok(mz_sql_pretty::FormatMode::Simple);
    }

    let Some(format_mode) = format_mode.as_string() else {
        return Err(JsError::new("formatMode must be a string"));
    };

    match format_mode.as_str() {
        "simple" | "Simple" => Ok(mz_sql_pretty::FormatMode::Simple),
        "simpleRedacted" | "SimpleRedacted" => Ok(mz_sql_pretty::FormatMode::SimpleRedacted),
        "stable" | "Stable" => Ok(mz_sql_pretty::FormatMode::Stable),
        _ => Err(JsError::new(&format!("invalid formatMode: {format_mode}"))),
    }
}

fn pretty_config(config: &JsPrettyConfig) -> Result<mz_sql_pretty::PrettyConfig, JsError> {
    let width = width(config.width())?;
    let indent = indent(config.indent())?;
    let format_mode = format_mode(config.format_mode())?;

    Ok(mz_sql_pretty::PrettyConfig {
        width,
        indent,
        format_mode,
    })
}

/// Pretty prints one SQL query.
///
/// Returns the pretty-printed query at the specified maximum target width. Returns an error if the
/// SQL query is not parseable.
#[wasm_bindgen(js_name = prettyStr)]
pub fn pretty_str(query: &str, width: usize) -> Result<String, JsError> {
    mz_sql_pretty::pretty_str_simple(query, width).map_err(|e| JsError::new(&e.to_string()))
}

/// Pretty prints one SQL query with the specified config.
///
/// Returns the pretty-printed query at the specified maximum target width, indent width, and format
/// mode. Returns an error if the SQL query is not parseable or if the config is invalid.
#[wasm_bindgen(js_name = prettyStrConfig)]
pub fn pretty_str_config(query: &str, config: &JsPrettyConfig) -> Result<String, JsError> {
    mz_sql_pretty::pretty_str(query, pretty_config(config)?)
        .map_err(|e| JsError::new(&e.to_string()))
}

/// Pretty prints many SQL queries.
///
/// Returns the list of pretty-printed queries at the specified maximum target width. Returns an
/// error if any SQL query is not parseable.
#[wasm_bindgen(js_name = prettyStrs)]
pub fn pretty_strs(queries: &str, width: usize) -> Result<Vec<String>, JsError> {
    Ok(mz_sql_pretty::pretty_strs_simple(queries, width)
        .map_err(|e| JsError::new(&e.to_string()))?
        .into_iter()
        .collect())
}

/// Pretty prints many SQL queries with the specified config.
///
/// Returns the list of pretty-printed queries at the specified maximum target width, indent width,
/// and format mode. Returns an error if any SQL query is not parseable or if the config is invalid.
#[wasm_bindgen(js_name = prettyStrsConfig)]
pub fn pretty_strs_config(queries: &str, config: &JsPrettyConfig) -> Result<Vec<String>, JsError> {
    Ok(mz_sql_pretty::pretty_strs(queries, pretty_config(config)?)
        .map_err(|e| JsError::new(&e.to_string()))?
        .into_iter()
        .collect())
}
