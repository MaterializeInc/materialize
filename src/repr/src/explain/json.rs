// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structs and traits for `EXPLAIN AS JSON`.

use crate::explain::*;

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Json`].
pub trait DisplayJson
where
    Self: Sized,
{
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value>;
}

/// Render a type `t: T` as [`ExplainFormat::Json`].
///
/// # Panics
///
/// Panics if the [`DisplayJson::to_serde_value`] or the subsequent
/// [`serde_json::to_string_pretty`] call return a [`serde_json::Error`].
pub fn json_string<T: DisplayJson>(t: &T) -> String {
    let value = t.to_serde_value().expect("serde_json::Value");
    serde_json::to_string_pretty(&value).expect("JSON string")
}

impl DisplayJson for String {
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        Ok(serde_json::Value::String(self.clone()))
    }
}

impl DisplayJson for UnsupportedFormat {
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        unreachable!()
    }
}
