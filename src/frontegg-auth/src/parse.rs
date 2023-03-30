// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Deserializer};

/// The pagination wrapper type for API calls that are paginated.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paginated<T> {
    pub items: Vec<T>,
    #[serde(rename = "_metadata")]
    pub metadata: PaginatedMetadata,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedMetadata {
    pub total_pages: u64,
}

/// A struct that deserializes nothing.
///
/// Useful for deserializing empty response bodies.
pub struct Empty;

impl<'de> Deserialize<'de> for Empty {
    fn deserialize<D>(_: D) -> Result<Empty, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Empty)
    }
}

pub mod nested_json {
    use std::fmt;

    use serde::de::{Error, Visitor};
    use serde::Deserializer;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Default)]
        struct NestedJson;

        impl<'de> Visitor<'de> for NestedJson {
            type Value = serde_json::Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "valid nested json object")
            }

            fn visit_unit<E>(self) -> Result<serde_json::Value, E>
            where
                E: Error,
            {
                Ok(serde_json::Value::Null)
            }

            fn visit_str<E>(self, value: &str) -> Result<serde_json::Value, E>
            where
                E: Error,
            {
                serde_json::from_str(value).map_err(Error::custom)
            }
        }

        deserializer.deserialize_any(NestedJson)
    }
}

pub fn empty_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}
