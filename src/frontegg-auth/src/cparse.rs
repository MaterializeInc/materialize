// Copyright Materialize, Inc. All rights reserved.
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