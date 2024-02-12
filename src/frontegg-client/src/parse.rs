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
