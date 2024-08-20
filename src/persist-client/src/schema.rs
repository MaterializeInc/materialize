// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist shard schema information.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, SchemaBuilder};
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_persist_types::columnar::Schema2;
use mz_persist_types::Codec;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// An ordered identifier for a pair of key and val schemas registered to a
/// shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
#[serde(try_from = "String", into = "String")]
pub struct SchemaId(pub(crate) usize);

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "h{}", self.0)
    }
}

impl From<SchemaId> for String {
    fn from(schema_id: SchemaId) -> Self {
        schema_id.to_string()
    }
}

impl TryFrom<String> for SchemaId {
    type Error = String;
    fn try_from(encoded: String) -> Result<Self, Self::Error> {
        let encoded = match encoded.strip_prefix('h') {
            Some(x) => x,
            None => return Err(format!("invalid SchemaId {}: incorrect prefix", encoded)),
        };
        let schema_id = u64::from_str(encoded)
            .map_err(|err| format!("invalid SchemaId {}: {}", encoded, err))?;
        Ok(SchemaId(usize::cast_from(schema_id)))
    }
}

/// The result returned by [crate::PersistClient::compare_and_evolve_schema].
#[derive(Debug)]
pub enum CaESchema<K: Codec, V: Codec> {
    /// The schema was successfully evolved and registered with the included id.
    Ok(SchemaId),
    /// The schema was not compatible with previously registered schemas.
    Incompatible,
    /// The `expected` SchemaId did not match reality. The current one is
    /// included for easy of retry.
    ExpectedMismatch {
        /// The current schema id.
        schema_id: SchemaId,
        /// The key schema at this id.
        key: K::Schema,
        /// The val schema at this id.
        val: V::Schema,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn schema_id() {
        assert_eq!(SchemaId(1).to_string(), "h1");
        assert_eq!(SchemaId::try_from("h1".to_owned()), Ok(SchemaId(1)));
        assert!(SchemaId::try_from("nope".to_owned()).is_err());
    }
}
