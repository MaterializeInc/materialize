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

use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
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

pub(crate) const SCHEMA_REGISTER: Config<bool> = Config::new(
    "persist_schema_register",
    true,
    "register schemas with the shard when opening a read or write handle",
);

pub(crate) const SCHEMA_REQUIRE: Config<bool> = Config::new(
    "persist_schema_require",
    true,
    "error if schema registration is unsuccessful when opening a read or write handle",
);

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
