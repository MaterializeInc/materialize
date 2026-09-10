// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Upstream schema changes that Materialize cannot follow.

use serde::{Deserialize, Serialize};

pub use mz_source_schema_change::{KeyRef, SchemaChange};

/// A [`SchemaChange`] on a PostgreSQL table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible schema change on {namespace}.{name} (oid {oid}): {change}")]
pub struct SchemaChangeError {
    pub namespace: String,
    pub name: String,
    pub oid: u32,
    pub change: SchemaChange,
}

impl SchemaChangeError {
    pub fn hint(&self) -> Option<String> {
        self.change.hint(&self.namespace, &self.name)
    }
}
