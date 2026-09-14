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

use crate::desc::{SqlServerTableConstraint, SqlServerTableConstraintType};

/// A [`SchemaChange`] on a SQL Server table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible schema change on {schema_name}.{name}: {change}")]
pub struct SchemaChangeError {
    pub schema_name: String,
    pub name: String,
    pub change: SchemaChange,
}

impl SchemaChangeError {
    pub fn hint(&self) -> Option<String> {
        self.change.hint(&self.schema_name, &self.name)
    }
}

impl From<&SqlServerTableConstraint> for KeyRef {
    fn from(constraint: &SqlServerTableConstraint) -> Self {
        KeyRef {
            name: constraint.constraint_name.clone(),
            is_primary: matches!(
                constraint.constraint_type,
                SqlServerTableConstraintType::PrimaryKey
            ),
            columns: constraint.column_names.clone(),
        }
    }
}
