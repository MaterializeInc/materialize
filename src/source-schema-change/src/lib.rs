// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Upstream schema changes that Materialize sources cannot follow.

use mz_ore::str::StrExt;
use postgres_protocol::escape;
use serde::{Deserialize, Serialize};

/// An upstream schema change that Materialize cannot follow.
///
/// Each source wraps this in an error naming the affected table. `Display`
/// renders the diagnosis. [`SchemaChange::hint`] renders the recovery steps,
/// which are surfaced separately: as the `HINT` of a SQL error and in the
/// source status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SchemaChange {
    #[error("table was dropped and recreated upstream (it now has oid {oid})")]
    TableDropped { oid: u32 },
    #[error(
        "table was renamed or moved upstream (it is now {schema}.{name}{})",
        oid_suffix(.oid)
    )]
    TableRenamed {
        schema: String,
        name: String,
        oid: Option<u32>,
    },
    #[error("column {} was dropped or renamed upstream", .column.quoted())]
    ColumnDropped { column: String },
    #[error(
        "column {} changed position upstream (the column or table was likely dropped and \
         recreated)",
        .column.quoted()
    )]
    ColumnMoved { column: String },
    #[error("the type of column {} changed upstream", .column.quoted())]
    ColumnTypeChanged { column: String },
    #[error("the NOT NULL constraint on column {} was dropped upstream", .column.quoted())]
    NotNullDropped { column: String },
    #[error("{key} was dropped upstream")]
    KeyDropped { key: KeyRef },
    #[error("{key} was renamed or recreated upstream")]
    KeyAltered { key: KeyRef },
    #[error("{0}")]
    DescriptionFailed(String),
}

fn oid_suffix(oid: &Option<u32>) -> String {
    oid.map_or_else(String::new, |oid| format!(" with oid {oid}"))
}

/// A PRIMARY KEY or UNIQUE constraint as recorded when the table was created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRef {
    pub name: String,
    pub is_primary: bool,
    pub columns: Vec<String>,
}

impl std::fmt::Display for KeyRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = if self.is_primary {
            "PRIMARY KEY"
        } else {
            "UNIQUE"
        };
        write!(
            f,
            "{kind} constraint {} ({})",
            self.name.quoted(),
            self.columns.join(", ")
        )
    }
}

impl SchemaChange {
    /// The recovery steps for a dropped constraint on `schema.name`, including
    /// the statements to run. Other changes carry no hint.
    pub fn hint(&self, schema: &str, name: &str) -> Option<String> {
        let recreate = |with_clause: Option<&str>| {
            let mut hint = format!(
                "To keep ingesting without this constraint, recreate the table in a new \
                 versioned schema, then swap your views to the new table:\n  CREATE SCHEMA v2;\n  \
                 CREATE TABLE v2.{}\n  FROM SOURCE <source> (REFERENCE {}.{})",
                escape::escape_identifier(name),
                escape::escape_identifier(schema),
                escape::escape_identifier(name),
            );
            if let Some(with_clause) = with_clause {
                hint.push_str(&format!("\n  WITH ({with_clause})"));
            }
            hint.push(';');
            hint
        };
        match self {
            SchemaChange::KeyDropped { key } | SchemaChange::KeyAltered { key } => Some(format!(
                "{}\nTo make a planned constraint drop a non-event, create the table with \
                 WITH (EXCLUDE CONSTRAINTS ({})) before the upstream drop.",
                recreate(None),
                escape::escape_literal(&key.name),
            )),
            SchemaChange::NotNullDropped { .. } => Some(recreate(Some("EXCLUDE ALL CONSTRAINTS"))),
            SchemaChange::TableDropped { .. }
            | SchemaChange::TableRenamed { .. }
            | SchemaChange::ColumnDropped { .. }
            | SchemaChange::ColumnMoved { .. }
            | SchemaChange::ColumnTypeChanged { .. }
            | SchemaChange::DescriptionFailed(_) => None,
        }
    }
}
