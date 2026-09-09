// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Upstream schema changes that Materialize cannot follow.

use itertools::Itertools;
use mz_ore::str::StrExt;
use postgres_protocol::escape;
use serde::{Deserialize, Serialize};

use crate::UnsupportedDataType;
use crate::desc::MySqlKeyDesc;

/// An upstream schema change that Materialize cannot follow.
///
/// `Display` renders the diagnosis. [`SchemaChangeError::hint`] renders the
/// recovery steps, which are surfaced separately: as the `HINT` of a SQL error
/// and in the source status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible schema change on {schema_name}.{name}: {change}")]
pub struct SchemaChangeError {
    pub schema_name: String,
    pub name: String,
    pub change: SchemaChange,
}

/// The upstream change behind a [`SchemaChangeError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SchemaChange {
    #[error("table was renamed or replaced upstream (it is now {schema_name}.{name})")]
    TableRenamed { schema_name: String, name: String },
    #[error("column {} was dropped or renamed upstream", .column.quoted())]
    ColumnDropped { column: String },
    #[error("the type of column {} changed upstream", .column.quoted())]
    ColumnTypeChanged { column: String },
    #[error("the NOT NULL constraint on column {} was dropped upstream", .column.quoted())]
    NotNullDropped { column: String },
    #[error("{key} was dropped upstream")]
    KeyDropped { key: MySqlKeyDesc },
    #[error("{key} was altered upstream")]
    KeyAltered { key: MySqlKeyDesc },
    #[error(
        "columns have types that Materialize cannot ingest: {}",
        .columns.iter().join(", ")
    )]
    UnsupportedColumnTypes { columns: Vec<UnsupportedDataType> },
    #[error("{0}")]
    DescriptionFailed(String),
}

impl SchemaChangeError {
    /// The recovery steps for a dropped constraint, including the statements
    /// to run. Other changes carry no hint.
    pub fn hint(&self) -> Option<String> {
        let recreate = |with_clause: Option<&str>| {
            let mut hint = format!(
                "To keep ingesting without this constraint, recreate the table in a new \
                 versioned schema, then swap your views to the new table:\n  CREATE SCHEMA v2;\n  \
                 CREATE TABLE v2.{}\n  FROM SOURCE <source> (REFERENCE {}.{})",
                escape::escape_identifier(&self.name),
                escape::escape_identifier(&self.schema_name),
                escape::escape_identifier(&self.name),
            );
            if let Some(with_clause) = with_clause {
                hint.push_str(&format!("\n  WITH ({with_clause})"));
            }
            hint.push(';');
            hint
        };
        match &self.change {
            SchemaChange::KeyDropped { key } | SchemaChange::KeyAltered { key } => Some(format!(
                "{}\nTo make a planned constraint drop a non-event, create the table with \
                 WITH (EXCLUDE CONSTRAINTS ({})) before the upstream drop.",
                recreate(None),
                escape::escape_literal(&key.name),
            )),
            SchemaChange::NotNullDropped { .. } => Some(recreate(Some("EXCLUDE ALL CONSTRAINTS"))),
            SchemaChange::TableRenamed { .. }
            | SchemaChange::ColumnDropped { .. }
            | SchemaChange::ColumnTypeChanged { .. }
            | SchemaChange::UnsupportedColumnTypes { .. }
            | SchemaChange::DescriptionFailed(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(change: SchemaChange) -> SchemaChangeError {
        SchemaChangeError {
            schema_name: "public".into(),
            name: "users".into(),
            change,
        }
    }

    #[mz_ore::test]
    fn dropped_key_names_the_constraint_and_exclusion() {
        let err = error(SchemaChange::KeyDropped {
            key: MySqlKeyDesc {
                name: "uq_wallet".into(),
                is_primary: false,
                columns: vec!["wallet".into()],
            },
        });
        assert_eq!(
            err.to_string(),
            "incompatible schema change on public.users: UNIQUE constraint \"uq_wallet\" \
             (wallet) was dropped upstream"
        );
        let hint = err.hint().expect("dropped keys carry a hint");
        assert!(hint.contains(
            "CREATE TABLE v2.\"users\"\n  FROM SOURCE <source> (REFERENCE \"public\".\"users\");"
        ));
        assert!(
            hint.ends_with("WITH (EXCLUDE CONSTRAINTS ('uq_wallet')) before the upstream drop.")
        );
    }

    #[mz_ore::test]
    fn dropped_primary_key_uses_mysql_index_name() {
        let err = error(SchemaChange::KeyDropped {
            key: MySqlKeyDesc {
                name: "PRIMARY".into(),
                is_primary: true,
                columns: vec!["id".into()],
            },
        });
        assert_eq!(
            err.to_string(),
            "incompatible schema change on public.users: PRIMARY KEY constraint \"PRIMARY\" \
             (id) was dropped upstream"
        );
        assert!(
            err.hint()
                .unwrap()
                .contains("EXCLUDE CONSTRAINTS ('PRIMARY')")
        );
    }

    #[mz_ore::test]
    fn dropped_not_null_hints_exclude_all() {
        let err = error(SchemaChange::NotNullDropped {
            column: "email".into(),
        });
        assert_eq!(
            err.to_string(),
            "incompatible schema change on public.users: the NOT NULL constraint on column \
             \"email\" was dropped upstream"
        );
        assert!(
            err.hint()
                .unwrap()
                .ends_with("WITH (EXCLUDE ALL CONSTRAINTS);")
        );
    }

    #[mz_ore::test]
    fn non_constraint_changes_carry_no_hint() {
        for change in [
            SchemaChange::TableRenamed {
                schema_name: "public".into(),
                name: "users_v2".into(),
            },
            SchemaChange::ColumnDropped {
                column: "email".into(),
            },
            SchemaChange::ColumnTypeChanged {
                column: "email".into(),
            },
            SchemaChange::UnsupportedColumnTypes { columns: vec![] },
            SchemaChange::DescriptionFailed("failed".into()),
        ] {
            assert_eq!(error(change).hint(), None);
        }
    }

    #[mz_ore::test]
    fn hint_quotes_identifiers_and_literals() {
        let err = SchemaChangeError {
            schema_name: "we\"ird".into(),
            name: "it's".into(),
            change: SchemaChange::KeyDropped {
                key: MySqlKeyDesc {
                    name: "o'k".into(),
                    is_primary: false,
                    columns: vec!["c".into()],
                },
            },
        };
        let hint = err.hint().unwrap();
        assert!(hint.contains("CREATE TABLE v2.\"it's\""));
        assert!(hint.contains("REFERENCE \"we\"\"ird\".\"it's\""));
        assert!(hint.contains("EXCLUDE CONSTRAINTS ('o''k')"));
    }
}
