// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Descriptions of PostgreSQL objects.

use std::collections::{BTreeMap, BTreeSet};

use mz_ore::str::StrExt;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Oid;

include!(concat!(env!("OUT_DIR"), "/mz_postgres_util.desc.rs"));

/// Describes a schema in a PostgreSQL database.
///
/// <https://www.postgresql.org/docs/current/catalog-pg-namespace.html>
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSchemaDesc {
    /// The OID of the schema.
    pub oid: Oid,
    /// The name of the schema.
    pub name: String,
    /// Owner of the namespace
    pub owner: Oid,
}

/// Describes a table in a PostgreSQL database.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresTableDesc {
    /// The OID of the table.
    pub oid: Oid,
    /// The name of the schema that the table belongs to.
    pub namespace: String,
    /// The name of the table.
    pub name: String,
    /// The description of each column, in order of their position in the table.
    #[proptest(strategy = "proptest::collection::vec(any::<PostgresColumnDesc>(), 1..4)")]
    pub columns: Vec<PostgresColumnDesc>,
    /// Applicable keys for this table (i.e. primary key and unique
    /// constraints).
    #[proptest(strategy = "proptest::collection::btree_set(any::<PostgresKeyDesc>(), 1..4)")]
    pub keys: BTreeSet<PostgresKeyDesc>,
}

/// An upstream schema change that Materialize cannot follow.
///
/// `Display` renders the diagnosis. [`SchemaChangeError::hint`] renders the
/// recovery steps, which are surfaced separately: as the `HINT` of a SQL error
/// and in the source status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible schema change on {namespace}.{name}: {change}")]
pub struct SchemaChangeError {
    pub namespace: String,
    pub name: String,
    pub change: SchemaChange,
}

/// The upstream change behind a [`SchemaChangeError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SchemaChange {
    #[error("table was renamed or moved upstream (it is now {namespace}.{name} with oid {oid})")]
    TableRenamed {
        namespace: String,
        name: String,
        oid: u32,
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
    #[error("column {} was altered upstream", .column.quoted())]
    ColumnAltered { column: String },
    #[error("{key} was dropped upstream")]
    KeyDropped { key: KeyRef },
    #[error("{key} was dropped and recreated upstream")]
    KeyRecreated { key: KeyRef },
    #[error("{key} was renamed upstream to {}", .new_name.quoted())]
    KeyRenamed { key: KeyRef, new_name: String },
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

impl SchemaChangeError {
    /// The recovery steps for this change, including the statements to run.
    pub fn hint(&self) -> String {
        let reference = format!("{}.{}", self.namespace, self.name);
        let recreate = |lead: &str, with_clause: Option<String>| {
            recreate_hint(lead, &self.name, &reference, with_clause.as_deref())
        };
        let exclude = |name: &str| format!("EXCLUDE CONSTRAINTS ('{}')", name.replace('\'', "''"));
        match &self.change {
            SchemaChange::TableRenamed {
                namespace, name, ..
            } => recreate_hint(
                "To keep ingesting from the upstream table as it now exists",
                name,
                &format!("{namespace}.{name}"),
                None,
            ),
            SchemaChange::ColumnDropped { column } => format!(
                "{}\nTo make a planned column drop a non-event, create the table with \
                 WITH (EXCLUDE COLUMNS ({})) before the upstream drop.",
                recreate("To keep ingesting without this column", None),
                column.quoted(),
            ),
            SchemaChange::ColumnMoved { .. } | SchemaChange::ColumnAltered { .. } => {
                recreate("To keep ingesting", None)
            }
            SchemaChange::ColumnTypeChanged { column } => recreate(
                "To ingest the column as text regardless of its upstream type",
                Some(format!("TEXT COLUMNS ({})", column.quoted())),
            ),
            SchemaChange::NotNullDropped { .. } => recreate(
                "To keep ingesting without this constraint",
                Some("EXCLUDE ALL CONSTRAINTS".into()),
            ),
            SchemaChange::KeyDropped { key } => format!(
                "{}\nTo make a planned constraint drop a non-event, create the table with \
                 WITH ({}) before the upstream drop.",
                recreate("To keep ingesting without this constraint", None),
                exclude(&key.name),
            ),
            SchemaChange::KeyRecreated { key } => recreate(
                "To keep ingesting without this constraint",
                Some(exclude(&key.name)),
            ),
            SchemaChange::KeyRenamed { new_name, .. } => recreate(
                "To keep ingesting without this constraint",
                Some(exclude(new_name)),
            ),
        }
    }
}

fn recreate_hint(
    lead: &str,
    table_name: &str,
    reference: &str,
    with_clause: Option<&str>,
) -> String {
    let mut hint = format!(
        "{lead}, recreate the table in a new versioned schema, then swap your views to the \
         new table:\n  CREATE SCHEMA v2;\n  CREATE TABLE v2.{table_name}\n  \
         FROM SOURCE <source> (REFERENCE {reference})"
    );
    if let Some(with_clause) = with_clause {
        hint.push_str(&format!("\n  WITH ({with_clause})"));
    }
    hint.push(';');
    hint
}

impl PostgresTableDesc {
    /// Determines if two `PostgresTableDesc` are compatible with one another in
    /// a way that Materialize can handle.
    ///
    /// Currently this means that the values are equal except for the following
    /// exceptions:
    /// - `self`'s columns are a compatible prefix of `other`'s columns.
    ///   Compatibility is defined as returning `true` for
    ///   `PostgresColumnDesc::is_compatible`.
    /// - `self`'s keys are all present in `other`
    ///
    /// On incompatibility, the error describes the first mismatch found and
    /// how to recover from it. The error becomes the permanent, user-visible
    /// error for the stalled table, so it must stand on its own.
    pub fn determine_compatibility(
        &self,
        other: &PostgresTableDesc,
        allow_type_to_change_by_col_num: &BTreeSet<u16>,
    ) -> Result<(), SchemaChangeError> {
        if self == other {
            return Ok(());
        }

        if self.oid != other.oid || self.namespace != other.namespace || self.name != other.name {
            return Err(self.schema_change(SchemaChange::TableRenamed {
                namespace: other.namespace.clone(),
                name: other.name.clone(),
                oid: other.oid,
            }));
        }

        let other_cols_by_name = BTreeMap::from_iter(other.columns.iter().map(|c| (&c.name, c)));
        for column in &self.columns {
            let allow_type_change = allow_type_to_change_by_col_num.contains(&column.col_num);
            let other_column = other_cols_by_name.get(&column.name).copied();
            if let Some(change) = column.diff(other_column, allow_type_change) {
                return Err(self.schema_change(change));
            }
        }

        if let Some(key) = self.keys.difference(&other.keys).next() {
            return Err(self.schema_change(self.key_change(key, other)));
        }

        Ok(())
    }

    fn schema_change(&self, change: SchemaChange) -> SchemaChangeError {
        SchemaChangeError {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            change,
        }
    }

    fn key_change(&self, key: &PostgresKeyDesc, other: &PostgresTableDesc) -> SchemaChange {
        let key_ref = KeyRef {
            name: key.name.clone(),
            is_primary: key.is_primary,
            columns: key
                .cols
                .iter()
                .map(|attnum| {
                    self.columns
                        .iter()
                        .find(|c| c.col_num == *attnum)
                        .map_or_else(|| format!("attnum {}", attnum), |c| c.name.clone())
                })
                .collect(),
        };
        if let Some(renamed) = other.keys.iter().find(|k| k.oid == key.oid) {
            SchemaChange::KeyRenamed {
                key: key_ref,
                new_name: renamed.name.clone(),
            }
        } else if other.keys.iter().any(|k| k.name == key.name) {
            SchemaChange::KeyRecreated { key: key_ref }
        } else {
            SchemaChange::KeyDropped { key: key_ref }
        }
    }
}

impl RustType<ProtoPostgresTableDesc> for PostgresTableDesc {
    fn into_proto(&self) -> ProtoPostgresTableDesc {
        ProtoPostgresTableDesc {
            oid: self.oid,
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.into_proto()).collect(),
            keys: self.keys.iter().map(PostgresKeyDesc::into_proto).collect(),
        }
    }

    fn from_proto(proto: ProtoPostgresTableDesc) -> Result<Self, TryFromProtoError> {
        Ok(PostgresTableDesc {
            oid: proto.oid,
            namespace: proto.namespace.clone(),
            name: proto.name.clone(),
            columns: proto
                .columns
                .into_iter()
                .map(PostgresColumnDesc::from_proto)
                .collect::<Result<_, _>>()?,
            keys: proto
                .keys
                .into_iter()
                .map(PostgresKeyDesc::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Describes a column in a [`PostgresTableDesc`].
#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Arbitrary
)]
pub struct PostgresColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The column's monotonic position in its table, i.e. "this was the _i_th
    /// column created" irrespective of the current number of columns.
    pub col_num: u16,
    /// The OID of the column's type.
    pub type_oid: Oid,
    /// The modifier for the column's type.
    pub type_mod: i32,
    /// True if the column lacks a `NOT NULL` constraint.
    pub nullable: bool,
}

impl PostgresColumnDesc {
    /// Determines if data a relation with a structure of `other` can be treated
    /// the same as `self`.
    ///
    /// Note that this function somewhat unnecessarily errors if the names
    /// differ; this is negotiable but we want users to understand the fixedness
    /// of names in our schemas.
    fn is_compatible(&self, other: &PostgresColumnDesc, allow_type_change: bool) -> bool {
        self.name == other.name
            && self.col_num == other.col_num
            && (self.type_oid == other.type_oid || allow_type_change)
            && (self.type_mod == other.type_mod || allow_type_change)
            // Columns are compatible if:
            // - self is nullable; introducing a not null constraint doesn't
            //   change this column's behavior.
            // - self and other are both not nullable
            && (self.nullable || self.nullable == other.nullable)
    }
}

impl PostgresColumnDesc {
    fn diff(
        &self,
        other: Option<&PostgresColumnDesc>,
        allow_type_change: bool,
    ) -> Option<SchemaChange> {
        let column = self.name.clone();
        let Some(other) = other else {
            return Some(SchemaChange::ColumnDropped { column });
        };
        if self.is_compatible(other, allow_type_change) {
            return None;
        }
        if self.col_num != other.col_num {
            return Some(SchemaChange::ColumnMoved { column });
        }
        if !allow_type_change
            && (self.type_oid != other.type_oid || self.type_mod != other.type_mod)
        {
            return Some(SchemaChange::ColumnTypeChanged { column });
        }
        if !self.nullable && other.nullable {
            return Some(SchemaChange::NotNullDropped { column });
        }
        Some(SchemaChange::ColumnAltered { column })
    }
}

impl RustType<ProtoPostgresColumnDesc> for PostgresColumnDesc {
    fn into_proto(&self) -> ProtoPostgresColumnDesc {
        ProtoPostgresColumnDesc {
            name: self.name.clone(),
            col_num: Some(self.col_num.into()),
            type_oid: self.type_oid,
            type_mod: self.type_mod,
            nullable: self.nullable,
        }
    }

    fn from_proto(proto: ProtoPostgresColumnDesc) -> Result<Self, TryFromProtoError> {
        let col_num_u32: u32 = proto
            .col_num
            .into_rust_if_some("ProtoPostgresColumnDesc::col_num")?;
        // `col_num` is `u16` on the Rust side. Reject u32 values that don't fit
        // instead of panicking. This is reachable from untrusted proto bytes.
        let col_num = u16::try_from(col_num_u32)
            .map_err(|e| TryFromProtoError::InvalidFieldError(e.to_string()))?;
        Ok(PostgresColumnDesc {
            name: proto.name,
            col_num,
            type_oid: proto.type_oid,
            type_mod: proto.type_mod,
            nullable: proto.nullable,
        })
    }
}

/// Describes a key in a [`PostgresTableDesc`].
#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    Arbitrary
)]
pub struct PostgresKeyDesc {
    /// This key is derived from the `pg_constraint` with this OID.
    pub oid: Oid,
    /// The name of the constraints.
    pub name: String,
    /// The `attnum` of the columns comprising the key. `attnum` is a unique identifier for a column
    /// in a PG table; see <https://www.postgresql.org/docs/current/catalog-pg-attribute.html>
    #[proptest(strategy = "proptest::collection::vec(any::<u16>(), 0..4)")]
    pub cols: Vec<u16>,
    /// Whether or not this key is the primary key.
    pub is_primary: bool,
    /// If this constraint was generated with NULLS NOT DISTINCT; see
    /// <https://www.postgresql.org/about/featurematrix/detail/392/>
    pub nulls_not_distinct: bool,
}

impl RustType<ProtoPostgresKeyDesc> for PostgresKeyDesc {
    fn into_proto(&self) -> ProtoPostgresKeyDesc {
        ProtoPostgresKeyDesc {
            oid: self.oid,
            name: self.name.clone(),
            cols: self.cols.clone().into_iter().map(u32::from).collect(),
            is_primary: self.is_primary,
            nulls_not_distinct: self.nulls_not_distinct,
        }
    }

    fn from_proto(proto: ProtoPostgresKeyDesc) -> Result<Self, TryFromProtoError> {
        // `cols` is `Vec<u16>` on the Rust side but `Vec<u32>` on the wire;
        // a u32 value above 65535 used to panic via `.expect`, which is
        // reachable from untrusted proto bytes.
        let cols = proto
            .cols
            .into_iter()
            .map(|c| {
                u16::try_from(c).map_err(|e| TryFromProtoError::InvalidFieldError(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PostgresKeyDesc {
            oid: proto.oid,
            name: proto.name,
            cols,
            is_primary: proto.is_primary,
            nulls_not_distinct: proto.nulls_not_distinct,
        })
    }
}
