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
/// `Display` renders the diagnosis. `hint` carries the recovery steps and is
/// surfaced separately: as the `HINT` of a SQL error and in the source status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaChangeError {
    pub table: String,
    pub change: String,
    pub hint: String,
}

impl std::fmt::Display for SchemaChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "incompatible schema change on {}: {}",
            self.table, self.change
        )
    }
}

impl std::error::Error for SchemaChangeError {}

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
            let reference = format!("{}.{}", other.namespace, other.name);
            return Err(self.error(
                format!(
                    "table was renamed or moved upstream (it is now {} with oid {})",
                    reference, other.oid
                ),
                recreate_hint(
                    "To keep ingesting from the upstream table as it now exists",
                    &other.name,
                    &reference,
                    None,
                ),
            ));
        }

        let other_cols_by_name = BTreeMap::from_iter(other.columns.iter().map(|c| (&c.name, c)));
        for column in &self.columns {
            let allow_type_change = allow_type_to_change_by_col_num.contains(&column.col_num);
            self.check_column(
                column,
                other_cols_by_name.get(&column.name).copied(),
                allow_type_change,
            )?;
        }

        if let Some(key) = self.keys.difference(&other.keys).next() {
            return Err(self.key_error(key, other));
        }

        Ok(())
    }

    fn check_column(
        &self,
        column: &PostgresColumnDesc,
        other: Option<&PostgresColumnDesc>,
        allow_type_change: bool,
    ) -> Result<(), SchemaChangeError> {
        let name = column.name.quoted();
        let Some(other) = other else {
            return Err(self.error(
                format!("column {name} was dropped or renamed upstream"),
                format!(
                    "{}\nTo make a planned column drop a non-event, create the table with \
                     WITH (EXCLUDE COLUMNS ({name})) before the upstream drop.",
                    self.recreate_hint("To keep ingesting without this column", None)
                ),
            ));
        };
        if column.is_compatible(other, allow_type_change) {
            return Ok(());
        }
        if column.col_num != other.col_num {
            return Err(self.error(
                format!(
                    "column {name} changed position upstream (the column or table was likely \
                     dropped and recreated)"
                ),
                self.recreate_hint("To keep ingesting", None),
            ));
        }
        if !allow_type_change
            && (column.type_oid != other.type_oid || column.type_mod != other.type_mod)
        {
            return Err(self.error(
                format!("the type of column {name} changed upstream"),
                self.recreate_hint(
                    "To ingest the column as text regardless of its upstream type",
                    Some(&format!("TEXT COLUMNS ({name})")),
                ),
            ));
        }
        if !column.nullable && other.nullable {
            return Err(self.error(
                format!("the NOT NULL constraint on column {name} was dropped upstream"),
                self.recreate_hint(
                    "To keep ingesting without this constraint",
                    Some("EXCLUDE ALL CONSTRAINTS"),
                ),
            ));
        }
        Err(self.error(
            format!("column {name} was altered upstream"),
            self.recreate_hint("To keep ingesting", None),
        ))
    }

    fn key_error(&self, key: &PostgresKeyDesc, other: &PostgresTableDesc) -> SchemaChangeError {
        let kind = if key.is_primary {
            "PRIMARY KEY"
        } else {
            "UNIQUE"
        };
        let cols = key
            .cols
            .iter()
            .map(|attnum| {
                self.columns
                    .iter()
                    .find(|c| c.col_num == *attnum)
                    .map_or_else(|| format!("attnum {}", attnum), |c| c.name.clone())
            })
            .collect::<Vec<_>>()
            .join(", ");
        let constraint = format!("{kind} constraint {} ({cols})", key.name.quoted());
        let exclude = |name: &str| format!("EXCLUDE CONSTRAINTS ('{}')", name.replace('\'', "''"));

        if let Some(renamed) = other.keys.iter().find(|k| k.oid == key.oid) {
            return self.error(
                format!(
                    "{constraint} was renamed upstream to {}",
                    renamed.name.quoted()
                ),
                self.recreate_hint(
                    "To keep ingesting without this constraint",
                    Some(&exclude(&renamed.name)),
                ),
            );
        }
        if other.keys.iter().any(|k| k.name == key.name) {
            return self.error(
                format!("{constraint} was dropped and recreated upstream"),
                self.recreate_hint(
                    "To keep ingesting without this constraint",
                    Some(&exclude(&key.name)),
                ),
            );
        }
        self.error(
            format!("{constraint} was dropped upstream"),
            format!(
                "{}\nTo make a planned constraint drop a non-event, create the table with \
                 WITH ({}) before the upstream drop.",
                self.recreate_hint("To keep ingesting without this constraint", None),
                exclude(&key.name),
            ),
        )
    }

    fn error(&self, change: String, hint: String) -> SchemaChangeError {
        SchemaChangeError {
            table: format!("{}.{}", self.namespace, self.name),
            change,
            hint,
        }
    }

    fn recreate_hint(&self, lead: &str, with_clause: Option<&str>) -> String {
        recreate_hint(
            lead,
            &self.name,
            &format!("{}.{}", self.namespace, self.name),
            with_clause,
        )
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
