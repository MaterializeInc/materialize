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

use anyhow::bail;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Oid;
use tracing::warn;

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
    /// On incompatibility, the error describes the first mismatch found and,
    /// where possible, how to recover from it. The error text becomes the
    /// permanent, user-visible error for the stalled table, so it must stand
    /// on its own.
    pub fn determine_compatibility(
        &self,
        other: &PostgresTableDesc,
        allow_type_to_change_by_col_num: &BTreeSet<u16>,
    ) -> Result<(), anyhow::Error> {
        if self == other {
            return Ok(());
        }

        let result = self.diff_incompatibility(other, allow_type_to_change_by_col_num);
        if result.is_err() {
            warn!(
                "Error validating table in publication. Expected: {:?} Actual: {:?}",
                &self, other
            );
        }
        result
    }

    /// Reports the first incompatibility between `self` (the schema captured
    /// when the Materialize table was created) and `other` (the current
    /// upstream schema), or `Ok(())` if `other` is a compatible evolution of
    /// `self`.
    fn diff_incompatibility(
        &self,
        other: &PostgresTableDesc,
        allow_type_to_change_by_col_num: &BTreeSet<u16>,
    ) -> Result<(), anyhow::Error> {
        let table = format!("{}.{}", self.namespace, self.name);

        if self.oid != other.oid || self.namespace != other.namespace || self.name != other.name {
            bail!(
                "source table {} with oid {} was renamed, dropped, or recreated upstream \
                 (it is now {}.{} with oid {}). Materialize binds a table to the upstream \
                 table's identity and cannot follow this change. To resume ingesting, \
                 recreate the Materialize table against the new upstream table in a new \
                 versioned schema and swap your views to it.",
                table,
                self.oid,
                other.namespace,
                other.name,
                other.oid,
            );
        }

        let other_cols_by_name = BTreeMap::from_iter(other.columns.iter().map(|c| (&c.name, c)));
        for info in &self.columns {
            let Some(other_info) = other_cols_by_name.get(&info.name) else {
                bail!(
                    "column {} of source table {} was dropped or renamed upstream. \
                     To resume ingesting, create a replacement table in a new versioned \
                     schema (its snapshot captures the current upstream schema), swap \
                     your views to it, and drop this table. To make a planned column \
                     drop a non-event, create the replacement table with \
                     WITH (EXCLUDE COLUMNS ({})) before the upstream drop.",
                    quoted(&info.name),
                    table,
                    quoted(&info.name),
                );
            };
            let allow_type_change = allow_type_to_change_by_col_num.contains(&info.col_num);
            if info.is_compatible(other_info, allow_type_change) {
                continue;
            }
            if info.col_num != other_info.col_num {
                bail!(
                    "column {} of source table {} changed position upstream (the column \
                     or table was likely dropped and recreated). To resume ingesting, \
                     recreate the Materialize table in a new versioned schema and swap \
                     your views to it.",
                    quoted(&info.name),
                    table,
                );
            }
            if !allow_type_change
                && (info.type_oid != other_info.type_oid || info.type_mod != other_info.type_mod)
            {
                bail!(
                    "the type of column {} of source table {} changed upstream. To ingest \
                     the column as text regardless of its upstream type, recreate the \
                     Materialize table with WITH (TEXT COLUMNS ({})) in a new versioned \
                     schema and swap your views to it.",
                    quoted(&info.name),
                    table,
                    quoted(&info.name),
                );
            }
            if !info.nullable && other_info.nullable {
                bail!(
                    "the NOT NULL constraint on column {} of source table {} was dropped \
                     upstream. Materialize relies on this constraint and cannot continue \
                     ingesting the table. To resume ingesting, create a replacement table \
                     in a new versioned schema (its snapshot captures the current upstream \
                     schema, where the column is nullable), swap your views to it, and \
                     drop this table. To make planned constraint drops a non-event, create \
                     the replacement table with WITH (EXCLUDE ALL CONSTRAINTS).",
                    quoted(&info.name),
                    table,
                );
            }
            bail!(
                "column {} of source table {} was altered upstream. To resume ingesting, \
                 recreate the Materialize table in a new versioned schema and swap your \
                 views to it.",
                quoted(&info.name),
                table,
            );
        }

        if let Some(key) = self.keys.difference(&other.keys).next() {
            let col_names = key
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
            let kind = if key.is_primary {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            };
            bail!(
                "{} constraint {} ({}) on source table {} was dropped or altered upstream. \
                 Materialize relies on this constraint and cannot continue ingesting the \
                 table. To resume ingesting, create a replacement table in a new versioned \
                 schema (its snapshot captures the current upstream schema, without this \
                 constraint), swap your views to it, and drop this table. To make a \
                 planned constraint drop a non-event, create the replacement table with \
                 WITH (EXCLUDE CONSTRAINTS ('{}')) before the upstream drop.",
                kind,
                quoted(&key.name),
                col_names,
                table,
                key.name,
            );
        }

        Ok(())
    }
}

/// Formats an identifier for inclusion in an error message.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, col_num: u16, nullable: bool) -> PostgresColumnDesc {
        PostgresColumnDesc {
            name: name.to_string(),
            col_num,
            type_oid: 23,
            type_mod: -1,
            nullable,
        }
    }

    fn key(oid: u32, name: &str, cols: Vec<u16>, is_primary: bool) -> PostgresKeyDesc {
        PostgresKeyDesc {
            oid,
            name: name.to_string(),
            cols,
            is_primary,
            nulls_not_distinct: false,
        }
    }

    fn table(columns: Vec<PostgresColumnDesc>, keys: Vec<PostgresKeyDesc>) -> PostgresTableDesc {
        PostgresTableDesc {
            oid: 100,
            namespace: "public".to_string(),
            name: "users".to_string(),
            columns,
            keys: keys.into_iter().collect(),
        }
    }

    #[mz_ore::test]
    fn compatible_evolutions() {
        let desc = table(
            vec![column("id", 1, false)],
            vec![key(200, "users_pkey", vec![1], true)],
        );

        // Identical.
        desc.determine_compatibility(&desc, &BTreeSet::new())
            .unwrap();

        // Extra upstream column and extra upstream key are non-events.
        let mut evolved = desc.clone();
        evolved.columns.push(column("extra", 2, true));
        evolved
            .keys
            .insert(key(201, "users_extra_key", vec![2], false));
        desc.determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap();

        // Upstream SET NOT NULL on a column we recorded as nullable.
        let desc = table(vec![column("id", 1, true)], vec![]);
        let evolved = table(vec![column("id", 1, false)], vec![]);
        desc.determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap();
    }

    #[mz_ore::test]
    fn dropped_key_names_constraint() {
        let desc = table(
            vec![column("id", 1, false), column("wallet", 2, false)],
            vec![
                key(200, "users_pkey", vec![1], true),
                key(201, "users_wallet_id_key", vec![2], false),
            ],
        );
        let mut evolved = desc.clone();
        evolved
            .keys
            .remove(&key(201, "users_wallet_id_key", vec![2], false));

        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("UNIQUE constraint \"users_wallet_id_key\" (wallet)"),
            "{err}"
        );
        assert!(
            err.contains("EXCLUDE CONSTRAINTS ('users_wallet_id_key')"),
            "{err}"
        );

        // Same-name key with a different constraint oid (drop + recreate) also
        // reads as dropped or altered.
        let mut recreated = desc.clone();
        recreated
            .keys
            .remove(&key(200, "users_pkey", vec![1], true));
        recreated.keys.insert(key(300, "users_pkey", vec![1], true));
        let err = desc
            .determine_compatibility(&recreated, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("PRIMARY KEY constraint \"users_pkey\" (id)"),
            "{err}"
        );
    }

    #[mz_ore::test]
    fn column_incompatibilities() {
        let desc = table(vec![column("id", 1, false)], vec![]);

        // Dropped column.
        let evolved = table(vec![column("other", 1, false)], vec![]);
        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("column \"id\" of source table public.users was dropped or renamed"),
            "{err}"
        );

        // DROP NOT NULL.
        let evolved = table(vec![column("id", 1, true)], vec![]);
        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("NOT NULL constraint on column \"id\""),
            "{err}"
        );
        assert!(err.contains("EXCLUDE ALL CONSTRAINTS"), "{err}");

        // Type change, without and with a TEXT COLUMNS exemption.
        let mut evolved = table(vec![column("id", 1, false)], vec![]);
        evolved.columns[0].type_oid = 25;
        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains("the type of column \"id\""), "{err}");
        desc.determine_compatibility(&evolved, &BTreeSet::from([1]))
            .unwrap();

        // Position change.
        let evolved = table(vec![column("id", 3, false)], vec![]);
        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains("changed position upstream"), "{err}");

        // Table renamed or recreated.
        let mut evolved = desc.clone();
        evolved.name = "users_renamed".to_string();
        let err = desc
            .determine_compatibility(&evolved, &BTreeSet::new())
            .unwrap_err()
            .to_string();
        assert!(err.contains("renamed, dropped, or recreated"), "{err}");
    }
}
