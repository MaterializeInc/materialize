// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use anyhow::bail;
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::ColumnType;

include!(concat!(env!("OUT_DIR"), "/mz_mysql_util.rs"));

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlTableDesc {
    /// In MySQL the schema and database of a table are synonymous.
    pub schema_name: String,
    /// The name of the table.
    pub name: String,
    /// Columns for the table
    ///
    /// The index of each column is based on its `ordinal_position`
    /// reported by the information_schema.columns table, which defines
    /// the order of column values when received in a row.
    #[proptest(strategy = "proptest::collection::vec(any::<MySqlColumnDesc>(), 0..4)")]
    pub columns: Vec<MySqlColumnDesc>,
    /// Applicable keys for this table (i.e. primary key and unique
    /// constraints).
    #[proptest(strategy = "proptest::collection::btree_set(any::<MySqlKeyDesc>(), 0..4)")]
    pub keys: BTreeSet<MySqlKeyDesc>,
}

impl RustType<ProtoMySqlTableDesc> for MySqlTableDesc {
    fn into_proto(&self) -> ProtoMySqlTableDesc {
        ProtoMySqlTableDesc {
            schema_name: self.schema_name.clone(),
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.into_proto()).collect(),
            keys: self.keys.iter().map(|c| c.into_proto()).collect(),
        }
    }

    fn from_proto(proto: ProtoMySqlTableDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            schema_name: proto.schema_name,
            name: proto.name,
            columns: proto
                .columns
                .into_iter()
                .map(MySqlColumnDesc::from_proto)
                .collect::<Result<_, _>>()?,
            keys: proto
                .keys
                .into_iter()
                .map(MySqlKeyDesc::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl MySqlTableDesc {
    /// Determines if two `MySqlTableDesc` are compatible with one another in
    /// a way that Materialize can handle.
    ///
    /// Currently this means that the values are equal except for the following
    /// exceptions:
    /// - `self`'s columns are a prefix of `other`'s columns.
    /// - `self`'s keys are all present in `other`
    pub fn determine_compatibility(&self, other: &MySqlTableDesc) -> Result<(), anyhow::Error> {
        if self == other {
            return Ok(());
        }

        if self.schema_name != other.schema_name || self.name != other.name {
            bail!(
                "table name mismatch: self: {}.{}, other: {}.{}",
                self.schema_name,
                self.name,
                other.schema_name,
                other.name
            );
        }

        // `columns` is ordered by the ordinal_position of each column in the table,
        // so as long as `self.columns` is a compatible prefix of `other.columns`, we can
        // ignore extra columns from `other.columns`.
        let mut other_columns = other.columns.iter();
        for self_column in &self.columns {
            let other_column = other_columns.next().ok_or_else(|| {
                anyhow::anyhow!(
                    "column {} no longer present in table {}",
                    self_column.name,
                    self.name
                )
            })?;
            if !self_column.is_compatible(other_column) {
                bail!(
                    "column {} in table {} has been altered",
                    self_column.name,
                    self.name
                );
            }
        }

        // Our keys are all still present in exactly the same shape.
        // TODO: Implement a more relaxed key compatibility check:
        // We should check that for all keys that we know about there exists an upstream key whose
        // set of columns is a subset of the set of columns of the key we know about. For example
        // if we had previously discovered that the table had two compound unique keys, key1 made
        // up of columns (a, b) and key2 made up of columns (a, c) but now the table only has a
        // single unique key of just the column a then it's compatible because {a} ⊆ {a, b} and
        // {a} ⊆ {a, c}.
        if self.keys.difference(&other.keys).next().is_some() {
            bail!(
                "keys in table {} have been altered: self: {:?}, other: {:?}",
                self.name,
                self.keys,
                other.keys
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The MySQL datatype of the column.
    pub column_type: ColumnType,
}

impl RustType<ProtoMySqlColumnDesc> for MySqlColumnDesc {
    fn into_proto(&self) -> ProtoMySqlColumnDesc {
        ProtoMySqlColumnDesc {
            name: self.name.clone(),
            column_type: Some(self.column_type.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMySqlColumnDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            name: proto.name,
            column_type: proto
                .column_type
                .into_rust_if_some("ProtoMySqlColumnDesc::column_type")?,
        })
    }
}

impl MySqlColumnDesc {
    /// Determines if two `MySqlColumnDesc` are compatible with one another in
    /// a way that Materialize can handle.
    pub fn is_compatible(&self, other: &MySqlColumnDesc) -> bool {
        self.name == other.name
            && self.column_type.scalar_type == other.column_type.scalar_type
            // Columns are compatible if:
            // - self is nullable; introducing a not null constraint doesn't
            //   change this column's behavior.
            // - self and other are both not nullable
            && (self.column_type.nullable || self.column_type.nullable == other.column_type.nullable)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd, Arbitrary)]
pub struct MySqlKeyDesc {
    /// The name of the index.
    pub name: String,
    /// Whether or not this key is the primary key.
    pub is_primary: bool,
    /// The columns that make up the key.
    #[proptest(strategy = "proptest::collection::vec(any::<String>(), 0..4)")]
    pub columns: Vec<String>,
}

impl RustType<ProtoMySqlKeyDesc> for MySqlKeyDesc {
    fn into_proto(&self) -> ProtoMySqlKeyDesc {
        ProtoMySqlKeyDesc {
            name: self.name.clone(),
            is_primary: self.is_primary.clone(),
            columns: self.columns.clone(),
        }
    }

    fn from_proto(proto: ProtoMySqlKeyDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            name: proto.name,
            is_primary: proto.is_primary,
            columns: proto.columns,
        })
    }
}
