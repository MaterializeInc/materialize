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
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::SqlColumnType;
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use self::proto_my_sql_column_desc::Meta;

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
pub struct MySqlColumnMetaEnum {
    #[proptest(strategy = "proptest::collection::vec(any::<String>(), 0..3)")]
    pub values: Vec<String>,
}

impl RustType<ProtoMySqlColumnMetaEnum> for MySqlColumnMetaEnum {
    fn into_proto(&self) -> ProtoMySqlColumnMetaEnum {
        ProtoMySqlColumnMetaEnum {
            values: self.values.clone(),
        }
    }

    fn from_proto(proto: ProtoMySqlColumnMetaEnum) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            values: proto.values,
        })
    }
}

trait IsCompatible {
    fn is_compatible(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum MySqlColumnMeta {
    /// The described column is an enum, with the given possible values.
    Enum(MySqlColumnMetaEnum),
    /// The described column is a json value.
    Json,
    /// The described column is a year value.
    Year,
    /// The described column is a date value.
    Date,
    /// The described column is a timestamp value with a set precision.
    Timestamp(u32),
    /// The described column is a `bit` column, with the given possibly precision.
    Bit(u32),
}

impl IsCompatible for Option<MySqlColumnMeta> {
    fn is_compatible(&self, other: &Option<MySqlColumnMeta>) -> bool {
        match (self, other) {
            (None, None) => true,
            (Some(_), None) => false,
            (None, Some(_)) => false,
            (Some(MySqlColumnMeta::Enum(self_enum)), Some(MySqlColumnMeta::Enum(other_enum))) => {
                // so as long as `self.values` is a compatible prefix of `other.values`, we can
                // ignore extra values from `other.values`.
                match other_enum.values.get(0..self_enum.values.len()) {
                    Some(prefix) => self_enum.values == prefix,
                    None => false,
                }
            }
            (Some(MySqlColumnMeta::Json), Some(MySqlColumnMeta::Json)) => true,
            (Some(MySqlColumnMeta::Year), Some(MySqlColumnMeta::Year)) => true,
            (Some(MySqlColumnMeta::Date), Some(MySqlColumnMeta::Date)) => true,
            // Timestamps are compatible as long as we don't lose precision
            (
                Some(MySqlColumnMeta::Timestamp(precision)),
                Some(MySqlColumnMeta::Timestamp(other_precision)),
            ) => precision <= other_precision,
            // We always cast bit columns to u64's and the max precision of a bit column
            // is 64 bits, so any bit column is always compatible with another.
            (Some(MySqlColumnMeta::Bit(_)), Some(MySqlColumnMeta::Bit(_))) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The intended data type of this column within Materialize
    /// If this is None, the column is intended to be skipped within Materialize
    pub column_type: Option<SqlColumnType>,
    /// Optional metadata about the column that may be necessary for decoding
    pub meta: Option<MySqlColumnMeta>,
}

impl RustType<ProtoMySqlColumnDesc> for MySqlColumnDesc {
    fn into_proto(&self) -> ProtoMySqlColumnDesc {
        ProtoMySqlColumnDesc {
            name: self.name.clone(),
            column_type: self.column_type.into_proto(),
            meta: self.meta.as_ref().and_then(|meta| match meta {
                MySqlColumnMeta::Enum(e) => Some(Meta::Enum(e.into_proto())),
                MySqlColumnMeta::Json => Some(Meta::Json(ProtoMySqlColumnMetaJson {})),
                MySqlColumnMeta::Year => Some(Meta::Year(ProtoMySqlColumnMetaYear {})),
                MySqlColumnMeta::Date => Some(Meta::Date(ProtoMySqlColumnMetaDate {})),
                MySqlColumnMeta::Timestamp(precision) => {
                    Some(Meta::Timestamp(ProtoMySqlColumnMetaTimestamp {
                        precision: *precision,
                    }))
                }
                MySqlColumnMeta::Bit(precision) => Some(Meta::Bit(ProtoMySqlColumnMetaBit {
                    precision: *precision,
                })),
            }),
        }
    }

    fn from_proto(proto: ProtoMySqlColumnDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            name: proto.name,
            column_type: proto.column_type.into_rust()?,
            meta: proto
                .meta
                .and_then(|meta| match meta {
                    Meta::Enum(e) => Some(
                        MySqlColumnMetaEnum::from_proto(e)
                            .and_then(|e| Ok(MySqlColumnMeta::Enum(e))),
                    ),
                    Meta::Json(_) => Some(Ok(MySqlColumnMeta::Json)),
                    Meta::Year(_) => Some(Ok(MySqlColumnMeta::Year)),
                    Meta::Date(_) => Some(Ok(MySqlColumnMeta::Date)),
                    Meta::Timestamp(e) => Some(Ok(MySqlColumnMeta::Timestamp(e.precision))),
                    Meta::Bit(e) => Some(Ok(MySqlColumnMeta::Bit(e.precision))),
                })
                .transpose()?,
        })
    }
}

impl IsCompatible for MySqlColumnDesc {
    /// Determines if two `MySqlColumnDesc` are compatible with one another in
    /// a way that Materialize can handle.
    fn is_compatible(&self, other: &MySqlColumnDesc) -> bool {
        self.name == other.name
            && match (&self.column_type, &other.column_type) {
                (None, None) => true,
                (Some(self_type), Some(other_type)) => {
                    self_type.scalar_type == other_type.scalar_type
                    // Columns are compatible if:
                    // - self is nullable; introducing a not null constraint doesn't
                    //   change this column's behavior.
                    // - self and other are both not nullable
                    && (self_type.nullable || self_type.nullable == other_type.nullable)
                }
                (Some(_), None) => false,
                (None, Some(_)) => false,
            }
            // Ensure any column metadata is compatible
            && self.meta.is_compatible(&other.meta)
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
