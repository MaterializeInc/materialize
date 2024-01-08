// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::ColumnType;
use proptest::prelude::{any, Arbitrary, Just};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_mysql_util.rs"));

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
    pub columns: Vec<MySqlColumnDesc>,
}

impl RustType<ProtoMySqlTableDesc> for MySqlTableDesc {
    fn into_proto(&self) -> ProtoMySqlTableDesc {
        ProtoMySqlTableDesc {
            schema_name: self.schema_name.clone(),
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.into_proto()).collect(),
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
        })
    }
}

impl Arbitrary for MySqlTableDesc {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<String>(),
            any::<Vec<MySqlColumnDesc>>(),
        )
            .prop_map(|(schema_name, name, columns)| Self {
                schema_name,
                name,
                columns,
            })
            .boxed()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The MySQL datatype of the column.
    pub column_type: ColumnType,
    pub column_key: Option<MySqlColumnKey>,
    // TODO: add more column properties
}

impl RustType<ProtoMySqlColumnDesc> for MySqlColumnDesc {
    fn into_proto(&self) -> ProtoMySqlColumnDesc {
        ProtoMySqlColumnDesc {
            name: self.name.clone(),
            column_type: Some(self.column_type.into_proto()),
            column_key: self.column_key.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMySqlColumnDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            name: proto.name,
            column_type: proto
                .column_type
                .into_rust_if_some("ProtoMySqlColumnDesc::column_type")?,
            column_key: proto
                .column_key
                .map(MySqlColumnKey::from_proto)
                .transpose()?,
        })
    }
}

impl Arbitrary for MySqlColumnDesc {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<ColumnType>(),
            any::<Option<MySqlColumnKey>>(),
        )
            .prop_map(|(name, column_type, column_key)| Self {
                name,
                column_type,
                column_key,
            })
            .boxed()
    }
}

/// Refers to the COLUMN_KEY column of the information_schema.columns table
/// https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MySqlColumnKey {
    PRI,
    UNI,
    MUL,
}

impl RustType<i32> for MySqlColumnKey {
    fn into_proto(&self) -> i32 {
        match self {
            MySqlColumnKey::PRI => ProtoMySqlColumnKey::Pri.into(),
            MySqlColumnKey::UNI => ProtoMySqlColumnKey::Uni.into(),
            MySqlColumnKey::MUL => ProtoMySqlColumnKey::Mul.into(),
        }
    }

    fn from_proto(proto: i32) -> Result<Self, TryFromProtoError> {
        match ProtoMySqlColumnKey::from_i32(proto) {
            Some(ProtoMySqlColumnKey::Pri) => Ok(MySqlColumnKey::PRI),
            Some(ProtoMySqlColumnKey::Uni) => Ok(MySqlColumnKey::UNI),
            Some(ProtoMySqlColumnKey::Mul) => Ok(MySqlColumnKey::MUL),
            None => Err(TryFromProtoError::UnknownEnumVariant(
                "MySqlColumnKey".to_string(),
            )),
        }
    }
}

impl Arbitrary for MySqlColumnKey {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            Just(MySqlColumnKey::PRI),
            Just(MySqlColumnKey::UNI),
            Just(MySqlColumnKey::MUL),
        ])
        .boxed()
    }
}
