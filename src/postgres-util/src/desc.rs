// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Descriptions of PostgreSQL objects.

use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};

use mz_proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_postgres_util.desc.rs"));

/// Describes a table in a PostgreSQL database.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresTableDesc {
    /// The OID of the table.
    pub oid: u32,
    /// The name of the schema that the table belongs to.
    pub namespace: String,
    /// The name of the table.
    pub name: String,
    /// The description of each column, in order of their position in the table.
    pub columns: Vec<PostgresColumnDesc>,
}

impl RustType<ProtoPostgresTableDesc> for PostgresTableDesc {
    fn into_proto(&self) -> ProtoPostgresTableDesc {
        ProtoPostgresTableDesc {
            oid: self.oid,
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.into_proto()).collect(),
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
        })
    }
}

impl Arbitrary for PostgresTableDesc {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<String>(),
            any::<u32>(),
            any::<Vec<PostgresColumnDesc>>(),
        )
            .prop_map(|(name, namespace, oid, columns)| PostgresTableDesc {
                name,
                namespace,
                oid,
                columns,
            })
            .boxed()
    }
}

/// Describes a column in a [`PostgresTableDesc`].
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The column's monotonic position in its table, i.e. "this was the _i_th
    /// column created" irrespective of the current number of columns.
    // TODO(migration): remove option in version v.50 (released in v0.48 + 1
    // additional release)
    pub col_num: Option<u16>,
    /// The OID of the column's type.
    pub type_oid: u32,
    /// The modifier for the column's type.
    pub type_mod: i32,
    /// True if the column lacks a `NOT NULL` constraint.
    pub nullable: bool,
    /// Whether the column is part of the table's primary key.
    ///
    /// TODO(benesch): this doesn't look descriptive enough. The order of the
    /// columns in the primary key matters too.
    pub primary_key: bool,
}

impl RustType<ProtoPostgresColumnDesc> for PostgresColumnDesc {
    fn into_proto(&self) -> ProtoPostgresColumnDesc {
        ProtoPostgresColumnDesc {
            name: self.name.clone(),
            col_num: self.col_num.map(|c| c.into()),
            type_oid: self.type_oid,
            type_mod: self.type_mod,
            nullable: self.nullable,
            primary_key: self.primary_key,
        }
    }

    fn from_proto(proto: ProtoPostgresColumnDesc) -> Result<Self, TryFromProtoError> {
        Ok(PostgresColumnDesc {
            name: proto.name,
            col_num: proto
                .col_num
                .map(|c| c.try_into().expect("values roundtrip")),
            type_oid: proto.type_oid,
            type_mod: proto.type_mod,
            nullable: proto.nullable,
            primary_key: proto.primary_key,
        })
    }
}

impl Arbitrary for PostgresColumnDesc {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<u16>(),
            any::<u32>(),
            any::<i32>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(name, col_num, type_oid, type_mod, nullable, primary_key)| PostgresColumnDesc {
                    name,
                    col_num: Some(col_num),
                    type_oid,
                    type_mod,
                    nullable,
                    primary_key,
                },
            )
            .boxed()
    }
}
