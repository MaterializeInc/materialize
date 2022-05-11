// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.postgres_source.rs"
));

use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy};

use mz_postgres_util::{PgColumn, TableInfo};

impl From<PgColumn> for PostgresColumn {
    fn from(c: PgColumn) -> PostgresColumn {
        PostgresColumn {
            name: c.name,
            type_oid: c.oid.try_into().unwrap(),
            type_mod: c.typmod,
            nullable: c.nullable,
            primary_key: c.primary_key,
        }
    }
}

impl From<PostgresColumn> for PgColumn {
    fn from(c: PostgresColumn) -> PgColumn {
        PgColumn {
            name: c.name,
            oid: c.type_oid.try_into().unwrap(),
            typmod: c.type_mod,
            nullable: c.nullable,
            primary_key: c.primary_key,
        }
    }
}

impl From<TableInfo> for PostgresTable {
    fn from(t: TableInfo) -> PostgresTable {
        PostgresTable {
            name: t.name,
            namespace: t.namespace,
            relation_id: t.rel_id,
            columns: t.schema.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<PostgresTable> for TableInfo {
    fn from(t: PostgresTable) -> TableInfo {
        TableInfo {
            name: t.name,
            namespace: t.namespace,
            rel_id: t.relation_id,
            schema: t.columns.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Arbitrary for PostgresColumn {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<i32>(),
            any::<i32>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(name, type_oid, type_mod, nullable, primary_key)| PostgresColumn {
                    name,
                    type_oid,
                    type_mod,
                    nullable,
                    primary_key,
                },
            )
            .boxed()
    }
}

impl Arbitrary for PostgresTable {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<String>(),
            any::<u32>(),
            any::<Vec<PostgresColumn>>(),
        )
            .prop_map(|(name, namespace, relation_id, columns)| PostgresTable {
                name,
                namespace,
                relation_id,
                columns,
            })
            .boxed()
    }
}

impl Arbitrary for PostgresSourceDetails {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<Vec<PostgresTable>>(), any::<String>())
            .prop_map(|(tables, slot)| PostgresSourceDetails { tables, slot })
            .boxed()
    }
}
