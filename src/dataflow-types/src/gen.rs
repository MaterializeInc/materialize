// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

use crate::postgres_source::{PostgresColumn, PostgresTable};
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
