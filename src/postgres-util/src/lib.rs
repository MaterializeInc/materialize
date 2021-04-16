// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides convenience functions for working with upstream Postgres sources from the `sql` package.

use anyhow::anyhow;

use sql_parser::ast::display::{AstDisplay, AstFormatter};
use sql_parser::impl_display;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::NoTls;

/// The schema of a single column
pub struct PgColumn {
    pub name: String,
    pub scalar_type: PgType,
    pub nullable: bool,
}

impl AstDisplay for PgColumn {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(&self.name);
        f.write_str(" ");
        f.write_str(&self.scalar_type);
        f.write_str(" ");
        if self.nullable {
            f.write_str("NULL");
        } else {
            f.write_str("NOT NULL");
        }
    }
}
impl_display!(PgColumn);

/// Information about a remote table
pub struct TableInfo {
    /// The OID of the table
    pub rel_id: u32,
    /// The schema of each column, in order
    pub schema: Vec<PgColumn>,
}

/// Fetches column information from an upstream Postgres source, given
/// a connection string, a namespace, and a target table.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream table does not exist or contains invalid values.
pub async fn table_info(
    conn: &str,
    namespace: &str,
    table: &str,
) -> Result<TableInfo, anyhow::Error> {
    let (client, connection) = tokio_postgres::connect(&conn, NoTls).await?;
    tokio::spawn(connection);

    let rel_id: u32 = client
        .query(
            "SELECT c.oid
                FROM pg_catalog.pg_class c
                INNER JOIN pg_catalog.pg_namespace n
                    ON (c.relnamespace = n.oid)
                WHERE n.nspname = $1
                    AND c.relname = $2;",
            &[&namespace, &table],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow!("table not found in the upstream catalog"))?
        .get(0);

    let schema = client
        .query(
            "SELECT a.attname, a.atttypid, a.attnotnull
                FROM pg_catalog.pg_attribute a
                WHERE a.attnum > 0::pg_catalog.int2
                    AND NOT a.attisdropped
                    AND a.attrelid = $1
                ORDER BY a.attnum",
            &[&rel_id],
        )
        .await?
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let oid = row.get(1);
            let scalar_type =
                PgType::from_oid(oid).ok_or_else(|| anyhow!("unknown type OID: {}", oid))?;
            let nullable = !row.get::<_, bool>(2);
            Ok(PgColumn {
                name,
                scalar_type,
                nullable,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(TableInfo { rel_id, schema })
}
