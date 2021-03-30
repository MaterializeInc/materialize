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

use tokio_postgres::types::Type as PgType;
use tokio_postgres::NoTls;

/// The schema of a single column
pub struct PgColumn {
    name: String,
    scalar_type: PgType,
    nullable: bool,
}

/// Fetches column information from an upstream Postgres source, given
/// a connection string, a namespace, and a target table.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream table does not exist or contains invalid values.
pub async fn fetch_columns(
    conn: &str,
    namespace: &str,
    table: &str,
) -> Result<Vec<PgColumn>, anyhow::Error> {
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

    // todo@jldlaughlin: fetch all constraints, so we correctly error in `plan_create_source` if they
    // are present.
    Ok(client
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
        .collect::<Result<Vec<_>, anyhow::Error>>()?)
}

/// Stringifies `PgColumn` information to appear as they would have been written in text.
pub fn format_columns(columns: Vec<PgColumn>) -> String {
    let nullable = |nullable| {
        if nullable {
            "NULL"
        } else {
            "NOT NULL"
        }
    };

    let mut formatted_columns = Vec::with_capacity(columns.len());
    for c in columns {
        formatted_columns.push(format!(
            "{} {} {}",
            c.name,
            c.scalar_type,
            nullable(c.nullable)
        ));
    }
    format!("({})", formatted_columns.join(","))
}
