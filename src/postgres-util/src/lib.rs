// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides convenience functions for working with upstream Postgres sources from the `sql` package.

use std::fmt;

use anyhow::anyhow;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::NoTls;

use sql_parser::ast::display::{AstDisplay, AstFormatter};
use sql_parser::impl_display;

/// The schema of a single column
pub struct PgColumn {
    pub name: String,
    pub scalar_type: PgType,
    pub nullable: bool,
    pub primary_key: bool,
}

impl AstDisplay for PgColumn {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(&self.name);
        f.write_str(" ");
        f.write_str(&self.scalar_type);
        if self.primary_key {
            f.write_str(" PRIMARY KEY");
        }
        if self.nullable {
            f.write_str(" NULL");
        } else {
            f.write_str(" NOT NULL");
        }
    }
}
impl_display!(PgColumn);

/// Information about a remote table
pub struct TableInfo {
    /// The OID of the table
    pub rel_id: u32,
    /// The namespace the table belongs to
    pub namespace: String,
    /// The name of the table
    pub name: String,
    /// The schema of each column, in order
    pub schema: Vec<PgColumn>,
}

/// Fetches table schema information from an upstream Postgres source for all tables that are part
/// of a publication, given a connection string and the publication name.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    conn: &str,
    publication: &str,
) -> Result<Vec<TableInfo>, anyhow::Error> {
    let (client, connection) = tokio_postgres::connect(&conn, NoTls).await?;
    tokio::spawn(connection);

    let tables = client
        .query(
            "SELECT
                c.oid, p.schemaname, p.tablename
            FROM
                pg_catalog.pg_class AS c
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_publication_tables AS p ON
                        c.relname = p.tablename AND n.nspname = p.schemaname
            WHERE
                p.pubname = $1",
            &[&publication],
        )
        .await?;

    let mut table_infos = vec![];
    for row in tables {
        let rel_id = row.get("oid");

        let schema = client
            .query(
                "SELECT
                        a.attname,
                        a.atttypid,
                        a.attnotnull,
                        b.oid IS NOT NULL
                    FROM pg_catalog.pg_attribute a
                    LEFT JOIN pg_catalog.pg_constraint b
                        ON a.attrelid = b.conrelid
                        AND b.contype = 'p'
                        AND a.attnum = ANY (b.conkey)
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
                let primary_key = row.get(3);
                Ok(PgColumn {
                    name,
                    scalar_type,
                    nullable,
                    primary_key,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        table_infos.push(TableInfo {
            rel_id,
            namespace: row.get("schemaname"),
            name: row.get("tablename"),
            schema,
        });
    }

    Ok(table_infos)
}
