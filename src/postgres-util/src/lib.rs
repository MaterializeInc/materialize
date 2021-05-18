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
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::{Client, Config};

use sql_parser::ast::display::{AstDisplay, AstFormatter};
use sql_parser::impl_display;

pub enum PgScalarType {
    Simple(PgType),
    Numeric { precision: u16, scale: u16 },
    NumericArray { precision: u16, scale: u16 },
}

impl AstDisplay for PgScalarType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Simple(typ) => {
                f.write_str(typ);
            }
            Self::Numeric { precision, scale } => {
                f.write_str("numeric(");
                f.write_str(precision);
                f.write_str(", ");
                f.write_str(scale);
                f.write_str(")");
            }
            Self::NumericArray { precision, scale } => {
                f.write_str("numeric(");
                f.write_str(precision);
                f.write_str(", ");
                f.write_str(scale);
                f.write_str(")[]");
            }
        }
    }
}
impl_display!(PgScalarType);

/// The schema of a single column
pub struct PgColumn {
    pub name: String,
    pub scalar_type: PgScalarType,
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

/// Creates a TLS connector that respects the Postgres' connector Config, in particular `sslmode`.
pub fn make_tls(_config: &Config) -> Result<MakeTlsConnector, anyhow::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // Currently supported sslmodes (disable, prefer, require) don't verify peer certificates.
    // todo(uce): add additional sslmodes (see #6716)
    builder.set_verify(SslVerifyMode::NONE);
    Ok(MakeTlsConnector::new(builder.build()))
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
    let config = conn.parse()?;
    let tls = make_tls(&config)?;
    let (client, connection) = config.connect(tls).await?;
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
                        a.attname AS name,
                        a.atttypid AS oid,
                        a.atttypmod AS modifier,
                        a.attnotnull AS not_null,
                        b.oid IS NOT NULL AS primary_key
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
                let name: String = row.get("name");
                let oid = row.get("oid");
                let pg_type =
                    PgType::from_oid(oid).ok_or_else(|| anyhow!("unknown type OID: {}", oid))?;
                let scalar_type = match pg_type {
                    typ @ PgType::NUMERIC | typ @ PgType::NUMERIC_ARRAY => {
                        let modifier: i32 = row.get("modifier");
                        // https://github.com/postgres/postgres/blob/REL_13_3/src/backend/utils/adt/numeric.c#L978-L983
                        let tmp_mod = modifier - 4;
                        let scale = (tmp_mod & 0xffff) as u16;
                        let precision = ((tmp_mod >> 16) & 0xffff) as u16;

                        if typ == PgType::NUMERIC {
                            PgScalarType::Numeric { scale, precision }
                        } else {
                            PgScalarType::NumericArray { scale, precision }
                        }
                    }
                    other => PgScalarType::Simple(other),
                };
                let not_null: bool = row.get("not_null");
                let primary_key = row.get("primary_key");
                Ok(PgColumn {
                    name,
                    scalar_type,
                    nullable: !not_null,
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

pub async fn drop_replication_slots(conn: &str, slots: &[String]) -> Result<(), anyhow::Error> {
    let config = conn.parse()?;
    let tls = make_tls(&config)?;
    let (client, connection) = tokio_postgres::connect(&conn, tls).await?;
    tokio::spawn(connection);

    for slot in slots {
        let active_pid = query_pg_replication_slots(&client, slot).await?;
        if let Some(pid) = active_pid {
            client
                .query("SELECT pg_terminate_backend($1)", &[&pid])
                .await?;
        }
        client
            .query("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await?;
    }
    Ok(())
}

pub async fn query_pg_replication_slots(
    client: &Client,
    slot: &str,
) -> Result<Option<i32>, anyhow::Error> {
    let row = client
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
            &[&slot],
        )
        .await?;
    Ok(row.get(0))
}
