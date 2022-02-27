// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides convenience functions for working with upstream Postgres sources from the `sql` package.

use anyhow::{anyhow, bail};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::{Client, Config};

use mz_ore::task;
use mz_pgrepr::Type as PgType;

/// The schema of a single column
pub struct PgColumn {
    pub name: String,
    pub ty: PgType,
    pub nullable: bool,
    pub primary_key: bool,
}

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

/// Creates a TLS connector for the given [`Config`].
pub fn make_tls(config: &Config) -> Result<MakeTlsConnector, anyhow::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate_file(ssl_cert, SslFiletype::PEM)?;
            builder.set_private_key_file(ssl_key, SslFiletype::PEM)?;
        }
        (None, Some(_)) => bail!("must provide both sslcert and sslkey, but only provided sslkey"),
        (Some(_), None) => bail!("must provide both sslcert and sslkey, but only provided sslcert"),
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder.set_ca_file(ssl_root_cert)?
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
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
    task::spawn(|| format!("postgres_publication_info:{conn}"), connection);

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow!("publication {:?} does not exist", publication))?;

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
                        a.atttypmod AS typmod,
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
                let typmod: i32 = row.get("typmod");
                let ty = match PgType::from_oid_and_typmod(oid, typmod) {
                    Some(ty) => ty,
                    None => bail!("unknown type with OID {}", oid),
                };
                let not_null: bool = row.get("not_null");
                let primary_key = row.get("primary_key");
                Ok(PgColumn {
                    name,
                    ty,
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
    task::spawn(
        || format!("postgres_drop_replication_slots:{conn}"),
        connection,
    );

    let replication_client = connect_replication(conn).await?;
    for slot in slots {
        let rows = client
            .query(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
                &[&slot],
            )
            .await?;
        match rows.len() {
            0 => {
                // DROP_REPLICATION_SLOT will error if the slot does not exist
                // todo@jldlaughlin: don't let invalid Postgres sources ship!
                continue;
            }
            1 => {
                replication_client
                    .simple_query(&format!("DROP_REPLICATION_SLOT {} WAIT", slot))
                    .await?;
            }
            _ => {
                return Err(anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                ))
            }
        }
    }
    Ok(())
}

/// Starts a replication connection to the upstream database
pub async fn connect_replication(conn: &str) -> Result<Client, anyhow::Error> {
    let mut config: Config = conn.parse()?;
    let tls = make_tls(&config)?;
    let (client, connection) = config
        .replication_mode(ReplicationMode::Logical)
        .connect(tls)
        .await?;
    task::spawn(
        || format!("postgres_connect_replication:{conn}"),
        connection,
    );
    Ok(client)
}
