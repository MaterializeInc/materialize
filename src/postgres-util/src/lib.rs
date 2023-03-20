// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::collapsible_if)]
#![warn(clippy::collapsible_else_if)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! PostgreSQL utility library.

use std::time::Duration;

use anyhow::anyhow;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{Host, ReplicationMode, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::Client;
use tracing::warn;

use mz_ore::task;
use mz_repr::GlobalId;
use mz_ssh_util::tunnel::SshTunnelConfig;

use crate::desc::{PostgresColumnDesc, PostgresKeyDesc, PostgresTableDesc};

pub mod desc;

/// An error representing pg, ssh, ssl, and other failures.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// Any other error we bail on.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// Error using ssh.
    #[error(transparent)]
    Ssh(#[from] openssh::Error),
    /// Error doing io to setup an ssh connection.
    #[error(transparent)]
    SshIo(#[from] std::io::Error),
    /// A postgres error.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// Error setting up postgres ssl.
    #[error(transparent)]
    PostgresSsl(#[from] openssl::error::ErrorStack),
}

macro_rules! bail_generic {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($fmt, $($arg)*)))
    };
    ($err:expr $(,)?) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($err)))
    };
}

/// Creates a TLS connector for the given [`Config`].
pub fn make_tls(config: &tokio_postgres::Config) -> Result<MakeTlsConnector, PostgresError> {
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
            builder.set_certificate(&*X509::from_pem(ssl_cert)?)?;
            builder.set_private_key(&*PKey::private_key_from_pem(ssl_key)?)?;
        }
        (None, Some(_)) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslkey")
        }
        (Some(_), None) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslcert")
        }
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder
            .cert_store_mut()
            .add_cert(X509::from_pem(ssl_root_cert)?)?;
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

/// Fetches table schema information from an upstream Postgres source for all
/// tables that are part of a publication, given a connection string and the
/// publication name.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    config: &Config,
    publication: &str,
) -> Result<Vec<PostgresTableDesc>, PostgresError> {
    let client = config.connect("postgres_publication_info").await?;

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("publication {:?} does not exist", publication))?;

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
        let oid = row.get("oid");

        let columns = client
            .query(
                "SELECT
                        a.attname AS name,
                        a.atttypid AS typoid,
                        a.attnum AS colnum,
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
                &[&oid],
            )
            .await?
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let type_oid = row.get("typoid");
                let col_num = Some(
                    row.get::<_, i16>("colnum")
                        .try_into()
                        .expect("non-negative values"),
                );
                let type_mod: i32 = row.get("typmod");
                let not_null: bool = row.get("not_null");
                Ok(PostgresColumnDesc {
                    name,
                    col_num,
                    type_oid,
                    type_mod,
                    nullable: !not_null,
                })
            })
            .collect::<Result<Vec<_>, PostgresError>>()?;

        // PG 15 adds UNIQUE NULLS NOT DISTINCT, which is a feature we cannot
        // support: NULL = NULL must be true in Materialize for the sake of
        // joins.
        let pg_15_plus_keys = "
        SELECT
            pg_constraint.oid,
            pg_constraint.conkey,
            pg_constraint.conname,
            pg_constraint.contype = 'p' AS is_primary
        FROM
            pg_constraint
                JOIN
                    pg_index
                    ON pg_index.indexrelid = pg_constraint.conindid
        WHERE
            pg_constraint.conrelid = $1
                AND
            pg_constraint.contype =ANY (ARRAY['p', 'u'])
                AND
            NOT pg_index.indnullsnotdistinct;";

        // As above but for versions of PG without indnullsnotdistinct.
        let pg_14_minus_keys = "
        SELECT
            pg_constraint.oid,
            pg_constraint.conkey,
            pg_constraint.conname,
            pg_constraint.contype = 'p' AS is_primary
        FROM pg_constraint
        WHERE
            pg_constraint.conrelid = $1
                AND
            pg_constraint.contype =ANY (ARRAY['p', 'u']);";

        let keys = match client.query(pg_15_plus_keys, &[&oid]).await {
            Ok(keys) => keys,
            Err(e)
                // PG versions prior to 15 do not contain this column.
                if e.to_string()
                    == "db error: ERROR: column pg_index.indnullsnotdistinct does not exist" =>
            {
                client.query(pg_14_minus_keys, &[&oid]).await?
            }
            e => e?,
        };

        let keys = keys
            .into_iter()
            .map(|row| {
                let oid: u32 = row.get("oid");
                let cols: Vec<i16> = row.get("conkey");
                let name: String = row.get("conname");
                let is_primary: bool = row.get("is_primary");
                let cols = cols
                    .into_iter()
                    .map(|col| u16::try_from(col).expect("non-negative colnums"))
                    .collect();
                PostgresKeyDesc {
                    oid,
                    name,
                    cols,
                    is_primary,
                }
            })
            .collect();

        table_infos.push(PostgresTableDesc {
            oid,
            namespace: row.get("schemaname"),
            name: row.get("tablename"),
            columns,
            keys,
        });
    }

    Ok(table_infos)
}

pub async fn drop_replication_slots(config: Config, slots: &[&str]) -> Result<(), PostgresError> {
    let client = config.connect("postgres_drop_replication_slots").await?;
    let replication_client = config.connect_replication().await?;
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
                return Err(PostgresError::Generic(anyhow::anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                )))
            }
        }
    }
    Ok(())
}

/// Configures an optional tunnel for use when connecting to a PostgreSQL
/// database.
#[derive(Debug, PartialEq, Clone)]
pub enum TunnelConfig {
    /// Establish a direct TCP connection to the database host.
    Direct,
    /// Establish a TCP connection to the database via an SSH tunnel.
    /// This means first establishing an SSH connection to a bastion host,
    /// and then opening a separate connection from that host to the database.
    /// This is commonly referred by vendors as a "direct SSH tunnel", in
    /// opposition to "reverse SSH tunnel", which is currently unsupported.
    Ssh(SshTunnelConfig),
    /// Establish a TCP connection to the database via an AWS PrivateLink
    /// service.
    AwsPrivatelink {
        /// The ID of the AWS PrivateLink service.
        connection_id: GlobalId,
    },
}

/// Configuration for PostgreSQL connections.
///
/// This wraps [`tokio_postgres::Config`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Debug, PartialEq, Clone)]
pub struct Config {
    inner: tokio_postgres::Config,
    tunnel: TunnelConfig,
}

impl Config {
    pub fn new(inner: tokio_postgres::Config, tunnel: TunnelConfig) -> Result<Self, PostgresError> {
        let config = Self { inner, tunnel };

        // Early validate that the configuration contains only a single TCP
        // server.
        config.address()?;

        Ok(config)
    }

    /// Connects to the configured PostgreSQL database.
    pub async fn connect(&self, task_name: &str) -> Result<Client, PostgresError> {
        self.connect_internal(task_name, |_| ()).await
    }

    /// Starts a replication connection to the configured PostgreSQL database.
    pub async fn connect_replication(&self) -> Result<Client, PostgresError> {
        self.connect_internal("postgres_connect_replication", |config| {
            config
                .replication_mode(ReplicationMode::Logical)
                .connect_timeout(Duration::from_secs(30))
                .keepalives_idle(Duration::from_secs(10 * 60));
        })
        .await
    }

    fn address(&self) -> Result<(&str, u16), PostgresError> {
        match (self.inner.get_hosts(), self.inner.get_ports()) {
            ([Host::Tcp(host)], [port]) => Ok((host, *port)),
            _ => bail_generic!("only TCP connections to a single PostgreSQL server are supported"),
        }
    }

    async fn connect_internal<F>(
        &self,
        task_name: &str,
        configure: F,
    ) -> Result<Client, PostgresError>
    where
        F: FnOnce(&mut tokio_postgres::Config),
    {
        let mut postgres_config = self.inner.clone();
        configure(&mut postgres_config);
        let mut tls = make_tls(&postgres_config)?;
        match &self.tunnel {
            TunnelConfig::Direct => {
                let (client, connection) = postgres_config.connect(tls).await?;
                task::spawn(|| task_name, connection);
                Ok(client)
            }
            TunnelConfig::Ssh(tunnel) => {
                let (host, port) = self.address()?;
                let (session, local_port) = tunnel.connect(host, port).await?;
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect(("localhost", local_port)).await?;
                let (client, connection) = postgres_config.connect_raw(tcp_stream, tls).await?;
                task::spawn(|| task_name, async {
                    if let Err(e) = connection.await {
                        warn!("postgres connection failed: {e}");
                    }
                    if let Err(e) = session.close().await {
                        // Convert to `anyhow::Error` to include error source
                        // via the alternate `Display` implementation, which can
                        // contain key details about the nature of the failure.
                        warn!("failed to close ssh tunnel: {:#}", anyhow!(e))
                    }
                });
                Ok(client)
            }
            TunnelConfig::AwsPrivatelink { connection_id } => {
                let (host, port) = self.address()?;
                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect((privatelink_host, port)).await?;
                let (client, connection) = postgres_config.connect_raw(tcp_stream, tls).await?;
                task::spawn(|| task_name, connection);
                Ok(client)
            }
        }
    }
}
