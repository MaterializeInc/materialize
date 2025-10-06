// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PostgreSQL utility library.

#[cfg(feature = "replication")]
pub mod replication;
#[cfg(feature = "replication")]
pub use replication::{
    available_replication_slots, bypass_rls_attribute, drop_replication_slots, get_current_wal_lsn,
    get_max_wal_senders, get_timeline_id, get_wal_level,
};
#[cfg(feature = "schemas")]
pub mod desc;
#[cfg(feature = "schemas")]
pub mod schemas;
#[cfg(feature = "schemas")]
pub use schemas::{get_schemas, publication_info};
#[cfg(feature = "tunnel")]
pub mod tunnel;
#[cfg(feature = "tunnel")]
pub use tunnel::{Client, Config, DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT, TunnelConfig};

pub mod query;
pub use query::simple_query_opt;

/// An error representing pg, ssh, ssl, and other failures.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// Any other error we bail on.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// Error using ssh.
    #[cfg(feature = "tunnel")]
    #[error("error setting up ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    /// Error doing io to setup an ssh connection.
    #[error("error communicating with ssh tunnel: {0}")]
    SshIo(std::io::Error),
    /// Error doing io to setup a connection.
    #[error("IO error in connection: {0}")]
    Io(#[from] std::io::Error),
    /// A postgres error.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// Error setting up postgres ssl.
    #[error(transparent)]
    PostgresSsl(#[from] openssl::error::ErrorStack),
    #[error("query returned more rows than expected")]
    UnexpectedRow,
    /// Cannot find publication
    ///
    /// This error is more specific than the others because its occurrence has
    /// differing semantics from other types of PG errors.
    #[error("publication {0} does not exist")]
    PublicationMissing(String),
}
