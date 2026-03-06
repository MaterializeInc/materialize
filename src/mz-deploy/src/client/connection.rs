//! Database client for mz-deploy.
//!
//! This module provides the main `Client` struct for interacting with Materialize.
//! The client handles connection management and delegates specialized operations
//! to submodules:
//!
//! - `errors` - Error types for client operations
//! - `deployment_ops` - Deployment tracking and management
//! - `introspection` - Database metadata queries
//! - `validation` - Project validation against the database

use crate::config::Profile;
use crate::client::errors::ConnectionError;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client as PgClient, NoTls, Row, ToStatement, Transaction};

/// Database client for interacting with Materialize.
///
/// The `Client` struct provides methods for:
/// - Connecting to the database
/// - Schema and cluster management
/// - Deployment tracking
/// - Database introspection
/// - Project validation
pub struct Client {
    client: PgClient,
    profile: Profile,
}

/// Domain sub-client for deployment lifecycle operations.
pub struct DeploymentsClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for deployment operations that require mutable client access.
pub struct DeploymentsClientMut<'a> {
    pub(crate) client: &'a mut Client,
}

/// Domain sub-client for metadata and object introspection operations.
pub struct IntrospectionClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for project and privilege validation operations.
pub struct ValidationClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for column/type introspection used by type checking and tests.
pub struct TypeInfoClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for provisioning databases, schemas, and clusters.
pub struct ProvisioningClient<'a> {
    pub(crate) client: &'a Client,
}

impl Client {
    // =========================================================================
    // Connection Methods
    // =========================================================================

    /// Connect to the database using a Profile directly.
    ///
    /// Tries TLS connection first (required for Materialize Cloud), then falls back
    /// to NoTls for local connections (e.g., localhost, Docker).
    pub async fn connect_with_profile(profile: Profile) -> Result<Self, ConnectionError> {
        // Build connection string
        // Values with special characters need to be quoted with single quotes,
        // and single quotes/backslashes within values need to be escaped
        let mut conn_str = format!("host={} port={}", profile.host, profile.port);

        if let Some(ref username) = profile.username {
            conn_str.push_str(&format!(" user='{}'", escape_conn_string_value(username)));
        }

        if let Some(ref password) = profile.password {
            conn_str.push_str(&format!(
                " password='{}'",
                escape_conn_string_value(password)
            ));
        }

        // Determine if this is likely a cloud connection (not localhost)
        let is_local = profile.host == "localhost"
            || profile.host == "127.0.0.1"
            || profile.host.starts_with("192.168.")
            || profile.host.starts_with("10.")
            || profile.host.starts_with("172.");

        let client = if is_local {
            // Local connection - use NoTls
            let (client, connection) =
                tokio_postgres::connect(&conn_str, NoTls)
                    .await
                    .map_err(|source| ConnectionError::Connect {
                        host: profile.host.clone(),
                        port: profile.port,
                        source,
                    })?;

            // Spawn the connection handler
            mz_ore::task::spawn(|| "mz-deploy-connection", async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            client
        } else {
            // Cloud connection - use TLS
            let mut builder = SslConnector::builder(SslMethod::tls()).map_err(|e| {
                ConnectionError::Message(format!("Failed to create TLS builder: {}", e))
            })?;

            // Load CA certificates - try platform-specific paths
            // macOS: Homebrew OpenSSL or system certificates
            // Linux: Standard system paths
            let ca_paths = [
                "/etc/ssl/cert.pem",                    // macOS system
                "/opt/homebrew/etc/openssl@3/cert.pem", // macOS Homebrew ARM
                "/usr/local/etc/openssl@3/cert.pem",    // macOS Homebrew Intel
                "/opt/homebrew/etc/openssl/cert.pem",   // macOS Homebrew ARM (older)
                "/usr/local/etc/openssl/cert.pem",      // macOS Homebrew Intel (older)
                "/etc/ssl/certs/ca-certificates.crt",   // Debian/Ubuntu
                "/etc/pki/tls/certs/ca-bundle.crt",     // RHEL/CentOS
                "/etc/ssl/ca-bundle.pem",               // OpenSUSE
            ];

            let mut ca_loaded = false;
            for path in &ca_paths {
                if std::path::Path::new(path).exists() {
                    if builder.set_ca_file(path).is_ok() {
                        ca_loaded = true;
                        break;
                    }
                }
            }

            if !ca_loaded {
                // Fall back to default paths as last resort
                let _ = builder.set_default_verify_paths();
            }

            builder.set_verify(SslVerifyMode::PEER);

            let connector = MakeTlsConnector::new(builder.build());

            let (client, connection) = tokio_postgres::connect(&conn_str, connector)
                .await
                .map_err(|source| ConnectionError::Connect {
                    host: profile.host.clone(),
                    port: profile.port,
                    source,
                })?;

            // Spawn the connection handler
            mz_ore::task::spawn(|| "mz-deploy-connection", async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            client
        };

        Ok(Client { client, profile })
    }

    /// Get the profile used for this connection.
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Start a transaction on the underlying connection.
    pub(crate) async fn begin_transaction(&mut self) -> Result<Transaction<'_>, ConnectionError> {
        self.client
            .transaction()
            .await
            .map_err(ConnectionError::Query)
    }

    /// Access deployment lifecycle operations.
    pub fn deployments(&self) -> DeploymentsClient<'_> {
        DeploymentsClient { client: self }
    }

    /// Access mutable deployment lifecycle operations.
    pub fn deployments_mut(&mut self) -> DeploymentsClientMut<'_> {
        DeploymentsClientMut { client: self }
    }

    /// Access metadata and object introspection operations.
    pub fn introspection(&self) -> IntrospectionClient<'_> {
        IntrospectionClient { client: self }
    }

    /// Access database validation operations.
    pub fn validation(&self) -> ValidationClient<'_> {
        ValidationClient { client: self }
    }

    /// Access type/column introspection operations.
    pub fn types(&self) -> TypeInfoClient<'_> {
        TypeInfoClient { client: self }
    }

    /// Access provisioning operations for databases, schemas, and clusters.
    pub fn provisioning(&self) -> ProvisioningClient<'_> {
        ProvisioningClient { client: self }
    }

    // =========================================================================
    // Basic Query Methods
    // =========================================================================

    /// Execute a SQL statement that doesn't return rows.
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .execute(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query_one(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }
}

/// Escape a value for use in a libpq connection string.
///
/// In connection strings, values containing special characters must be quoted
/// with single quotes, and any single quotes or backslashes within the value
/// must be escaped with a backslash.
fn escape_conn_string_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}
