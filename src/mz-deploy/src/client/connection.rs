// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database client for mz-deploy.
//!
//! This module provides the main `Client` struct for interacting with Materialize.
//! The client handles connection management and delegates specialized operations
//! to domain-specific sub-clients.
//!
//! ## Sub-Client Architecture
//!
//! Operations are grouped into domain sub-clients accessed via accessor methods
//! on `Client`. Each sub-client borrows the `Client` and provides a focused API:
//!
//! | Sub-client | Accessor | Responsibility |
//! |------------|----------|---------------|
//! | `DeploymentsClient` | `.deployments()` | Deployment lifecycle (stage, promote, abort) |
//! | `DeploymentsClientMut` | `.deployments_mut()` | Mutable deployment ops (SUBSCRIBE cursors) |
//! | `IntrospectionClient` | `.introspection()` | Read-only catalog metadata queries |
//! | `ValidationClient` | `.validation()` | Pre-deployment environment checks |
//! | `TypeInfoClient` | `.types()` | Column/type introspection for type checking |
//! | `ProvisioningClient` | `.provisioning()` | Idempotent DDL for databases, schemas, clusters |
//!
//! ## TLS Policy
//!
//! Per-profile `sslmode` with libpq semantics (`disable`, `prefer`, `require`,
//! `verify-ca`, `verify-full`). When unset, loopback hosts default to
//! `prefer` and everything else defaults to `require`. See the design at
//! `docs/superpowers/specs/2026-04-22-profile-tls-design.md` for the behavior
//! table and migration notes.

use crate::client::errors::ConnectionError;
use crate::config::{Profile, SslMode};
use crate::info;
use std::collections::BTreeMap;
use tokio_postgres::types::ToSql;
use tokio_postgres::{
    Client as PgClient, NoTls, Row, SimpleQueryMessage, ToStatement, Transaction,
};

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

/// Domain sub-client for developer overlay manifest operations.
pub struct DevOverlaysClient<'a> {
    pub(crate) client: &'a Client,
}

const APPLICATION_NAME: &str = "mz-deploy";

impl Client {
    /// Connect to the database using a Profile directly.
    ///
    /// TLS behavior is driven by `profile.sslmode`; when unset, loopback hosts
    /// default to `prefer` and everything else defaults to `require`. Verification
    /// (`verify-ca` / `verify-full`) sources CAs from `profile.sslrootcert`, then
    /// the platform CA hunt, then OpenSSL's compiled-in defaults.
    ///
    /// Every connection is pinned to `_mz_deploy_server` via libpq options;
    /// any user-supplied `cluster` in profile.options is silently overridden.
    /// The unit-test runtime uses [`connect_with_profile_no_pin`] instead —
    /// its ephemeral Docker container has no `_mz_deploy_server` cluster.
    pub async fn connect_with_profile(profile: Profile) -> Result<Self, ConnectionError> {
        Self::connect_with_profile_inner(profile, /* pin_server_cluster */ true).await
    }

    /// Connect without pinning the session cluster to `_mz_deploy_server`.
    ///
    /// Used in two places where `_mz_deploy_server` is not yet (or never)
    /// present:
    /// - The ephemeral Docker container used by unit-test execution.
    /// - `setup::run`, which is the command that creates the cluster.
    ///
    /// Uses whatever cluster the profile or server default selects.
    /// Deliberately `pub(crate)` so nothing outside the crate can bypass
    /// the production session-cluster pin.
    pub(crate) async fn connect_with_profile_no_pin(
        profile: Profile,
    ) -> Result<Self, ConnectionError> {
        Self::connect_with_profile_inner(profile, /* pin_server_cluster */ false).await
    }

    async fn connect_with_profile_inner(
        profile: Profile,
        pin_server_cluster: bool,
    ) -> Result<Self, ConnectionError> {
        let host = profile.require_host()?;
        let mut config = tokio_postgres::Config::new();
        config.host(host);
        config.port(profile.port);
        config.user(&profile.username);
        if let Some(password) = &profile.password {
            config.password(password.as_str());
        }
        config.application_name(APPLICATION_NAME);

        let mut effective_options = profile.options.clone();
        if pin_server_cluster {
            effective_options.insert(
                "cluster".to_string(),
                crate::client::SERVER_CLUSTER_NAME.to_string(),
            );
        }
        if let Some(inner) = build_options_string(&effective_options) {
            config.options(&inner);
        }

        let mode = profile.sslmode.unwrap_or_else(|| default_sslmode(host));
        let hunt: Vec<&std::path::Path> =
            DEFAULT_CA_PATHS.iter().map(std::path::Path::new).collect();
        let spec = plan_connector(mode, profile.sslrootcert.as_deref(), host, &hunt, |p| {
            p.exists()
        })?;
        let connector = build_connector(spec)?;

        config.ssl_mode(tokio_ssl_mode(mode));

        // `config.connect(NoTls)` and `config.connect(tls)` return `Connection`s
        // parameterized over different TLS stream types that can't unify. We box
        // both to a common `dyn Future` so there's a single spawn site below.
        type BoxConnection =
            Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send + Unpin>;
        let (client, connection): (PgClient, BoxConnection) = match connector {
            Connector::NoTls => {
                let (client, connection) = config
                    .connect(NoTls)
                    .await
                    .map_err(|source| classify_connect_error(source, &profile, mode))?;
                (client, Box::new(connection))
            }
            Connector::Tls(tls) => {
                let (client, connection) = config
                    .connect(tls)
                    .await
                    .map_err(|source| classify_connect_error(source, &profile, mode))?;
                (client, Box::new(connection))
            }
        };

        mz_ore::task::spawn(|| "mz-deploy-connection", async move {
            if let Err(e) = connection.await {
                info!("connection error: {}", e);
            }
        });

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

    /// Access developer overlay manifest operations.
    pub fn dev_overlays(&self) -> DevOverlaysClient<'_> {
        DevOverlaysClient { client: self }
    }

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

    /// Execute a SQL statement using the simple query protocol (text-only, no binary encoding).
    pub async fn simple_query(
        &self,
        query: &str,
    ) -> Result<Vec<SimpleQueryMessage>, ConnectionError> {
        self.client
            .simple_query(query)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute one or more SQL statements that don't return rows, using the simple query protocol.
    pub async fn batch_execute(&self, query: &str) -> Result<(), ConnectionError> {
        self.client
            .batch_execute(query)
            .await
            .map_err(ConnectionError::Query)
    }
}

/// Platform CA bundle candidates, walked in order by `build_connector` when
/// `sslmode` resolves to `verify-ca` / `verify-full` and the profile does not
/// set `sslrootcert`. Kept in sync with libpq-like installations on our
/// supported platforms.
const DEFAULT_CA_PATHS: &[&str] = &[
    "/etc/ssl/cert.pem",                    // macOS system
    "/opt/homebrew/etc/openssl@3/cert.pem", // macOS Homebrew ARM
    "/usr/local/etc/openssl@3/cert.pem",    // macOS Homebrew Intel
    "/opt/homebrew/etc/openssl/cert.pem",   // macOS Homebrew ARM (older)
    "/usr/local/etc/openssl/cert.pem",      // macOS Homebrew Intel (older)
    "/etc/ssl/certs/ca-certificates.crt",   // Debian/Ubuntu
    "/etc/pki/tls/certs/ca-bundle.crt",     // RHEL/CentOS
    "/etc/ssl/ca-bundle.pem",               // OpenSUSE
];

/// The default `SslMode` applied when a profile does not set `sslmode`.
///
/// Loopback hosts get `Prefer` so local Mz (which does not offer TLS) works
/// without explicit config. Everything else gets `Require` — TLS is required
/// but certificate verification is not. Users who want verification set
/// `sslmode = "verify-ca"` or `sslmode = "verify-full"` explicitly.
pub(crate) fn default_sslmode(host: &str) -> SslMode {
    if is_loopback_host(host) {
        SslMode::Prefer
    } else {
        SslMode::Require
    }
}

/// Returns `true` if `host` names the loopback interface.
///
/// Recognizes `localhost`, any address in `127.0.0.0/8`, and `::1` (with or
/// without URL-style brackets). Used by the SQL TLS defaults and by
/// `mz-deploy mcp` to pick `http://` vs `https://`.
pub(crate) fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = unbracketed.parse::<std::net::IpAddr>() {
        return ip.is_loopback();
    }
    false
}

fn tokio_ssl_mode(mode: SslMode) -> tokio_postgres::config::SslMode {
    use tokio_postgres::config::SslMode as TokioMode;
    match mode {
        SslMode::Disable => TokioMode::Disable,
        SslMode::Prefer => TokioMode::Prefer,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => TokioMode::Require,
    }
}

/// How `verify-full` should match the cert's SAN entries.
#[derive(Debug)]
enum HostCheck {
    /// Match a DNS name. Host parsed as a non-IP string.
    Dns(String),
    /// Match an IPv4 or IPv6 literal. Host parsed as `IpAddr`.
    Ip(std::net::IpAddr),
}

/// Pure-data representation of the TLS setup for a connection, derived from
/// a profile's effective `SslMode` and `sslrootcert`.
#[derive(Debug)]
enum ConnectorSpec {
    NoTls,
    Tls {
        verify: openssl::ssl::SslVerifyMode,
        host_check: Option<HostCheck>,
        ca_source: CaSource,
    },
}

/// Where the CA bundle comes from for verifying the server cert, or the
/// absence thereof for non-verifying modes.
#[derive(Debug)]
enum CaSource {
    /// `disable` / `prefer` / `require` — no CA is loaded.
    None,
    /// Explicit path from the profile's `sslrootcert` field.
    Explicit(std::path::PathBuf),
    /// Path discovered by walking `DEFAULT_CA_PATHS`.
    Hunted(std::path::PathBuf),
    /// Fallback to OpenSSL's compiled-in default verify paths
    /// (`set_default_verify_paths`). Used only when the hunt finds nothing
    /// and no explicit path is set.
    DefaultVerifyPaths,
}

/// Runtime-ready connector variant handed to `tokio_postgres::Config::connect`.
enum Connector {
    NoTls,
    Tls(postgres_openssl::MakeTlsConnector),
}

impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Connector::NoTls => write!(f, "Connector::NoTls"),
            Connector::Tls(_) => write!(f, "Connector::Tls(...)"),
        }
    }
}

/// Plan the TLS setup for a connection from the resolved (mode, CA) inputs.
///
/// Pure: does no network I/O and — aside from the injected `ca_exists`
/// predicate — does no filesystem I/O. Returns a [`ConnectorSpec`] that
/// [`build_connector`] then materializes into an OpenSSL context.
///
/// `hunt_candidates` is the ordered list of default CA paths to probe when
/// `sslrootcert` is not set. In production this is [`DEFAULT_CA_PATHS`];
/// tests pass their own list plus a stubbed `ca_exists` predicate.
fn plan_connector(
    mode: SslMode,
    sslrootcert: Option<&std::path::Path>,
    host: &str,
    hunt_candidates: &[&std::path::Path],
    ca_exists: impl Fn(&std::path::Path) -> bool,
) -> Result<ConnectorSpec, ConnectionError> {
    use openssl::ssl::SslVerifyMode;

    match mode {
        SslMode::Disable => Ok(ConnectorSpec::NoTls),
        SslMode::Prefer | SslMode::Require => Ok(ConnectorSpec::Tls {
            verify: SslVerifyMode::NONE,
            host_check: None,
            ca_source: CaSource::None,
        }),
        SslMode::VerifyCa | SslMode::VerifyFull => {
            let ca_source = resolve_ca_source(sslrootcert, hunt_candidates, ca_exists)?;
            let host_check = if matches!(mode, SslMode::VerifyFull) {
                Some(match host.parse::<std::net::IpAddr>() {
                    Ok(ip) => HostCheck::Ip(ip),
                    Err(_) => HostCheck::Dns(host.to_string()),
                })
            } else {
                None
            };
            Ok(ConnectorSpec::Tls {
                verify: SslVerifyMode::PEER,
                host_check,
                ca_source,
            })
        }
    }
}

fn resolve_ca_source(
    explicit: Option<&std::path::Path>,
    hunt_candidates: &[&std::path::Path],
    ca_exists: impl Fn(&std::path::Path) -> bool,
) -> Result<CaSource, ConnectionError> {
    if let Some(path) = explicit {
        if ca_exists(path) {
            return Ok(CaSource::Explicit(path.to_path_buf()));
        } else {
            return Err(ConnectionError::TlsCaNotFound);
        }
    }
    for candidate in hunt_candidates {
        if ca_exists(candidate) {
            return Ok(CaSource::Hunted(candidate.to_path_buf()));
        }
    }
    Ok(CaSource::DefaultVerifyPaths)
}

/// Convert a [`ConnectorSpec`] into a runtime [`Connector`] by wiring up the
/// OpenSSL context. All filesystem I/O for CAs happens here.
fn build_connector(spec: ConnectorSpec) -> Result<Connector, ConnectionError> {
    use openssl::ssl::{SslConnector, SslMethod};

    match spec {
        ConnectorSpec::NoTls => Ok(Connector::NoTls),
        ConnectorSpec::Tls {
            verify,
            host_check,
            ca_source,
        } => {
            let mut builder = SslConnector::builder(SslMethod::tls()).map_err(|e| {
                ConnectionError::Message(format!("Failed to create TLS builder: {}", e))
            })?;

            match ca_source {
                CaSource::None => {}
                CaSource::Explicit(path) | CaSource::Hunted(path) => {
                    builder
                        .set_ca_file(&path)
                        .map_err(|_| ConnectionError::TlsCaNotFound)?;
                }
                CaSource::DefaultVerifyPaths => {
                    builder
                        .set_default_verify_paths()
                        .map_err(|_| ConnectionError::TlsCaNotFound)?;
                }
            }

            builder.set_verify(verify);

            if let Some(check) = host_check {
                let param = builder.verify_param_mut();
                match check {
                    HostCheck::Dns(name) => {
                        param
                            .set_host(&name)
                            .map_err(|e| ConnectionError::Message(format!("{}", e)))?;
                    }
                    HostCheck::Ip(ip) => {
                        param
                            .set_ip(ip)
                            .map_err(|e| ConnectionError::Message(format!("{}", e)))?;
                    }
                }
            }

            Ok(Connector::Tls(postgres_openssl::MakeTlsConnector::new(
                builder.build(),
            )))
        }
    }
}

/// Classify a `tokio_postgres::Error` surfaced from `Config::connect(...)`
/// into the most specific `ConnectionError` variant.
///
/// Rules:
/// - OpenSSL error found in the source chain + `mode` is `verify-*` →
///   [`ConnectionError::TlsVerification`] (with `hostname_suffix` if the
///   OpenSSL message names a hostname / IP mismatch).
/// - `mode` is `require` / `verify-*` and the error message indicates the
///   server refused TLS → [`ConnectionError::TlsRequiredNotSupported`].
/// - Otherwise → [`ConnectionError::Connect`].
fn classify_connect_error(
    source: tokio_postgres::Error,
    profile: &Profile,
    mode: SslMode,
) -> ConnectionError {
    // Caller has already gone through `require_host()` to attempt the
    // connection that produced this error, so `host` must be `Some` here.
    let host = profile.host.clone().unwrap_or_default();
    if matches!(mode, SslMode::VerifyCa | SslMode::VerifyFull) {
        if let Some(ssl_msg) = ssl_error_in_chain(&source) {
            let hostname_suffix = if ssl_msg.contains("hostname mismatch")
                || ssl_msg.contains("Hostname mismatch")
                || ssl_msg.contains("IP address mismatch")
            {
                " (hostname mismatch)"
            } else {
                ""
            };
            return ConnectionError::TlsVerification {
                host,
                port: profile.port,
                hostname_suffix,
                source,
            };
        }
    }

    if matches!(
        mode,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull
    ) && message_indicates_tls_refused(&source)
    {
        return ConnectionError::TlsRequiredNotSupported {
            host,
            port: profile.port,
            source,
        };
    }

    ConnectionError::Connect {
        host,
        port: profile.port,
        source,
    }
}

/// Walk the source chain of a `tokio_postgres::Error` and return the string
/// form of the first `openssl::error::ErrorStack` found.
fn ssl_error_in_chain(err: &tokio_postgres::Error) -> Option<String> {
    let mut cur: &(dyn std::error::Error + 'static) = err;
    while let Some(source) = std::error::Error::source(cur) {
        if source.is::<openssl::error::ErrorStack>() {
            return Some(source.to_string());
        }
        cur = source;
    }
    None
}

/// Heuristic: does the error look like "server said no to our TLS request"?
///
/// `tokio_postgres` surfaces this as an io error or a "server does not
/// support TLS" message depending on version. We string-match the Display
/// form because the typed variants are not all public.
fn message_indicates_tls_refused(err: &tokio_postgres::Error) -> bool {
    matches_tls_refused_message(&err.to_string())
}

/// Pure string check for the substrings `tokio_postgres` produces when the
/// server refuses the TLS startup request (responds `'N'` to the SSL byte).
///
/// Extracted from `message_indicates_tls_refused` so we can unit-test the
/// substring list — the caller takes `&tokio_postgres::Error`, which has no
/// public constructor.
fn matches_tls_refused_message(msg: &str) -> bool {
    msg.contains("TLS was required")
        || msg.contains("server does not support TLS")
        || msg.contains("server does not support SSL")
}

/// Escape a value for embedding inside the libpq `options` connection
/// parameter.
///
/// Within the `options` string, spaces separate `-c key=value` tokens unless
/// escaped, and backslash is the escape character. Only spaces and backslashes
/// are special; all other characters are literal.
fn escape_options_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str(r"\\"),
            ' ' => out.push_str(r"\ "),
            other => out.push(other),
        }
    }
    out
}

/// Build the inner value of the libpq `options` connection parameter from a
/// profile's options map.
///
/// Produces a space-separated string of `-c key=value` tokens in sorted-key
/// order, with each value inner-escaped per [`escape_options_value`].
/// Returns `None` when the map is empty so the caller can omit the fragment.
pub(crate) fn build_options_string(options: &BTreeMap<String, String>) -> Option<String> {
    if options.is_empty() {
        return None;
    }
    let joined = options
        .iter()
        .map(|(k, v)| format!("-c {k}={}", escape_options_value(v)))
        .collect::<Vec<_>>()
        .join(" ");
    Some(joined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_options_value_plain() {
        assert_eq!(escape_options_value("prod"), "prod");
    }

    #[test]
    fn test_escape_options_value_space() {
        assert_eq!(escape_options_value("prod cluster"), r"prod\ cluster");
    }

    #[test]
    fn test_escape_options_value_backslash() {
        assert_eq!(escape_options_value(r"a\b"), r"a\\b");
    }

    #[test]
    fn test_escape_options_value_mixed() {
        // Space then backslash
        assert_eq!(escape_options_value(r"a \b"), r"a\ \\b");
    }

    #[test]
    fn test_build_options_string_empty() {
        let options: BTreeMap<String, String> = BTreeMap::new();
        assert_eq!(build_options_string(&options), None);
    }

    #[test]
    fn test_build_options_string_single() {
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), "prod".to_string());
        assert_eq!(
            build_options_string(&options),
            Some("-c cluster=prod".to_string())
        );
    }

    #[test]
    fn test_build_options_string_multiple_sorted() {
        let mut options = BTreeMap::new();
        // Insert in reverse order to verify BTreeMap iteration sorts keys.
        options.insert("search_path".to_string(), "public".to_string());
        options.insert("cluster".to_string(), "prod".to_string());
        assert_eq!(
            build_options_string(&options),
            Some("-c cluster=prod -c search_path=public".to_string())
        );
    }

    #[test]
    fn test_build_options_string_escapes_value_space() {
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), "prod cluster".to_string());
        assert_eq!(
            build_options_string(&options),
            Some(r"-c cluster=prod\ cluster".to_string())
        );
    }

    #[test]
    fn test_build_options_string_escapes_value_backslash() {
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), r"a\b".to_string());
        assert_eq!(
            build_options_string(&options),
            Some(r"-c cluster=a\\b".to_string())
        );
    }

    use std::path::Path;

    #[test]
    fn plan_disable_produces_notls() {
        let spec = plan_connector(SslMode::Disable, None, "example.com", &[], |_| false).unwrap();
        assert!(matches!(spec, ConnectorSpec::NoTls));
    }

    #[test]
    fn plan_prefer_and_require_have_verify_none_and_no_ca() {
        for mode in [SslMode::Prefer, SslMode::Require] {
            let spec = plan_connector(mode, None, "example.com", &[], |_| true).unwrap();
            match spec {
                ConnectorSpec::Tls {
                    verify,
                    host_check,
                    ca_source,
                } => {
                    assert_eq!(verify, openssl::ssl::SslVerifyMode::NONE);
                    assert!(host_check.is_none());
                    assert!(matches!(ca_source, CaSource::None));
                }
                ConnectorSpec::NoTls => panic!("expected Tls for {:?}, got NoTls", mode),
            }
        }
    }

    #[test]
    fn plan_verify_ca_has_peer_verify_no_host_check() {
        let spec = plan_connector(
            SslMode::VerifyCa,
            None,
            "example.com",
            &[Path::new("/does/not/exist"), Path::new("/tmp/fake-ca.pem")],
            |p| p == Path::new("/tmp/fake-ca.pem"),
        )
        .unwrap();
        match spec {
            ConnectorSpec::Tls {
                verify,
                host_check,
                ca_source,
            } => {
                assert_eq!(verify, openssl::ssl::SslVerifyMode::PEER);
                assert!(host_check.is_none());
                assert!(
                    matches!(ca_source, CaSource::Hunted(p) if p == Path::new("/tmp/fake-ca.pem"))
                );
            }
            ConnectorSpec::NoTls => panic!("expected Tls, got NoTls"),
        }
    }

    #[test]
    fn plan_verify_full_dns_host_check() {
        let spec = plan_connector(
            SslMode::VerifyFull,
            None,
            "example.com",
            &[Path::new("/tmp/fake-ca.pem")],
            |_| true,
        )
        .unwrap();
        match spec {
            ConnectorSpec::Tls {
                host_check: Some(HostCheck::Dns(ref name)),
                ..
            } => assert_eq!(name, "example.com"),
            other => panic!("expected Tls with Dns host check, got {:?}", other),
        }
    }

    #[test]
    fn plan_verify_full_ip_host_check() {
        let spec = plan_connector(
            SslMode::VerifyFull,
            None,
            "10.0.0.5",
            &[Path::new("/tmp/fake-ca.pem")],
            |_| true,
        )
        .unwrap();
        match spec {
            ConnectorSpec::Tls {
                host_check: Some(HostCheck::Ip(ip)),
                ..
            } => assert_eq!(ip, "10.0.0.5".parse::<std::net::IpAddr>().unwrap()),
            other => panic!("expected Tls with Ip host check, got {:?}", other),
        }
    }

    #[test]
    fn plan_explicit_sslrootcert_wins_over_hunt() {
        let explicit = std::path::PathBuf::from("/my/ca.pem");
        let spec = plan_connector(
            SslMode::VerifyCa,
            Some(&explicit),
            "example.com",
            &[Path::new("/tmp/should-be-ignored.pem")],
            |p| p == explicit.as_path(),
        )
        .unwrap();
        match spec {
            ConnectorSpec::Tls {
                ca_source: CaSource::Explicit(p),
                ..
            } => assert_eq!(p, explicit),
            other => panic!("expected Tls/Explicit, got {:?}", other),
        }
    }

    #[test]
    fn plan_explicit_sslrootcert_missing_is_ca_not_found() {
        let explicit = std::path::PathBuf::from("/no/such/file.pem");
        let err = plan_connector(
            SslMode::VerifyCa,
            Some(&explicit),
            "example.com",
            &[Path::new("/tmp/fake-ca.pem")],
            |_| false,
        )
        .unwrap_err();
        assert!(matches!(err, ConnectionError::TlsCaNotFound));
    }

    #[test]
    fn plan_no_ca_sources_at_all_falls_back_to_default_verify_paths() {
        let spec = plan_connector(
            SslMode::VerifyFull,
            None,
            "example.com",
            &[Path::new("/nope1"), Path::new("/nope2")],
            |_| false,
        )
        .unwrap();
        match spec {
            ConnectorSpec::Tls {
                ca_source: CaSource::DefaultVerifyPaths,
                ..
            } => {}
            other => panic!("expected Tls/DefaultVerifyPaths, got {:?}", other),
        }
    }

    #[test]
    fn build_disable_returns_notls() {
        let connector = build_connector(ConnectorSpec::NoTls).unwrap();
        assert!(matches!(connector, Connector::NoTls));
    }

    #[test]
    fn build_prefer_returns_tls_no_ca_work() {
        let connector = build_connector(ConnectorSpec::Tls {
            verify: openssl::ssl::SslVerifyMode::NONE,
            host_check: None,
            ca_source: CaSource::None,
        })
        .unwrap();
        assert!(matches!(connector, Connector::Tls(_)));
    }

    #[test]
    fn build_explicit_missing_ca_returns_ca_not_found() {
        let err = build_connector(ConnectorSpec::Tls {
            verify: openssl::ssl::SslVerifyMode::PEER,
            host_check: None,
            ca_source: CaSource::Explicit(std::path::PathBuf::from("/absolutely/not/a/real/file")),
        })
        .unwrap_err();
        assert!(matches!(err, ConnectionError::TlsCaNotFound));
    }

    #[test]
    fn matches_tls_refused_tls_was_required() {
        assert!(matches_tls_refused_message(
            "some prefix: TLS was required but not provided"
        ));
    }

    #[test]
    fn matches_tls_refused_does_not_support_tls() {
        assert!(matches_tls_refused_message(
            "error: server does not support TLS"
        ));
    }

    #[test]
    fn matches_tls_refused_does_not_support_ssl() {
        assert!(matches_tls_refused_message(
            "error: server does not support SSL"
        ));
    }

    #[test]
    fn matches_tls_refused_unrelated_message() {
        assert!(!matches_tls_refused_message("connection refused"));
        assert!(!matches_tls_refused_message("database does not exist"));
        assert!(!matches_tls_refused_message(""));
    }
}
