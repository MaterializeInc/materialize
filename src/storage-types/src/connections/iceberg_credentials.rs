// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog-vended storage credentials for Iceberg sinks.
//!
//! A REST catalog asked for access delegation answers `loadTable` with temporary, table-scoped
//! storage credentials instead of expecting the client to hold its own. Those credentials expire,
//! and OpenDAL has no notion of that: it takes what the `FileIO` was built with and signs every
//! request with it. [`VendedCredentialLoader`] closes that gap by re-fetching from the catalog's
//! `loadCredentials` endpoint, and the rest of this module locates that endpoint.
//!
//! Locating it takes a round trip of its own. The REST specification has servers announce a
//! request prefix from their `config` endpoint (`catalogs/<name>` for Unity Catalog, absent for
//! others), and every resource path carries it. `iceberg-rust` resolves the same value when it
//! builds the catalog but keeps it private, so this module asks the server directly.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, anyhow};
use http::{HeaderMap, HeaderName, HeaderValue};
use iceberg::TableIdent;
use iceberg::io::{GCS_TOKEN, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN};
use iceberg_catalog_rest::{StorageCredential, TokenProvider};
use iceberg_storage_opendal::{AwsCredential, GcsCredential, GcsToken, ProvideCredential};
use mz_ore::error::ErrorExt;
use reqsign_core::time::Timestamp;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use url::Url;

use crate::connections::IcebergAccessDelegation;

/// The `X-Iceberg-Access-Delegation` header, spelled for requests Materialize issues itself
/// rather than through the catalog client, which takes it as a `header.*` catalog property.
const ICEBERG_ACCESS_DELEGATION_HEADER: HeaderName =
    HeaderName::from_static("x-iceberg-access-delegation");

/// Property name for the most common way catalogs report when vended S3 credentials expire.
/// Catalogs are not required to send it.
const S3_SESSION_TOKEN_EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";

/// Property name for when a vended GCS token expires, in epoch milliseconds.
///
/// NOTE: no `-ms` suffix, unlike [`S3_SESSION_TOKEN_EXPIRES_AT_MS`], even though both carry
/// milliseconds. The spelling comes from Iceberg's `GCPProperties`, not from analogy with the S3
/// property.
const GCS_TOKEN_EXPIRES_AT: &str = "gcs.oauth2.token-expires-at";

/// How far ahead of a reported expiry to re-fetch a vended credential.
///
/// NOTE: must stay comfortably above reqsign's own refresh buffers, which are 120s for a GCS
/// token. A GCS token handed back inside that window fails signing outright, because a vended
/// credential carries no service account for reqsign to fall back on.
const VENDED_CREDENTIAL_REFRESH_BUFFER: Duration = Duration::from_secs(900);

/// How long to trust a vended credential that reports no expiry.
///
/// A catalog that omits the expiry property leaves nothing to schedule against, and a credential
/// held past its real lifetime fails every storage request until the dataflow restarts. So
/// re-fetch on a short interval instead: each one is a single REST call against a credential the
/// sink is already using.
// TODO SS-449: make this a dyncfg
const VENDED_CREDENTIAL_DEFAULT_TTL: Duration = Duration::from_secs(300);

/// A storage credential that a REST catalog can vend.
///
/// Implementors carry the object store's notion of a credential and know which
/// `storage-credentials` properties encode it. Everything else about vending, including the fetch,
/// the caching, and the refresh schedule, is shared by [`VendedCredentialLoader`].
pub(super) trait VendedCredential: Clone + Debug + Send + Sync + Unpin + 'static {
    /// The object store this credential authenticates against, for diagnostics.
    const STORE: &'static str;

    /// Builds the credential from one entry of a catalog's `storage-credentials`.
    fn from_vended(vended: &StorageCredential) -> reqsign_core::Result<Self>;

    /// When the credential expires, if the catalog reported it.
    fn expires_at(&self) -> Option<Timestamp>;
}

impl VendedCredential for AwsCredential {
    const STORE: &'static str = "S3";

    fn from_vended(vended: &StorageCredential) -> reqsign_core::Result<Self> {
        Ok(AwsCredential {
            access_key_id: required(vended, S3_ACCESS_KEY_ID)?,
            secret_access_key: required(vended, S3_SECRET_ACCESS_KEY)?,
            session_token: vended.config.get(S3_SESSION_TOKEN).cloned(),
            expires_in: vended_expires_at(vended, S3_SESSION_TOKEN_EXPIRES_AT_MS)?,
        })
    }

    fn expires_at(&self) -> Option<Timestamp> {
        self.expires_in
    }
}

impl VendedCredential for GcsCredential {
    const STORE: &'static str = "GCS";

    fn from_vended(vended: &StorageCredential) -> reqsign_core::Result<Self> {
        Ok(GcsCredential::with_token(GcsToken {
            access_token: required(vended, GCS_TOKEN)?,
            expires_at: vended_expires_at(vended, GCS_TOKEN_EXPIRES_AT)?,
        }))
    }

    fn expires_at(&self) -> Option<Timestamp> {
        self.token.as_ref().and_then(|token| token.expires_at)
    }
}

/// Reads a property a vended credential cannot do without.
fn required(vended: &StorageCredential, prop: &str) -> reqsign_core::Result<String> {
    vended.config.get(prop).cloned().ok_or_else(|| {
        reqsign_core::Error::credential_invalid(format!(
            "vended Iceberg storage credential for prefix {} is missing {prop}",
            vended.prefix
        ))
    })
}

/// Reads an expiry property, which catalogs are not required to send.
fn vended_expires_at(
    vended: &StorageCredential,
    prop: &str,
) -> reqsign_core::Result<Option<Timestamp>> {
    let Some(raw) = vended.config.get(prop) else {
        debug!(
            prefix = vended.prefix,
            prop, "Iceberg catalog vended a storage credential with no expiry"
        );
        return Ok(None);
    };
    let millis = raw.parse::<i64>().map_err(|e| {
        reqsign_core::Error::credential_invalid(format!(
            "vended Iceberg storage credential for prefix {} has an unparseable {prop}",
            vended.prefix
        ))
        .with_source(e)
    })?;
    Ok(Some(Timestamp::from_millisecond(millis)?))
}

#[derive(Debug)]
pub(super) struct VendedCredentialLoader<C> {
    client: reqwest::Client,
    credential_endpoint: Url,
    token: Arc<dyn TokenProvider>,
    /// Every header this request needs beyond the bearer token, resolved once.
    headers: HeaderMap,
    cached: Mutex<Option<(C, Instant)>>,
}

impl<C: VendedCredential> VendedCredentialLoader<C> {
    /// `headers` are the headers the catalog client puts on its own requests, which this one
    /// bypasses. The delegation header is added here rather than expected in `headers`, since
    /// asking the catalog to vend is the whole point of this request.
    pub(super) fn new(
        client: reqwest::Client,
        credential_endpoint: Url,
        token: Arc<dyn TokenProvider>,
        mut headers: HeaderMap,
    ) -> Self {
        headers.insert(
            ICEBERG_ACCESS_DELEGATION_HEADER,
            HeaderValue::from_static(IcebergAccessDelegation::VendedCredentials.as_header_value()),
        );
        Self {
            client,
            credential_endpoint,
            token,
            headers,
            cached: Mutex::new(None),
        }
    }

    /// Fetches a fresh credential from the catalog, paired with the instant at which it should be
    /// re-fetched.
    async fn fetch(&self) -> reqsign_core::Result<(C, Instant)> {
        let token = self.token.token().await.map_err(|e| {
            reqsign_core::Error::credential_invalid(
                "failed to obtain a catalog token for vended Iceberg storage credentials",
            )
            .with_source(e)
        })?;

        let response = self
            .client
            .get(self.credential_endpoint.clone())
            .bearer_auth(token)
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|e| {
                reqsign_core::Error::unexpected(format!(
                    "failed to request vended Iceberg storage credentials from {}",
                    self.credential_endpoint
                ))
                .with_source(e)
            })?;

        let status = response.status();
        if !status.is_success() {
            // A rejected token stays rejected until something re-mints it, and nothing else on
            // this path does: the REST client's own 401 handling covers catalog requests, not
            // ours. Drop it so the next attempt fetches a new one.
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                if let Err(e) = self.token.invalidate().await {
                    warn!(
                        error = %e.display_with_causes(),
                        "failed to invalidate catalog token after {status} from the \
                         Iceberg credentials endpoint"
                    );
                }
            }
            // Safe to surface the body: only success responses carry credentials.
            let body = response.text().await.unwrap_or_default();
            return Err(reqsign_core::Error::unexpected(format!(
                "Iceberg catalog returned {status} for vended storage credentials at {}: {body}",
                self.credential_endpoint
            )));
        }

        let response: LoadCredentialsResponse = response.json().await.map_err(|e| {
            reqsign_core::Error::unexpected(
                "failed to parse the Iceberg catalog's vended storage credentials",
            )
            .with_source(e)
        })?;

        // `provide_credential` is handed no path, so the longest-prefix match the Iceberg spec
        // describes is not available to us. This endpoint is scoped to a single table and in
        // practice returns one credential; if a catalog returns several, the most specific
        // prefix is the closest thing to a safe default.
        if response.storage_credentials.len() > 1 {
            debug!(
                endpoint = %self.credential_endpoint,
                count = response.storage_credentials.len(),
                "Iceberg catalog vended multiple storage credentials; using the longest prefix"
            );
        }
        let vended = response
            .storage_credentials
            .into_iter()
            .max_by_key(|credential| credential.prefix.len())
            .ok_or_else(|| {
                reqsign_core::Error::credential_invalid(format!(
                    "Iceberg catalog vended no storage credentials at {}",
                    self.credential_endpoint
                ))
            })?;

        let credential = C::from_vended(&vended)?;
        let refresh_at = refresh_deadline(credential.expires_at());
        Ok((credential, refresh_at))
    }
}

impl<C: VendedCredential> ProvideCredential for VendedCredentialLoader<C> {
    type Credential = C;

    async fn provide_credential(
        &self,
        _ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        // The lock is deliberately held across the fetch. `create_operator` builds a fresh
        // OpenDAL `Operator` for every file operation, so reqsign's own credential cache never
        // outlives a single call and this cache is all that stands between the sink and one
        // catalog round trip per storage request. Serializing here means a stale entry costs one
        // refetch rather than one per in-flight operation.
        let mut cached = self.cached.lock().await;

        if let Some((credential, refresh_at)) = &*cached
            && Instant::now() < *refresh_at
        {
            return Ok(Some(credential.clone()));
        }

        // The chain reqsign consults reports only that credential loading failed, without a
        // cause, so log the real one here or it is lost.
        let (credential, refresh_at) = self.fetch().await.inspect_err(|e| {
            warn!(
                store = C::STORE,
                endpoint = %self.credential_endpoint,
                error = %e,
                "failed to refresh vended Iceberg storage credentials"
            );
        })?;
        *cached = Some((credential.clone(), refresh_at));
        Ok(Some(credential))
    }
}

/// Returns when a vended credential expiring at `expires_in` should be re-fetched.
///
/// Refreshes [`VENDED_CREDENTIAL_REFRESH_BUFFER`] early to absorb clock skew and the latency of
/// the fetch itself. A credential that expires within the buffer, or has expired already, is
/// re-fetched on the next call.
fn refresh_deadline(expires_in: Option<Timestamp>) -> Instant {
    let now = Instant::now();
    let Some(expires_in) = expires_in else {
        return now + VENDED_CREDENTIAL_DEFAULT_TTL;
    };
    let remaining = expires_in
        .as_system_time()
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    now + remaining.saturating_sub(VENDED_CREDENTIAL_REFRESH_BUFFER)
}

/// The `loadCredentials` response envelope. `iceberg-rust` models the credential entries but not
/// this wrapper, because it only ever reads them out of a `loadTable` response.
#[derive(Debug, Deserialize)]
struct LoadCredentialsResponse {
    #[serde(default, rename = "storage-credentials")]
    storage_credentials: Vec<StorageCredential>,
}

/// The part of the Iceberg REST `config` response Materialize reads for itself.
#[derive(Debug, Default, Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    defaults: BTreeMap<String, String>,
    #[serde(default)]
    overrides: BTreeMap<String, String>,
}

impl CatalogConfigResponse {
    /// The request prefix the server wants inserted between `/v1` and the resource path,
    /// `catalogs/<name>` for Unity Catalog and absent for catalogs that do not use one.
    ///
    /// Overrides win over defaults, matching how the catalog client merges the two.
    fn announced_prefix(&self) -> Option<&str> {
        self.overrides
            .get("prefix")
            .or_else(|| self.defaults.get("prefix"))
            .map(String::as_str)
    }
}

/// Builds the catalog's `config` endpoint, which is where a server announces its request prefix.
///
/// Unlike every other REST path, this one is not prefixed: the prefix is what it returns.
fn catalog_config_url(uri: &Url, warehouse: Option<&str>) -> Result<Url, anyhow::Error> {
    let mut url = uri.clone();
    url.path_segments_mut()
        .map_err(|_| anyhow!("Iceberg catalog URI cannot be a base: {uri}"))?
        // A configured URI is as likely to be written with a trailing slash as without, and
        // that empty last segment would otherwise become `//v1` in the path.
        .pop_if_empty()
        .extend(["v1", "config"]);
    if let Some(warehouse) = warehouse {
        url.query_pairs_mut().append_pair("warehouse", warehouse);
    }
    Ok(url)
}

/// Builds the endpoint that vends storage credentials for `table`.
fn table_credentials_url(
    uri: &Url,
    prefix: Option<&str>,
    table: &TableIdent,
) -> Result<Url, anyhow::Error> {
    let mut url = uri.clone();
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow!("Iceberg catalog URI cannot be a base: {uri}"))?;
        segments.pop_if_empty().push("v1");
        // A prefix is a path fragment rather than a single segment, so it is split out instead
        // of pushed whole, which would percent-encode its separators.
        if let Some(prefix) = prefix {
            segments.extend(prefix.split('/').filter(|s| !s.is_empty()));
        }
        // `to_url_string` joins multi-level namespaces with the unit separator the
        // specification mandates; pushing it as one segment percent-encodes it to `%1F`.
        segments
            .push("namespaces")
            .push(&table.namespace().to_url_string())
            .push("tables")
            .push(&table.name)
            .push("credentials");
    }
    Ok(url)
}

/// Resolves the REST endpoint that vends storage credentials for `table`.
///
/// Takes a round trip to the catalog's `config` endpoint, because the resource path carries a
/// request prefix that only the server knows. `headers` are the headers the catalog client puts
/// on its own requests, which this one bypasses.
pub(super) async fn table_credentials_endpoint(
    uri: &Url,
    client: &reqwest::Client,
    token: &Arc<dyn TokenProvider>,
    headers: &HeaderMap,
    warehouse: Option<&str>,
    table: &TableIdent,
) -> Result<Url, anyhow::Error> {
    let config_endpoint = catalog_config_url(uri, warehouse)?;

    let bearer = token
        .token()
        .await
        .map_err(|e| anyhow!("failed to obtain an Iceberg catalog token: {e}"))?;
    let response = client
        .get(config_endpoint.clone())
        .bearer_auth(bearer)
        .headers(headers.clone())
        .send()
        .await
        .with_context(|| {
            format!("failed to request Iceberg catalog config at {config_endpoint}")
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Iceberg catalog returned {status} for its config at {config_endpoint}: {body}"
        ));
    }
    let config: CatalogConfigResponse = response.json().await.with_context(|| {
        format!("failed to parse Iceberg catalog config from {config_endpoint}")
    })?;

    table_credentials_url(uri, config.announced_prefix(), table)
}

#[cfg(test)]
mod tests {
    use iceberg::NamespaceIdent;

    use super::*;

    fn table(namespace: &[&str], name: &str) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::from_strs(namespace).expect("valid namespace"),
            name.to_string(),
        )
    }

    fn config_with(defaults: &[(&str, &str)], overrides: &[(&str, &str)]) -> CatalogConfigResponse {
        let to_map = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>()
        };
        CatalogConfigResponse {
            defaults: to_map(defaults),
            overrides: to_map(overrides),
        }
    }

    fn vended(prefix: &str, config: &[(&str, &str)]) -> StorageCredential {
        StorageCredential {
            prefix: prefix.to_string(),
            config: config
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[mz_ore::test]
    fn test_aws_credential_from_vended() {
        let credential = AwsCredential::from_vended(&vended(
            "s3://bucket/warehouse",
            &[
                (S3_ACCESS_KEY_ID, "key"),
                (S3_SECRET_ACCESS_KEY, "secret"),
                (S3_SESSION_TOKEN, "session"),
                (S3_SESSION_TOKEN_EXPIRES_AT_MS, "1700000000000"),
            ],
        ))
        .expect("complete credential");
        assert_eq!(credential.access_key_id, "key");
        assert_eq!(credential.secret_access_key, "secret");
        assert_eq!(credential.session_token.as_deref(), Some("session"));
        assert_eq!(
            credential.expires_at(),
            Some(Timestamp::from_millisecond(1700000000000).unwrap())
        );

        // A catalog need not report an expiry, and long-lived static keys have no session token.
        let credential = AwsCredential::from_vended(&vended(
            "s3://bucket/warehouse",
            &[(S3_ACCESS_KEY_ID, "key"), (S3_SECRET_ACCESS_KEY, "secret")],
        ))
        .expect("credential without expiry");
        assert_eq!(credential.session_token, None);
        assert_eq!(credential.expires_at(), None);

        // A credential missing either key is unusable, and says which one.
        let err = AwsCredential::from_vended(&vended(
            "s3://bucket/warehouse",
            &[(S3_ACCESS_KEY_ID, "key")],
        ))
        .expect_err("incomplete credential");
        assert!(
            err.to_string().contains(S3_SECRET_ACCESS_KEY),
            "unexpected error: {err}"
        );

        // A malformed expiry is an error rather than a silently ignored property: treating it as
        // absent would fall back to the default TTL and hide a catalog bug.
        let err = AwsCredential::from_vended(&vended(
            "s3://bucket/warehouse",
            &[
                (S3_ACCESS_KEY_ID, "key"),
                (S3_SECRET_ACCESS_KEY, "secret"),
                (S3_SESSION_TOKEN_EXPIRES_AT_MS, "soon"),
            ],
        ))
        .expect_err("unparseable expiry");
        assert!(
            err.to_string().contains("unparseable"),
            "unexpected error: {err}"
        );
    }

    #[mz_ore::test]
    fn test_gcs_credential_from_vended() {
        let credential = GcsCredential::from_vended(&vended(
            "gs://bucket/warehouse",
            &[
                (GCS_TOKEN, "ya29.token"),
                (GCS_TOKEN_EXPIRES_AT, "1700000000000"),
            ],
        ))
        .expect("complete credential");
        // A vended credential is a bare token: reqsign has no service account to fall back on.
        assert!(credential.service_account.is_none());
        let token = credential.token.as_ref().expect("token");
        assert_eq!(token.access_token, "ya29.token");
        assert_eq!(
            credential.expires_at(),
            Some(Timestamp::from_millisecond(1700000000000).unwrap())
        );

        let credential =
            GcsCredential::from_vended(&vended("gs://bucket/warehouse", &[(GCS_TOKEN, "tok")]))
                .expect("credential without expiry");
        assert_eq!(credential.expires_at(), None);

        let err = GcsCredential::from_vended(&vended(
            "gs://bucket/warehouse",
            &[(GCS_TOKEN_EXPIRES_AT, "1700000000000")],
        ))
        .expect_err("credential without a token");
        assert!(
            err.to_string().contains(GCS_TOKEN),
            "unexpected error: {err}"
        );
    }

    #[mz_ore::test]
    fn test_refresh_deadline() {
        // No reported expiry falls back to the fixed interval.
        let deadline = refresh_deadline(None);
        assert!(deadline > Instant::now());
        assert!(deadline <= Instant::now() + VENDED_CREDENTIAL_DEFAULT_TTL);

        // A comfortably distant expiry is refreshed a buffer ahead of it.
        let lifetime = Duration::from_secs(3600);
        let deadline = refresh_deadline(Some(Timestamp::now() + lifetime));
        let expected = Instant::now() + lifetime - VENDED_CREDENTIAL_REFRESH_BUFFER;
        assert!(
            deadline <= expected && deadline > expected - Duration::from_secs(60),
            "deadline is not a buffer ahead of the expiry"
        );

        // An expiry inside the buffer, or already past, is refreshed on the next call rather
        // than yielding a deadline reqsign would reject anyway.
        for offset in [VENDED_CREDENTIAL_REFRESH_BUFFER / 2, Duration::from_secs(0)] {
            assert!(refresh_deadline(Some(Timestamp::now() + offset)) <= Instant::now());
        }
    }

    #[mz_ore::test]
    fn test_announced_prefix() {
        // Overrides win, matching how the catalog client merges the two.
        let config = config_with(
            &[("prefix", "from-defaults")],
            &[("prefix", "from-overrides")],
        );
        assert_eq!(config.announced_prefix(), Some("from-overrides"));

        // Defaults are consulted only when overrides is silent, and an unrelated override does
        // not suppress them.
        let config = config_with(&[("prefix", "from-defaults")], &[("warehouse", "wh")]);
        assert_eq!(config.announced_prefix(), Some("from-defaults"));

        // A catalog that uses no prefix announces none.
        assert_eq!(config_with(&[], &[]).announced_prefix(), None);
    }

    #[mz_ore::test]
    fn test_catalog_config_url() {
        let url = |uri: &str, warehouse: Option<&str>| {
            catalog_config_url(&Url::parse(uri).expect("valid URI"), warehouse)
                .expect("URI is a base")
                .to_string()
        };

        // The config endpoint is never prefixed: the prefix is what it returns.
        assert_eq!(
            url("https://catalog.example/api", None),
            "https://catalog.example/api/v1/config"
        );

        // A trailing slash must not leave an empty segment behind as `//v1`.
        assert_eq!(
            url("https://catalog.example/api/", None),
            "https://catalog.example/api/v1/config"
        );
        assert_eq!(
            url("https://catalog.example", None),
            "https://catalog.example/v1/config"
        );

        // The warehouse rides along as a query parameter, percent-encoded.
        assert_eq!(
            url("https://catalog.example", Some("my catalog")),
            "https://catalog.example/v1/config?warehouse=my+catalog"
        );

        // A URI that cannot be a base has no path segments to extend.
        assert!(
            catalog_config_url(&Url::parse("mailto:nobody@example.com").unwrap(), None).is_err()
        );
    }

    #[mz_ore::test]
    fn test_table_credentials_url() {
        let url = |uri: &str, prefix: Option<&str>, t: &TableIdent| {
            table_credentials_url(&Url::parse(uri).expect("valid URI"), prefix, t)
                .expect("URI is a base")
                .to_string()
        };

        // A catalog announcing no prefix puts the resource path directly under `/v1`.
        assert_eq!(
            url(
                "https://catalog.example",
                None,
                &table(&["sales"], "orders")
            ),
            "https://catalog.example/v1/namespaces/sales/tables/orders/credentials"
        );

        // Unity Catalog's multi-segment prefix keeps its separator rather than being encoded
        // as one segment, and the base path of the configured URI is preserved.
        assert_eq!(
            url(
                "https://dbc.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
                Some("catalogs/sink-catalog"),
                &table(&["sink-namespace"], "mz_append_test")
            ),
            "https://dbc.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest/v1/\
             catalogs/sink-catalog/namespaces/sink-namespace/tables/mz_append_test/credentials"
        );

        // A trailing slash on either the URI or the prefix must not produce an empty segment.
        assert_eq!(
            url(
                "https://catalog.example/",
                Some("/catalogs/main/"),
                &table(&["sales"], "orders")
            ),
            "https://catalog.example/v1/catalogs/main/namespaces/sales/tables/orders/credentials"
        );

        // Multi-level namespaces are one segment joined by the unit separator the specification
        // mandates, which percent-encodes to `%1F`.
        assert_eq!(
            url(
                "https://catalog.example",
                None,
                &table(&["sales", "eu"], "orders")
            ),
            "https://catalog.example/v1/namespaces/sales%1Feu/tables/orders/credentials"
        );

        // Names that would otherwise change the path are escaped, not injected into it.
        assert_eq!(
            url("https://catalog.example", None, &table(&["a/b"], "c?d#e")),
            "https://catalog.example/v1/namespaces/a%2Fb/tables/c%3Fd%23e/credentials"
        );
    }
}
