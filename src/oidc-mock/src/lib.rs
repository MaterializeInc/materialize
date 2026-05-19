// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! OIDC mock server for testing.
//!
//! This module provides a mock OIDC server that serves JWKS endpoints
//! for validating JWT tokens in tests.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use jsonwebtoken::{EncodingKey, Header, encode};
use mz_ore::now::NowFn;
use mz_ore::task::AbortOnDropHandle;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

/// JWKS response structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JwkSet {
    pub keys: Vec<Jwk>,
}

/// JSON Web Key structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Jwk {
    pub kty: String,
    pub kid: String,
    #[serde(rename = "use")]
    pub key_use: String,
    pub alg: String,
    pub n: String,
    pub e: String,
}

/// Shared context for the OIDC mock server.
struct OidcMockContext {
    /// The issuer URL (base URL of this server).
    issuer: String,
    /// RSA public key in JWK format.
    jwk: Jwk,
}

/// Audience claim value: either a single string or a list of strings.
///
/// Serializes as a JSON string when `Single`, and as a JSON array when `Multiple`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AudClaim {
    Single(String),
    Multiple(Vec<String>),
}

impl Default for AudClaim {
    fn default() -> Self {
        AudClaim::Multiple(vec![])
    }
}

/// Claims struct used for JWT encoding in the mock server.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockClaims {
    iss: String,
    exp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    iat: Option<i64>,
    aud: AudClaim,
    #[serde(flatten)]
    unknown_claims: BTreeMap<String, serde_json::Value>,
}

/// Options for generating JWT tokens.
#[derive(Debug, Clone, Default)]
pub struct GenerateJwtOptions<'a> {
    /// Optional email claim.
    pub email: Option<&'a str>,
    /// Custom expiration time. If None, uses server's default expires_in_secs.
    pub exp: Option<i64>,
    /// Custom issuer. If None, uses server's issuer.
    pub issuer: Option<&'a str>,
    /// Audience claim. If None, uses empty array.
    /// Use `AudClaim::Single` for a single string or `AudClaim::Multiple` for an array.
    pub aud: Option<AudClaim>,
    /// Additional claims as arbitrary JSON values (e.g., arrays for group claims).
    pub extra_claims: Option<BTreeMap<String, serde_json::Value>>,
}

/// OIDC mock server for testing.
pub struct OidcMockServer {
    /// The issuer URL. Used as the base URL of the server
    /// and as the issuer for JWT iss claim.
    pub issuer: String,
    /// Key ID used in JWT headers.
    pub kid: String,
    /// Encoding key for signing JWTs (for generating test tokens).
    pub encoding_key: EncodingKey,
    /// Function for getting current time.
    pub now: NowFn,
    /// How long tokens should be valid (in seconds).
    pub expires_in_secs: i64,
    /// Handle to the server task. Aborts the task when dropped.
    pub handle: AbortOnDropHandle<Result<(), std::io::Error>>,
    /// Per-user group memberships. When set, `generate_jwt` auto-includes
    /// the `groups` claim for any user with a registered group list.
    /// Can be overridden by passing `extra_claims` in `GenerateJwtOptions`.
    pub user_groups: Arc<Mutex<BTreeMap<String, Vec<String>>>>,
}

impl OidcMockServer {
    /// Starts an [`OidcMockServer`].
    ///
    /// Must be started from within a [`tokio::runtime::Runtime`].
    ///
    /// # Arguments
    ///
    /// * `addr` - Optional address to bind to. If None, binds to localhost on a random port.
    /// * `encoding_key` - PEM-encoded RSA private key string for signing JWTs.
    /// * `kid` - Key ID to use in JWT headers and JWKS.
    /// * `now` - Function for getting current time.
    /// * `expires_in_secs` - How long tokens should be valid.
    pub async fn start(
        addr: Option<&SocketAddr>,
        encoding_key: String,
        kid: String,
        now: NowFn,
        expires_in_secs: i64,
    ) -> Result<OidcMockServer, anyhow::Error> {
        // Convert PEM string to key.
        let encoding_key_typed = EncodingKey::from_rsa_pem(encoding_key.as_bytes())?;

        // Parse the private key PEM to extract RSA components for JWKS.
        let pkey = PKey::private_key_from_pem(encoding_key.as_bytes())?;
        let rsa = pkey.rsa().expect("pkey should be RSA");

        let addr = match addr {
            Some(addr) => Cow::Borrowed(addr),
            None => Cow::Owned(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        };

        let listener = TcpListener::bind(*addr).await.unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });
        let issuer = format!("http://{}", listener.local_addr().unwrap());

        // Extract RSA public key components from the decoding key
        // We need to serialize the public key to get n and e values
        let jwk = create_jwk(&kid, &rsa);

        let context = Arc::new(OidcMockContext {
            issuer: issuer.clone(),
            jwk,
        });

        let router = Router::new()
            .route("/.well-known/jwks.json", get(handle_jwks))
            .route(
                "/.well-known/openid-configuration",
                get(handle_openid_config),
            )
            .with_state(context);

        let server = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        );
        println!("oidc-mock listening...");
        println!(" HTTP address: {}", issuer);
        let handle =
            mz_ore::task::spawn(|| "oidc-mock-server", server.into_future()).abort_on_drop();

        Ok(OidcMockServer {
            issuer,
            kid,
            encoding_key: encoding_key_typed,
            now,
            expires_in_secs,
            handle,
            user_groups: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Generates a JWT token for testing.
    ///
    /// If `opts.extra_claims` does not include a `"groups"` key and the user
    /// has registered groups via [`Self::set_user_groups`], the groups are
    /// automatically included as the `"groups"` claim.
    ///
    /// # Arguments
    ///
    /// * `sub` - Subject (user identifier).
    /// * `opts` - Optional JWT generation options. Use `Default::default()` for defaults.
    pub fn generate_jwt(&self, sub: &str, opts: GenerateJwtOptions<'_>) -> String {
        let now_ms = (self.now)();
        let now_secs = i64::try_from(now_ms / 1000).expect("timestamp must fit in i64");
        let sub_claim_map = BTreeMap::from([("sub".to_string(), sub.to_string())]);

        let mut unknown_claims: BTreeMap<String, serde_json::Value> = sub_claim_map
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();

        // Auto-include groups from the registry unless the caller already
        // provided a "groups" key in extra_claims.
        let extra_has_groups = opts
            .extra_claims
            .as_ref()
            .is_some_and(|e| e.contains_key("groups"));
        if !extra_has_groups {
            if let Some(groups) = self.user_groups.lock().unwrap().get(sub) {
                let arr: serde_json::Value = groups
                    .iter()
                    .map(|g| serde_json::Value::String(g.clone()))
                    .collect::<Vec<_>>()
                    .into();
                unknown_claims.insert("groups".to_string(), arr);
            }
        }

        if let Some(extra) = opts.extra_claims {
            unknown_claims.extend(extra);
        }

        let claims = MockClaims {
            iss: opts.issuer.unwrap_or(&self.issuer).to_string(),
            exp: opts.exp.unwrap_or(now_secs + self.expires_in_secs),
            iat: Some(now_secs),
            aud: opts.aud.unwrap_or_default(),
            unknown_claims,
        };

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(self.kid.clone());

        encode(&header, &claims, &self.encoding_key).expect("failed to encode JWT")
    }

    /// Generates a JWT with an explicit `groups` claim.
    ///
    /// Shorthand for passing `groups` via [`GenerateJwtOptions::extra_claims`].
    ///
    /// # Arguments
    ///
    /// * `sub` - Subject (user identifier).
    /// * `groups` - Group names to include in the `"groups"` claim.
    pub fn generate_jwt_with_groups(&self, sub: &str, groups: &[&str]) -> String {
        let groups_val: serde_json::Value = groups
            .iter()
            .map(|g| serde_json::Value::String(g.to_string()))
            .collect::<Vec<_>>()
            .into();
        self.generate_jwt(
            sub,
            GenerateJwtOptions {
                extra_claims: Some(BTreeMap::from([("groups".to_string(), groups_val)])),
                ..Default::default()
            },
        )
    }

    /// Registers the group memberships for a user.
    ///
    /// After calling this, [`Self::generate_jwt`] will automatically include the
    /// `"groups"` claim for `sub` unless overridden by `extra_claims`.
    /// Passing an empty slice clears all groups for the user.
    pub fn set_user_groups(&self, sub: &str, groups: &[&str]) {
        self.user_groups.lock().unwrap().insert(
            sub.to_string(),
            groups.iter().map(|s| s.to_string()).collect(),
        );
    }

    /// Removes all registered group memberships for a user.
    pub fn clear_user_groups(&self, sub: &str) {
        self.user_groups.lock().unwrap().remove(sub);
    }

    /// Returns the JWKS URL for this server.
    pub fn jwks_url(&self) -> String {
        format!("{}/.well-known/jwks.json", self.issuer)
    }
}

/// Handler for JWKS endpoint.
async fn handle_jwks(State(context): State<Arc<OidcMockContext>>) -> Json<JwkSet> {
    Json(JwkSet {
        keys: vec![context.jwk.clone()],
    })
}

/// OpenID Configuration response.
#[derive(Serialize)]
struct OpenIdConfiguration {
    issuer: String,
    jwks_uri: String,
}

/// Handler for OpenID Configuration endpoint.
async fn handle_openid_config(
    State(context): State<Arc<OidcMockContext>>,
) -> Json<OpenIdConfiguration> {
    Json(OpenIdConfiguration {
        issuer: context.issuer.clone(),
        jwks_uri: format!("{}/.well-known/jwks.json", context.issuer),
    })
}

/// Creates a JWK from RSA key components.
fn create_jwk(kid: &str, rsa: &Rsa<Private>) -> Jwk {
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let n = rsa.n().to_vec();
    let e = rsa.e().to_vec();

    Jwk {
        kty: "RSA".to_string(),
        kid: kid.to_string(),
        key_use: "sig".to_string(),
        alg: "RS256".to_string(),
        n: engine.encode(n),
        e: engine.encode(e),
    }
}
