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
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use jsonwebtoken::{EncodingKey, Header, encode};
use mz_authenticator::OidcClaims;
use mz_ore::now::NowFn;
use mz_ore::task::JoinHandle;
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

/// Options for generating JWT tokens.
#[derive(Debug, Clone, Default)]
pub struct GenerateJwtOptions<'a> {
    /// Optional email claim.
    pub email: Option<&'a str>,
    /// Custom expiration time. If None, uses server's default expires_in_secs.
    pub exp: Option<i64>,
    /// Custom issuer. If None, uses server's issuer.
    pub issuer: Option<&'a str>,
    /// Audience claim. If None, uses empty vec.
    pub aud: Option<Vec<String>>,
    /// Additional claims.
    pub unknown_claims: Option<BTreeMap<String, String>>,
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
    /// Handle to the server task.
    pub handle: JoinHandle<Result<(), std::io::Error>>,
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
        let handle = mz_ore::task::spawn(|| "oidc-mock-server", server.into_future());

        Ok(OidcMockServer {
            issuer,
            kid,
            encoding_key: encoding_key_typed,
            now,
            expires_in_secs,
            handle,
        })
    }

    /// Generates a JWT token for testing.
    ///
    /// # Arguments
    ///
    /// * `sub` - Subject (user identifier).
    /// * `opts` - Optional JWT generation options. Use `Default::default()` for defaults.
    pub fn generate_jwt(&self, sub: &str, opts: GenerateJwtOptions<'_>) -> String {
        let now_ms = (self.now)();
        let now_secs = i64::try_from(now_ms / 1000).expect("timestamp must fit in i64");

        let claims = OidcClaims {
            sub: sub.to_string(),
            iss: opts.issuer.unwrap_or(&self.issuer).to_string(),
            exp: opts.exp.unwrap_or(now_secs + self.expires_in_secs),
            iat: Some(now_secs),
            aud: opts.aud.unwrap_or_default(),
            unknown_claims: opts
                .unknown_claims
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v.to_string())))
                .collect(),
        };

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(self.kid.clone());

        encode(&header, &claims, &self.encoding_key).expect("failed to encode JWT")
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
