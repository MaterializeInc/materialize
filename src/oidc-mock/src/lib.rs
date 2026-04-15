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

use aws_lc_rs::signature::KeyPair as _;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use jsonwebtoken::{EncodingKey, Header, encode};
use mz_ore::now::NowFn;
use mz_ore::task::JoinHandle;
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
        // Try PKCS#8 first (BEGIN PRIVATE KEY), then PKCS#1 (BEGIN RSA PRIVATE KEY).
        let der = pem_to_der(&encoding_key);
        let rsa_key = aws_lc_rs::rsa::KeyPair::from_pkcs8(&der)
            .or_else(|_| aws_lc_rs::rsa::KeyPair::from_der(&der))
            .map_err(|e| anyhow::anyhow!("failed to parse RSA key: {e}"))?;

        let addr = match addr {
            Some(addr) => Cow::Borrowed(addr),
            None => Cow::Owned(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        };

        let listener = TcpListener::bind(*addr).await.unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });
        let issuer = format!("http://{}", listener.local_addr().unwrap());

        // Extract RSA public key components for JWKS.
        // PublicKey::as_ref() gives PKCS#1 RSAPublicKey DER.
        let public_key_der = rsa_key.public_key().as_ref();
        let (n, e) = parse_rsa_public_key_der(public_key_der);
        let jwk = create_jwk(&kid, &n, &e);

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
        let sub_claim_map = BTreeMap::from([("sub".to_string(), sub.to_string())]);

        let unknown_claims = opts
            .unknown_claims
            .unwrap_or_default()
            .into_iter()
            .chain(sub_claim_map)
            .map(|(k, v)| (k, serde_json::Value::String(v.to_string())))
            .collect();

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

/// Creates a JWK from RSA public key components.
fn create_jwk(kid: &str, n: &[u8], e: &[u8]) -> Jwk {
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    Jwk {
        kty: "RSA".to_string(),
        kid: kid.to_string(),
        key_use: "sig".to_string(),
        alg: "RS256".to_string(),
        n: engine.encode(n),
        e: engine.encode(e),
    }
}

/// Strips PEM headers/footers and base64-decodes to DER bytes.
fn pem_to_der(pem: &str) -> Vec<u8> {
    let b64: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .expect("valid base64 in PEM")
}

/// Parses a PKCS#1 RSAPublicKey DER (RFC 8017) to extract (n, e).
///
/// Format: SEQUENCE { INTEGER n, INTEGER e }
fn parse_rsa_public_key_der(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut pos = 0;

    // Read a DER length field.
    let read_len = |data: &[u8], pos: &mut usize| -> usize {
        let b = data[*pos];
        *pos += 1;
        if b < 0x80 {
            usize::from(b)
        } else {
            let n = usize::from(b & 0x7f);
            let mut len = 0usize;
            for _ in 0..n {
                len = (len << 8) | usize::from(data[*pos]);
                *pos += 1;
            }
            len
        }
    };

    // Read an INTEGER, stripping the leading zero sign byte if present.
    let read_integer = |data: &[u8], pos: &mut usize| -> Vec<u8> {
        assert_eq!(data[*pos], 0x02, "expected INTEGER tag");
        *pos += 1;
        let len = read_len(data, pos);
        let mut value = &data[*pos..*pos + len];
        *pos += len;
        // Strip leading zero byte used for positive sign in DER.
        if value.first() == Some(&0) && value.len() > 1 {
            value = &value[1..];
        }
        value.to_vec()
    };

    // Outer SEQUENCE.
    assert_eq!(der[pos], 0x30, "expected SEQUENCE tag");
    pos += 1;
    let _seq_len = read_len(der, &mut pos);

    let n = read_integer(der, &mut pos);
    let e = read_integer(der, &mut pos);
    (n, e)
}
