// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SASL mechanisms: PLAIN, SCRAM-SHA-256/512 (RFC 5802, RFC 7677) and
//! OAUTHBEARER (RFC 7628), plus the OAUTHBEARER token source.
//!
//! A mechanism is a client-side state machine: [`Mechanism::step`] takes the
//! broker's last message (none for the first step) and produces the next
//! client message, until the exchange is complete. Error texts follow
//! librdkafka's.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use rand::RngExt;
use sha2::{Digest, Sha256, Sha512};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::settings::{SaslMechanism, SaslSettings};
use crate::error::KafkaError;
use crate::internal::cluster::EventQueue;
use crate::internal::hooks::{self, Hooks};
use crate::types::RDKafkaErrorCode;

/// One SASL exchange.
pub(crate) enum Mechanism {
    Plain(Option<Vec<u8>>),
    Scram(Box<Scram>),
    OAuthBearer(OAuthBearer),
}

/// What a mechanism step produced.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Send this message to the broker and pass its answer to the next step.
    Send(Vec<u8>),
    /// Authentication is complete.
    Done,
}

impl Mechanism {
    /// Starts an exchange. `token` is the current OAUTHBEARER token.
    pub(crate) fn new(
        mechanism: SaslMechanism,
        sasl: &SaslSettings,
        token: Option<&TokenValue>,
    ) -> Result<Mechanism, String> {
        let username = sasl.username.as_deref().unwrap_or("");
        let password = sasl.password.as_deref().unwrap_or("");
        Ok(match mechanism {
            SaslMechanism::Plain => {
                let mut msg = Vec::with_capacity(2 + username.len() + password.len());
                msg.push(0);
                msg.extend_from_slice(username.as_bytes());
                msg.push(0);
                msg.extend_from_slice(password.as_bytes());
                Mechanism::Plain(Some(msg))
            }
            SaslMechanism::ScramSha256 => Mechanism::Scram(Box::new(Scram::new(
                ScramHash::Sha256,
                username,
                password,
                &generate_nonce(),
            ))),
            SaslMechanism::ScramSha512 => Mechanism::Scram(Box::new(Scram::new(
                ScramHash::Sha512,
                username,
                password,
                &generate_nonce(),
            ))),
            SaslMechanism::OAuthBearer => match token {
                Some(token) => Mechanism::OAuthBearer(OAuthBearer::new(token)),
                None => return Err("no token".into()),
            },
        })
    }

    /// Produces the next client message given the broker's last message.
    pub(crate) fn step(&mut self, server: Option<&[u8]>) -> Result<Step, String> {
        match self {
            Mechanism::Plain(first) => Ok(match first.take() {
                Some(msg) => Step::Send(msg),
                None => Step::Done,
            }),
            Mechanism::Scram(scram) => scram.step(server),
            Mechanism::OAuthBearer(oauth) => oauth.step(server),
        }
    }
}

/// librdkafka's SCRAM nonce: 32 random printable characters from `-` to
/// `~`, which excludes `,`.
fn generate_nonce() -> String {
    let mut rng = rand::rng();
    (0..32)
        .map(|_| char::from(rng.random_range(0x2du8..=0x7eu8)))
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScramHash {
    Sha256,
    Sha512,
}

impl ScramHash {
    fn hmac(self, key: &[u8], parts: &[&[u8]]) -> Vec<u8> {
        match self {
            ScramHash::Sha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key length");
                for p in parts {
                    mac.update(p);
                }
                mac.finalize().into_bytes().to_vec()
            }
            ScramHash::Sha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("any key length");
                for p in parts {
                    mac.update(p);
                }
                mac.finalize().into_bytes().to_vec()
            }
        }
    }

    fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            ScramHash::Sha256 => Sha256::digest(data).to_vec(),
            ScramHash::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    /// `Hi()` of RFC 5802: PBKDF2 with HMAC and one output block.
    fn hi(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut u = self.hmac(password, &[salt, &1u32.to_be_bytes()]);
        let mut out = u.clone();
        for _ in 1..iterations {
            u = self.hmac(password, &[&u]);
            for (i, x) in u.iter().enumerate() {
                out[i] ^= x;
            }
        }
        out
    }
}

enum ScramState {
    ClientFirst,
    ServerFirst,
    ServerFinal { server_signature: Vec<u8> },
    Done,
}

/// The client side of SCRAM (RFC 5802) without channel binding.
pub(crate) struct Scram {
    hash: ScramHash,
    password: String,
    nonce: String,
    client_first_bare: String,
    state: ScramState,
}

/// SCRAM `saslname` escaping: `=` and `,` in user names.
fn scram_escape(name: &str) -> String {
    name.replace('=', "=3D").replace(',', "=2C")
}

/// Finds attribute `attr` in a SCRAM message, like librdkafka's
/// `rd_kafka_sasl_scram_get_attr`.
fn scram_attr(msg: &str, attr: char) -> Option<&str> {
    msg.split(',').find_map(|part| {
        let mut chars = part.chars();
        (chars.next() == Some(attr) && chars.next() == Some('=')).then(|| &part[2..])
    })
}

impl Scram {
    pub(crate) fn new(hash: ScramHash, username: &str, password: &str, nonce: &str) -> Scram {
        Scram {
            hash,
            password: password.to_string(),
            nonce: nonce.to_string(),
            client_first_bare: format!("n={},r={}", scram_escape(username), nonce),
            state: ScramState::ClientFirst,
        }
    }

    fn step(&mut self, server: Option<&[u8]>) -> Result<Step, String> {
        match std::mem::replace(&mut self.state, ScramState::Done) {
            ScramState::ClientFirst => {
                self.state = ScramState::ServerFirst;
                Ok(Step::Send(
                    format!("n,,{}", self.client_first_bare).into_bytes(),
                ))
            }
            ScramState::ServerFirst => {
                let server_first = String::from_utf8_lossy(server.unwrap_or_default()).into_owned();
                if scram_attr(&server_first, 'm').is_some() {
                    return Err("Unsupported mandatory SCRAM extension".into());
                }
                let server_nonce = scram_attr(&server_first, 'r').ok_or_else(|| {
                    "Server nonce in server-first-message: could not find attribute (r)".to_string()
                })?;
                if !server_nonce.starts_with(&self.nonce) {
                    return Err("Server/client nonce mismatch in server-first-message".into());
                }
                let salt_b64 = scram_attr(&server_first, 's').ok_or_else(|| {
                    "Salt in server-first-message: could not find attribute (s)".to_string()
                })?;
                let salt = BASE64
                    .decode(salt_b64)
                    .map_err(|_| "Invalid Base64 Salt in server-first-message".to_string())?;
                let iterations = scram_attr(&server_first, 'i')
                    .ok_or_else(|| {
                        "Iteration count in server-first-message: could not find attribute (i)"
                            .to_string()
                    })?
                    .parse::<u32>()
                    .ok()
                    .filter(|i| *i > 0)
                    .ok_or_else(|| {
                        "Invalid value (not integer or too large) for Iteration count in \
                         server-first-message"
                            .to_string()
                    })?;
                let h = self.hash;
                let salted = h.hi(self.password.as_bytes(), &salt, iterations);
                let client_key = h.hmac(&salted, &[b"Client Key"]);
                let stored_key = h.hash(&client_key);
                // "biws" is base64("n,,"): no channel binding, no authzid.
                let final_wo_proof = format!("c=biws,r={server_nonce}");
                let auth_message = format!(
                    "{},{},{}",
                    self.client_first_bare, server_first, final_wo_proof
                );
                let client_signature = h.hmac(&stored_key, &[auth_message.as_bytes()]);
                let proof: Vec<u8> = client_key
                    .iter()
                    .enumerate()
                    .map(|(i, a)| a ^ client_signature[i])
                    .collect();
                let server_key = h.hmac(&salted, &[b"Server Key"]);
                let server_signature = h.hmac(&server_key, &[auth_message.as_bytes()]);
                self.state = ScramState::ServerFinal { server_signature };
                Ok(Step::Send(
                    format!("{final_wo_proof},p={}", BASE64.encode(proof)).into_bytes(),
                ))
            }
            ScramState::ServerFinal { server_signature } => {
                let server_final = String::from_utf8_lossy(server.unwrap_or_default()).into_owned();
                if let Some(e) = scram_attr(&server_final, 'e') {
                    return Err(format!(
                        "SASL SCRAM authentication failed: broker responded with {e}"
                    ));
                }
                let Some(v) = scram_attr(&server_final, 'v') else {
                    return Err(
                        "SASL SCRAM authentication failed: no verifier or server-error \
                                returned from broker"
                            .into(),
                    );
                };
                let ours = BASE64.encode(&server_signature);
                if v != ours {
                    return Err(format!(
                        "SASL SCRAM authentication failed: ServerSignature mismatch \
                         (server's {v} != ours {ours})"
                    ));
                }
                Ok(Step::Done)
            }
            ScramState::Done => Ok(Step::Done),
        }
    }
}

/// A token to authenticate with.
#[derive(Clone, Debug)]
pub(crate) struct TokenValue {
    pub value: String,
    pub principal: String,
    /// Expiry in milliseconds since the Unix epoch.
    pub lifetime_ms: i64,
}

enum OAuthState {
    ClientFirst,
    ServerFirst,
    /// The broker sent an error; the client acknowledged it.
    ServerError(String),
    Done,
}

/// The client side of OAUTHBEARER (RFC 7628) without extensions.
pub(crate) struct OAuthBearer {
    token: TokenValue,
    state: OAuthState,
}

impl OAuthBearer {
    fn new(token: &TokenValue) -> OAuthBearer {
        OAuthBearer {
            token: token.clone(),
            state: OAuthState::ClientFirst,
        }
    }

    fn step(&mut self, server: Option<&[u8]>) -> Result<Step, String> {
        match std::mem::replace(&mut self.state, OAuthState::Done) {
            OAuthState::ClientFirst => {
                self.state = OAuthState::ServerFirst;
                // gs2-header "n,," with an empty authzid, then the auth
                // key-value pair; librdkafka sends no principal here.
                Ok(Step::Send(
                    format!("n,,\x01auth=Bearer {}\x01\x01", self.token.value).into_bytes(),
                ))
            }
            OAuthState::ServerFirst => match server.filter(|s| !s.is_empty()) {
                None => Ok(Step::Done),
                Some(err) => {
                    // RFC 7628 3.2.2: acknowledge the error with a kvsep.
                    self.state = OAuthState::ServerError(String::from_utf8_lossy(err).into_owned());
                    Ok(Step::Send(b"\x01".to_vec()))
                }
            },
            OAuthState::ServerError(msg) => Err(format!(
                "SASL OAUTHBEARER authentication failed (principal={}): {msg}",
                self.token.principal
            )),
            OAuthState::Done => Ok(Step::Done),
        }
    }
}

/// The current OAUTHBEARER token and the last token error.
#[derive(Clone, Debug, Default)]
pub(crate) struct TokenState {
    pub token: Option<TokenValue>,
    pub error: Option<String>,
    /// Whether at least one attempt to obtain a token completed.
    pub attempted: bool,
}

/// The OAUTHBEARER token source of one client: calls
/// [`ClientContext::generate_oauth_token`](crate::ClientContext::generate_oauth_token)
/// at creation, again when 80% of the token's remaining lifetime has passed,
/// and 10 seconds after a failure, like librdkafka.
pub(crate) struct OAuthTokens {
    state: watch::Sender<TokenState>,
}

impl OAuthTokens {
    /// Starts the refresh task, which stops when `shutdown` is cancelled.
    /// Token errors are raised on `events`.
    ///
    /// `enabled` is `ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`; without it
    /// there is no token source and authentication fails.
    pub(crate) fn start(
        hooks: Arc<dyn Hooks>,
        events: Arc<EventQueue>,
        config: Option<String>,
        enabled: bool,
        shutdown: CancellationToken,
    ) -> Arc<OAuthTokens> {
        let (tx, _) = watch::channel(TokenState::default());
        let tokens = Arc::new(OAuthTokens { state: tx });
        if !enabled {
            tokens.state.send_modify(|s| s.attempted = true);
            return tokens;
        }
        let weak = Arc::downgrade(&tokens);
        crate::internal::runtime::spawn("kafka-oauth-refresh", async move {
            loop {
                let result = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    r = hooks::generate_oauth_token(&hooks, config.clone()) => r,
                };
                let Some(tokens) = weak.upgrade() else {
                    return;
                };
                let now_ms = now_millis();
                let result = result.and_then(|t| {
                    if t.lifetime_ms <= now_ms {
                        Err(format!(
                            "Must supply an unexpired token: now={now_ms}ms, exp={}ms",
                            t.lifetime_ms
                        ))
                    } else {
                        Ok(t)
                    }
                });
                let wait = match result {
                    Ok(t) => {
                        let remaining = u64::try_from(t.lifetime_ms - now_ms).unwrap_or(0);
                        tokens.state.send_modify(|s| {
                            s.token = Some(TokenValue {
                                value: t.token,
                                principal: t.principal_name,
                                lifetime_ms: t.lifetime_ms,
                            });
                            s.attempted = true;
                        });
                        Duration::from_millis(remaining * 8 / 10)
                    }
                    Err(e) => {
                        let changed = tokens.state.borrow().error.as_deref() != Some(e.as_str());
                        // A previous token may still be valid; keep it.
                        tokens.state.send_modify(|s| {
                            s.error = Some(e.clone());
                            s.attempted = true;
                        });
                        if changed {
                            events.error(
                                KafkaError::Global(RDKafkaErrorCode::Authentication),
                                &format!("Failed to acquire SASL OAUTHBEARER token: {e}"),
                            );
                        }
                        Duration::from_secs(10)
                    }
                };
                drop(tokens);
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        });
        tokens
    }

    /// Waits until the first token attempt has completed, then returns the
    /// current token, or librdkafka's error text if there is none.
    pub(crate) async fn current(&self) -> Result<TokenValue, String> {
        let mut rx = self.state.subscribe();
        let state = rx
            .wait_for(|s| s.attempted)
            .await
            .map(|s| s.clone())
            .unwrap_or_default();
        state.token.ok_or_else(|| {
            format!(
                "OAUTHBEARER cannot log in because there is no token available; last error: {}",
                state.error.as_deref().unwrap_or("(not available)")
            )
        })
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_scram(
        hash: ScramHash,
        user: &str,
        password: &str,
        nonce: &str,
        server_first: &str,
        expected_final: &str,
        server_final: &str,
    ) {
        let mut scram = Scram::new(hash, user, password, nonce);
        let Step::Send(first) = scram.step(None).unwrap() else {
            panic!()
        };
        assert_eq!(
            String::from_utf8(first).unwrap(),
            format!("n,,n={user},r={nonce}")
        );
        let Step::Send(fin) = scram.step(Some(server_first.as_bytes())).unwrap() else {
            panic!()
        };
        assert_eq!(String::from_utf8(fin).unwrap(), expected_final);
        assert_eq!(
            scram.step(Some(server_final.as_bytes())).unwrap(),
            Step::Done
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn scram_sha256_rfc7677_vector() {
        // RFC 7677, section 3.
        run_scram(
            ScramHash::Sha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO",
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn scram_sha1_rfc5802_structure_with_sha256_hi() {
        // RFC 5802's vector uses SHA-1, which this client does not offer;
        // check `Hi` against the PBKDF2-HMAC-SHA256 test vector of RFC 7914
        // section 11 (c = 1, dkLen = 32) instead.
        let out = ScramHash::Sha256.hi(b"passwd", b"salt", 1);
        assert_eq!(
            out,
            [
                0x55, 0xac, 0x04, 0x6e, 0x56, 0xe3, 0x08, 0x9f, 0xec, 0x16, 0x91, 0xc2, 0x25, 0x44,
                0xb6, 0x05, 0xf9, 0x41, 0x85, 0x21, 0x6d, 0xde, 0x04, 0x65, 0xe6, 0x8b, 0x9d, 0x57,
                0xc2, 0x0d, 0xac, 0xbc
            ]
        );
    }

    #[mz_ore::test]
    fn scram_errors() {
        let mut scram = Scram::new(ScramHash::Sha512, "u", "p", "abc");
        scram.step(None).unwrap();
        assert_eq!(
            scram.step(Some(b"r=xyz,s=AAAA,i=1")).unwrap_err(),
            "Server/client nonce mismatch in server-first-message"
        );
        let mut scram = Scram::new(ScramHash::Sha512, "u", "p", "abc");
        scram.step(None).unwrap();
        assert_eq!(
            scram.step(Some(b"r=abcd,s=!!,i=1")).unwrap_err(),
            "Invalid Base64 Salt in server-first-message"
        );
        let mut scram = Scram::new(ScramHash::Sha512, "u", "p", "abc");
        scram.step(None).unwrap();
        scram.step(Some(b"r=abcd,s=AAAA,i=2")).unwrap();
        assert_eq!(
            scram.step(Some(b"e=invalid-proof")).unwrap_err(),
            "SASL SCRAM authentication failed: broker responded with invalid-proof"
        );
        assert_eq!(scram_escape("a=b,c"), "a=3Db=2Cc");
        let nonce = generate_nonce();
        assert_eq!(nonce.len(), 32);
        assert!(!nonce.contains(','));
    }

    #[mz_ore::test]
    fn plain_and_oauthbearer() {
        let sasl = SaslSettings {
            mechanism: Some(SaslMechanism::Plain),
            username: Some("user".into()),
            password: Some("pw".into()),
            oauthbearer_config: None,
        };
        let mut plain = Mechanism::new(SaslMechanism::Plain, &sasl, None).unwrap();
        assert_eq!(
            plain.step(None).unwrap(),
            Step::Send(b"\0user\0pw".to_vec())
        );
        assert_eq!(plain.step(Some(b"")).unwrap(), Step::Done);

        let token = TokenValue {
            value: "tok".into(),
            principal: String::new(),
            lifetime_ms: 0,
        };
        let mut oauth = Mechanism::new(SaslMechanism::OAuthBearer, &sasl, Some(&token)).unwrap();
        assert_eq!(
            oauth.step(None).unwrap(),
            Step::Send(b"n,,\x01auth=Bearer tok\x01\x01".to_vec())
        );
        assert_eq!(oauth.step(Some(b"")).unwrap(), Step::Done);

        let mut oauth = Mechanism::new(SaslMechanism::OAuthBearer, &sasl, Some(&token)).unwrap();
        oauth.step(None).unwrap();
        assert_eq!(
            oauth.step(Some(b"{\"status\":\"invalid_token\"}")).unwrap(),
            Step::Send(b"\x01".to_vec())
        );
        assert_eq!(
            oauth.step(Some(b"")).unwrap_err(),
            "SASL OAUTHBEARER authentication failed (principal=): {\"status\":\"invalid_token\"}"
        );
        assert!(Mechanism::new(SaslMechanism::OAuthBearer, &sasl, None).is_err());
    }
}
