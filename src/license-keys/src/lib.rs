// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, bail};
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};

#[cfg(feature = "signing")]
mod signing;
#[cfg(feature = "signing")]
pub use signing::{get_pubkey_pem, make_license_key};

const ISSUER: &str = "Materialize, Inc.";
// list of public keys which are allowed to validate license keys. this is a
// list to allow for key rotation if necessary.
const PUBLIC_KEYS: &[&str] = &[include_str!("license_keys/production.pub")];
// keys which we have issued but need to be revoked before their expiration
// (due to being accidentally exposed or similar).
const REVOKED_KEYS: &[&str] = &["eddaf004-dc1e-48cf-9cc1-41d1543d940a"];

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum ExpirationBehavior {
    Warn,
    DisableClusterCreation,
    Disable,
}

#[derive(Debug, Clone)]
pub struct ValidatedLicenseKey {
    pub id: String,
    pub organization: String,
    pub environment_id: String,
    pub expiration: u64,
    pub not_before: u64,

    pub max_credit_consumption_rate: f64,
    pub allow_credit_consumption_override: bool,
    pub expiration_behavior: ExpirationBehavior,
    pub expired: bool,
    /// Optional feature flags / third-party integrations enabled for this key
    /// (e.g. `"ory"` to permit pulling images through the OCI registry proxy).
    /// Empty for keys that predate this field.
    pub entitlements: Vec<String>,
}

impl ValidatedLicenseKey {
    pub fn for_tests() -> Self {
        Self {
            id: "".to_string(),
            organization: "".to_string(),
            environment_id: "".to_string(),
            expiration: 0,
            not_before: 0,

            max_credit_consumption_rate: 999999.0,
            allow_credit_consumption_override: true,
            expiration_behavior: ExpirationBehavior::Warn,
            expired: false,
            entitlements: Vec::new(),
        }
    }

    pub fn disabled() -> Self {
        Self {
            id: "".to_string(),
            organization: "".to_string(),
            environment_id: "".to_string(),
            expiration: 0,
            not_before: 0,

            max_credit_consumption_rate: 999999.0,
            allow_credit_consumption_override: true,
            expiration_behavior: ExpirationBehavior::Warn,
            expired: false,
            entitlements: Vec::new(),
        }
    }

    /// Returns true if `entitlement` is present in this key.
    pub fn has_entitlement(&self, entitlement: &str) -> bool {
        self.entitlements.iter().any(|e| e == entitlement)
    }

    pub fn max_credit_consumption_rate(&self) -> Option<f64> {
        if self.expired
            && matches!(
                self.expiration_behavior,
                ExpirationBehavior::DisableClusterCreation | ExpirationBehavior::Disable
            )
        {
            Some(0.0)
        } else if self.allow_credit_consumption_override {
            None
        } else {
            Some(self.max_credit_consumption_rate)
        }
    }
}

impl Default for ValidatedLicenseKey {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            organization: "".to_string(),
            environment_id: "".to_string(),
            expiration: 0,
            not_before: 0,

            max_credit_consumption_rate: 24.0,
            allow_credit_consumption_override: false,
            expiration_behavior: ExpirationBehavior::Disable,
            expired: false,
            entitlements: Vec::new(),
        }
    }
}

pub fn validate(license_key: &str) -> anyhow::Result<ValidatedLicenseKey> {
    let mut err = None;
    for pubkey in PUBLIC_KEYS {
        match validate_with_pubkey(license_key, pubkey) {
            Ok(key) => {
                return Ok(key);
            }
            Err(e) => {
                err = Some(e);
            }
        }
    }

    if let Some(err) = err {
        Err(err)
    } else {
        Err(anyhow!("no public key found"))
    }
}

fn validate_with_pubkey(
    license_key: &str,
    pubkey_pem: &str,
) -> anyhow::Result<ValidatedLicenseKey> {
    // don't just read the version out of the payload before verifying it,
    // trusting unsigned data to determine how to verify the signature is a
    // bad idea. instead, just try validating it as each version
    // independently, and if the signature is valid, only then check to
    // ensure that the version matches what we validated.

    // try current version first, so we can prefer that for error messages
    let res = validate_with_pubkey_v1(license_key, pubkey_pem);
    let err = match res {
        Ok(key) => return Ok(key),
        Err(e) => e,
    };

    let previous_versions: Vec<Box<dyn Fn() -> anyhow::Result<ValidatedLicenseKey>>> = vec![
        // add to this if/when we add new versions
        // for example,
        // Box::new(|| validate_with_pubkey_v1(license_key, pubkey_pem, environment_id)),
    ];
    for validator in previous_versions {
        if let Ok(key) = validator() {
            return Ok(key);
        }
    }

    Err(err)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    sub: String,
    exp: u64,
    nbf: u64,
    iss: String,
    aud: String,
    iat: u64,
    jti: String,

    version: u64,
    max_credit_consumption_rate: f64,
    #[serde(default, skip_serializing_if = "is_default")]
    allow_credit_consumption_override: bool,
    expiration_behavior: ExpirationBehavior,
    // Defaulted + skipped-when-empty so keys issued before entitlements
    // existed continue to validate and we don't bloat keys that don't need
    // any entitlements.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    entitlements: Vec<String>,
}

fn validate_with_pubkey_v1(
    license_key: &str,
    pubkey_pem: &str,
) -> anyhow::Result<ValidatedLicenseKey> {
    let mut validation = Validation::new(Algorithm::PS256);
    validation.set_required_spec_claims(&["exp", "nbf", "aud", "iss", "sub"]);
    validation.set_issuer(&[ISSUER]);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = false;

    let key = DecodingKey::from_rsa_pem(pubkey_pem.as_bytes())?;

    let (jwt, expired): (TokenData<Payload>, _) =
        jsonwebtoken::decode(license_key, &key, &validation).map_or_else(
            |e| {
                if matches!(e.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) {
                    validation.validate_exp = false;
                    Ok((jsonwebtoken::decode(license_key, &key, &validation)?, true))
                } else {
                    Err::<_, anyhow::Error>(e.into())
                }
            },
            |jwt| Ok((jwt, false)),
        )?;

    if jwt.header.typ.as_deref() != Some("JWT") {
        bail!("invalid jwt header type");
    }

    if jwt.claims.version != 1 {
        bail!("invalid license key version");
    }

    if !(jwt.claims.nbf..=jwt.claims.exp).contains(&jwt.claims.iat) {
        bail!("invalid issuance time");
    }

    if REVOKED_KEYS.contains(&jwt.claims.jti.as_str()) {
        bail!("revoked license key");
    }

    Ok(ValidatedLicenseKey {
        id: jwt.claims.jti,
        organization: jwt.claims.sub,
        environment_id: jwt.claims.aud,
        expiration: jwt.claims.exp,
        not_before: jwt.claims.nbf,

        max_credit_consumption_rate: jwt.claims.max_credit_consumption_rate,
        allow_credit_consumption_override: jwt.claims.allow_credit_consumption_override,
        expiration_behavior: jwt.claims.expiration_behavior,
        expired,
        entitlements: jwt.claims.entitlements,
    })
}

fn is_default<T: PartialEq + Eq + Default>(val: &T) -> bool {
    *val == T::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_payload(entitlements: Vec<String>) -> Payload {
        Payload {
            sub: "org-1".to_string(),
            exp: 200,
            nbf: 100,
            iss: ISSUER.to_string(),
            aud: "env-1".to_string(),
            iat: 100,
            jti: "jti-1".to_string(),
            version: 1,
            max_credit_consumption_rate: 10.0,
            allow_credit_consumption_override: false,
            expiration_behavior: ExpirationBehavior::Warn,
            entitlements,
        }
    }

    #[mz_ore::test]
    fn entitlements_roundtrip_through_payload() {
        let payload = sample_payload(vec!["ory".to_string(), "foo".to_string()]);
        let json = serde_json::to_string(&payload).unwrap();
        assert!(
            json.contains(r#""entitlements":["ory","foo"]"#),
            "payload should serialize entitlements: {json}"
        );

        let decoded: Payload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.entitlements, vec!["ory", "foo"]);
    }

    #[mz_ore::test]
    fn empty_entitlements_omitted_from_payload() {
        let payload = sample_payload(Vec::new());
        let json = serde_json::to_string(&payload).unwrap();
        // Skipping the field on empty keeps issued JWTs the same shape they
        // were before entitlements existed, so old + new validators agree.
        assert!(
            !json.contains("entitlements"),
            "empty entitlements should be skipped: {json}"
        );
    }

    #[mz_ore::test]
    fn legacy_payload_without_entitlements_decodes() {
        // Pre-DEP-130 keys have no `entitlements` field. They must still
        // deserialize, with entitlements defaulting to empty.
        let legacy = serde_json::json!({
            "sub": "org-1",
            "exp": 200,
            "nbf": 100,
            "iss": ISSUER,
            "aud": "env-1",
            "iat": 100,
            "jti": "jti-1",
            "version": 1,
            "max_credit_consumption_rate": 10.0,
            "expiration_behavior": "Warn",
        });
        let decoded: Payload = serde_json::from_value(legacy).unwrap();
        assert!(decoded.entitlements.is_empty());
    }
}
