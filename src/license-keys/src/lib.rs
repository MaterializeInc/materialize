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
const ANY_ENVIRONMENT_AUD: &str = "00000000-0000-0000-0000-000000000000";
// list of public keys which are allowed to validate license keys. this is a
// list to allow for key rotation if necessary.
const PUBLIC_KEYS: &[&str] = &[include_str!(
    "license_keys/8c5829bc-3981-49cf-9c0a-2a6e6649801c.pub"
)];

#[derive(Debug, Clone)]
pub struct ValidatedLicenseKey {
    pub organization_id: String,
    pub max_credit_consumption_rate: f64,
    pub allow_credit_consumption_override: bool,
    pub expired: bool,
}

pub fn validate(license_key: &str, environment_id: &str) -> anyhow::Result<ValidatedLicenseKey> {
    let mut err = anyhow!("no public key found");
    for pubkey in PUBLIC_KEYS {
        match validate_with_pubkey(license_key, pubkey, environment_id) {
            Ok(key) => {
                return Ok(key);
            }
            Err(e) => {
                err = e;
            }
        }
    }

    Err(err)
}

fn validate_with_pubkey(
    license_key: &str,
    pubkey_pem: &str,
    environment_id: &str,
) -> anyhow::Result<ValidatedLicenseKey> {
    // don't just read the version out of the payload before verifying it,
    // trusting unsigned data to determine how to verify the signature is a
    // bad idea. instead, just try validating it as each version
    // independently, and if the signature is valid, only then check to
    // ensure that the version matches what we validated.

    // try current version first, so we can prefer that for error messages
    let res = validate_with_pubkey_v1(license_key, pubkey_pem, environment_id);
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
}

fn validate_with_pubkey_v1(
    license_key: &str,
    pubkey_pem: &str,
    environment_id: &str,
) -> anyhow::Result<ValidatedLicenseKey> {
    let mut validation = Validation::new(Algorithm::PS256);
    validation.set_required_spec_claims(&["exp", "nbf", "aud", "iss", "sub"]);
    validation.set_audience(&[environment_id, ANY_ENVIRONMENT_AUD]);
    validation.set_issuer(&[ISSUER]);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = true;

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

    Ok(ValidatedLicenseKey {
        organization_id: jwt.claims.sub,
        max_credit_consumption_rate: jwt.claims.max_credit_consumption_rate,
        allow_credit_consumption_override: jwt.claims.allow_credit_consumption_override,
        expired,
    })
}

fn is_default<T: PartialEq + Eq + Default>(val: &T) -> bool {
    *val == T::default()
}
