// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use aws_lc_rs::digest;
use aws_sdk_kms::{
    primitives::Blob,
    types::{MessageType, SigningAlgorithmSpec},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, Header};
use pem::Pem;
use serde::Serialize;
use uuid::Uuid;

use crate::{ExpirationBehavior, INTERMEDIATE_TYP, ISSUER, IntermediatePayload, Payload};

const VERSION: u64 = 1;

pub async fn get_pubkey_pem(client: &aws_sdk_kms::Client, key_id: &str) -> anyhow::Result<String> {
    let pubkey = get_pubkey(client, key_id).await?;
    let pem = Pem::new("PUBLIC KEY", pubkey);
    Ok(pem.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn make_license_key(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    validity: Duration,
    organization_id: String,
    environment_id: String,
    max_credit_consumption_rate: f64,
    allow_credit_consumption_override: bool,
    expiration_behavior: ExpirationBehavior,
    entitlements: Vec<String>,
) -> anyhow::Result<String> {
    let now = SystemTime::now();
    let expiration = now + validity;
    let payload = Payload {
        sub: organization_id,
        exp: format_time(&expiration),
        nbf: format_time(&now),
        iss: ISSUER.to_string(),
        aud: environment_id,
        iat: format_time(&now),
        jti: Uuid::new_v4().to_string(),
        version: VERSION,
        max_credit_consumption_rate,
        allow_credit_consumption_override,
        expiration_behavior,
        entitlements,
    };
    sign_jwt(client, key_id, "JWT", &payload).await
}

/// Signs a certificate with the root key `root_key_id` that allows license
/// keys signed by the private half of `public_key_pem` to validate.
pub async fn make_intermediate_key(
    client: &aws_sdk_kms::Client,
    root_key_id: &str,
    validity: Duration,
    name: String,
    public_key_pem: String,
) -> anyhow::Result<String> {
    let now = SystemTime::now();
    let expiration = now + validity;
    let payload = IntermediatePayload {
        sub: name,
        exp: format_time(&expiration),
        nbf: format_time(&now),
        iss: ISSUER.to_string(),
        iat: format_time(&now),
        jti: Uuid::new_v4().to_string(),
        version: VERSION,
        public_key: public_key_pem,
    };
    sign_jwt(client, root_key_id, INTERMEDIATE_TYP, &payload).await
}

async fn sign_jwt(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    typ: &str,
    payload: &impl Serialize,
) -> anyhow::Result<String> {
    let mut headers = Header::new(Algorithm::PS256);
    headers.typ = Some(typ.to_string());
    let headers = URL_SAFE_NO_PAD.encode(serde_json::to_string(&headers).unwrap().as_bytes());
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(payload).unwrap().as_bytes());

    let signing_string = format!("{}.{}", headers, payload);
    let signature = URL_SAFE_NO_PAD.encode(sign(client, key_id, signing_string.as_bytes()).await?);

    Ok(format!("{}.{}", signing_string, signature))
}

async fn get_pubkey(client: &aws_sdk_kms::Client, key_id: &str) -> anyhow::Result<Vec<u8>> {
    if let Some(pubkey) = client
        .get_public_key()
        .key_id(key_id)
        .send()
        .await?
        .public_key
    {
        Ok(pubkey.into_inner())
    } else {
        Err(anyhow!("failed to get pubkey"))
    }
}

async fn sign(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    message: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let hash = digest::digest(&digest::SHA256, message);
    let digest = hash.as_ref().to_vec();

    if let Some(sig) = client
        .sign()
        .key_id(key_id)
        .signing_algorithm(SigningAlgorithmSpec::RsassaPssSha256)
        .message_type(MessageType::Digest)
        .message(Blob::new(digest))
        .send()
        .await?
        .signature
    {
        Ok(sig.into_inner())
    } else {
        Err(anyhow!("failed to get signature"))
    }
}

fn format_time(t: &SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
