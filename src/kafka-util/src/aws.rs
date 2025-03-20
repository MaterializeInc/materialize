// Copyright 2024 Yuhao Su. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from:
//
//    https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs
//
// It was incorporated directly into Materialize on August 1, 2024.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! AWS integration for Kafka.

use std::time::{Duration, SystemTime};

use anyhow::{bail, Context};
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use aws_types::SdkConfig;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::NaiveDateTime;
use thiserror::Error;
use url::Url;

/// The default expiration time in seconds.
const DEFAULT_EXPIRY_SECONDS: u32 = 900;

/// An error while signing an AWS IAM URL.
#[derive(Error, Debug)]
pub enum SignerError {
    /// An error while fetching AWS credentials.
    #[error("failed to provide credentials: {0}")]
    ProvideCredentials(#[from] CredentialsError),
    /// An error constructing the authentication token.
    #[error("failed constuct auth token: {0}")]
    ConstructAuthToken(String),
}

/// Generate a base64-encoded signed url as an auth token by loading IAM
/// credentials from an AWS credentials provider.
pub async fn generate_auth_token(sdk_config: &SdkConfig) -> Result<(String, i64), anyhow::Error> {
    let Some(region) = sdk_config.region() else {
        bail!("internal error: AWS configuration missing region");
    };

    let Some(credentials_provider) = sdk_config.credentials_provider() else {
        bail!("internal error: AWS configuration missing credentials");
    };
    let credentials = credentials_provider.provide_credentials().await?;

    // TODO: figure out how to generate the endpoint from the SDK configuration
    // to support localstack, FIPS, etc. The SDK does not make this easy, so for
    // now we just hardcode the endpoint construction for the major AWS regions.
    let endpoint_url = format!("https://kafka.{}.amazonaws.com", region);

    let mut url = build_url(&endpoint_url).context("failed to build request for signing")?;

    sign_url(&mut url, region, credentials).context("failed to sign request with aws sig v4")?;

    let expiration_time_ms =
        get_expiration_time_ms(&url).context("failed to extract expiration from signed url")?;

    url.query_pairs_mut()
        .append_pair("User-Agent", "materialize");

    Ok((base64_encode(url), expiration_time_ms))
}

fn build_url(endpoint_url: &str) -> Result<Url, anyhow::Error> {
    let mut url = Url::parse(endpoint_url).context("failed to parse url: {e}")?;
    url.query_pairs_mut()
        .append_pair("Action", "kafka-cluster:Connect");
    Ok(url)
}

fn sign_url(url: &mut Url, region: &Region, credentials: Credentials) -> Result<(), anyhow::Error> {
    use aws_sigv4::http_request::{
        sign, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
    };
    use aws_sigv4::sign::v4;

    let mut signing_settings = SigningSettings::default();
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(Duration::from_secs(u64::from(DEFAULT_EXPIRY_SECONDS)));
    let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region.as_ref())
        .name("kafka-cluster")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .context("failed to build signing parameters")?;
    let signable_request = SignableRequest::new(
        "GET",
        url.as_str(),
        std::iter::empty(),
        SignableBody::Bytes(&[]),
    )
    .expect("signable request");

    let sign_output =
        sign(signable_request, &signing_params.into()).context("failed to build sign request")?;
    let (sign_instructions, _) = sign_output.into_parts();

    let mut url_queries = url.query_pairs_mut();
    for (name, value) in sign_instructions.params() {
        url_queries.append_pair(name, value);
    }
    Ok(())
}

fn get_expiration_time_ms(signed_url: &Url) -> Result<i64, anyhow::Error> {
    let (_name, value) = &signed_url
        .query_pairs()
        .find(|(name, _value)| name == "X-Amz-Date")
        .unwrap_or_else(|| ("".into(), "".into()));

    let date_time = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
        .with_context(|| format!("failed to parse 'X-Amz-Date' param {value} from signed url"))?;

    let signing_time_ms = date_time.and_utc().timestamp_millis();

    Ok(signing_time_ms + i64::from(DEFAULT_EXPIRY_SECONDS) * 1000)
}

fn base64_encode(signed_url: Url) -> String {
    BASE64_URL_SAFE_NO_PAD.encode(signed_url.as_str().as_bytes())
}
