// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions for AWS.

use anyhow::{anyhow, Context};
use hyper_tls::HttpsConnector;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, ChainProvider, ProvideAwsCredentials, StaticProvider,
};
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};
use serde::{Deserialize, Serialize};
use tokio::time::{self, Duration};

pub(crate) const DEFAULT_AWS_TIMEOUT: Duration = Duration::from_secs(5);

/// Information required to connnect to AWS
///
/// Credentials are optional because in most cases users should use the
/// [`ChainProvider`] to pull information from the process or AWS environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectInfo {
    /// The AWS Region to connect to
    pub region: Region,
    /// Credentials, if missing will be obtained from environment
    pub credentials: Option<Credentials>,
}

/// A thin dupe of [`AwsCredentials`] so we can impl Serialize
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    key: String,
    secret: String,
    token: Option<String>,
}

impl From<Credentials> for AwsCredentials {
    fn from(creds: Credentials) -> AwsCredentials {
        AwsCredentials::new(creds.key, creds.secret, creds.token, None)
    }
}

impl ConnectInfo {
    /// Construct a ConnectInfo
    pub fn new(
        region: Region,
        key: Option<String>,
        secret: Option<String>,
        token: Option<String>,
    ) -> Result<ConnectInfo, anyhow::Error> {
        match (key, secret) {
            (Some(key), Some(secret)) => Ok(ConnectInfo {
                region,
                credentials: Some(Credentials { key, secret, token }),
            }),
            (None, None) => Ok(ConnectInfo {
                region,
                credentials: None,
            }),
            (_, _) => {
                anyhow::bail!(
                    "Both aws_acccess_key_id and aws_secret_access_key \
                               must be provided, or neither"
                );
            }
        }
    }
}

/// Construct a new TLS-secured rusoto client
pub fn https_client() -> HttpClient {
    HttpClient::from_connector(HttpsConnector::new())
}

/// Fetches the AWS account number of the caller via AWS Security Token Service.
///
/// For details about STS, see AWS documentation, because the endpoint used by
/// this method is [always valid][], you can use this method to verify that
/// that AWS credentials provided are legitimate.
///
/// [always-valid]: https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html
pub async fn account(
    provider: impl ProvideAwsCredentials + Send + Sync + 'static,
    region: Region,
    timeout: Duration,
) -> Result<String, anyhow::Error> {
    let sts_client = StsClient::new_with(https_client(), provider, region);
    let get_identity = sts_client.get_caller_identity(GetCallerIdentityRequest {});
    let account = time::timeout(timeout, get_identity)
        .await
        .context("timeout while retrieving AWS account number from STS".to_owned())?
        .context("retrieving AWS account ID")?
        .account
        .ok_or_else(|| anyhow!("AWS did not return account ID"))?;
    Ok(account)
}

/// Verify that the provided credentials are legitimate
///
/// This uses an [always-valid][] API request to check that the AWS credentials
/// provided are legitimate.
///
/// [always-valid]: https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html
pub async fn validate_credentials(
    conn_info: ConnectInfo,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    if let Some(creds) = conn_info.credentials {
        let provider = StaticProvider::from(AwsCredentials::from(creds));
        crate::aws::account(provider.clone(), conn_info.region, timeout)
            .await
            .context("Using statically provided credentials")?;
    } else {
        let mut provider = ChainProvider::new();
        provider.set_timeout(Duration::from_secs(10));
        let provider =
            AutoRefreshingProvider::new(provider).context("generating AWS credentials")?;
        crate::aws::account(provider.clone(), conn_info.region, timeout)
            .await
            .context("Looking through the environment for credentials")?;
    }
    Ok(())
}
