// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions for AWS.

use rusoto_core::Region;
use rusoto_credential::{AwsCredentials, ChainProvider, ProvideAwsCredentials};
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};
use serde::{Deserialize, Serialize};
use tokio::time::error::Elapsed;
use tokio::time::{self, Duration};

/// Information required to connnect to AWS
///
/// Credentials are optional because in most cases users should use the
/// [`ChainProvider`] to pull information from the process or AWS environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectInfo {
    /// The AWS Region to connect to
    pub region: Region,
    /// Credentials, if missing will be obtained from environment
    pub(crate) credentials: Option<Credentials>,
}

/// A thin dupe of [`AwsCredentials`] so we can impl Serialize
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Credentials {
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

/// Fetches the AWS account number of the caller via AWS Security Token Service.
///
/// For details about STS, see AWS documentation.
pub async fn account(timeout: Duration) -> Result<String, anyhow::Error> {
    let sts_client = StsClient::new(Region::default());
    let get_identity = sts_client.get_caller_identity(GetCallerIdentityRequest {});
    let account = time::timeout(timeout, get_identity)
        .await
        .map_err(|e: Elapsed| {
            anyhow::Error::new(e)
                .context("timeout while retrieving AWS account number from STS".to_owned())
        })?
        .map_err(|e| anyhow::Error::new(e).context("retrieving AWS account ID".to_owned()))?
        .account
        .ok_or_else(|| anyhow::Error::msg("AWS did not return account ID".to_owned()))?;
    Ok(account)
}

/// Fetches AWS credentials by consulting several known sources.
///
/// For details about where AWS credentials can be stored, see Rusoto's
/// [`ChainProvider`] documentation.
pub async fn credentials(timeout: Duration) -> Result<AwsCredentials, anyhow::Error> {
    let mut provider = ChainProvider::new();
    provider.set_timeout(timeout);
    let credentials = provider.credentials().await?;
    Ok(credentials)
}
