// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility mod for AWS.

use std::time::Duration;

use rusoto_core::Region;
use rusoto_credential::{AwsCredentials, ChainProvider, ProvideAwsCredentials};
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

/// Fetches the AWS account number of the caller via AWS Security Token Service.
///
/// For details about STS, see AWS documentation.
pub async fn account(timeout: Duration) -> Result<String, anyhow::Error> {
    let sts_client = StsClient::new(Region::default());
    let get_identity = sts_client.get_caller_identity(GetCallerIdentityRequest {});
    let account = tokio::time::timeout(timeout, get_identity)
        .await
        .map_err(|e: tokio::time::Elapsed| {
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
