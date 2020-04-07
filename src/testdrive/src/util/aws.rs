// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use rusoto_core::Region;
use rusoto_credential::{AwsCredentials, ChainProvider, ProvideAwsCredentials};
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

use crate::error::Error;

/// Fetches AWS credentials and the corresponding account number by consulting
/// several known sources.
///
/// For details about where AWS credentials can be stored, see Rusoto's
/// [`ChainProvider`] documentation.
pub async fn account_details(timeout: Duration) -> Result<(String, AwsCredentials), Error> {
    let mut provider = ChainProvider::new();
    provider.set_timeout(timeout);
    let credentials = provider.credentials().await.map_err(|e| Error::General {
        ctx: "retrieving AWS credentials".into(),
        cause: Some(Box::new(e)),
        hints: vec![],
    })?;
    let sts_client = StsClient::new(Region::default());
    let get_identity = sts_client.get_caller_identity(GetCallerIdentityRequest {});
    let account = tokio::time::timeout(timeout, get_identity)
        .await
        .map_err(|_: tokio::time::Elapsed| Error::General {
            ctx: "timeout while retrieving AWS account number from STS".into(),
            cause: None,
            hints: vec![],
        })?
        .map_err(|e| Error::General {
            ctx: "retrieving AWS account ID".into(),
            cause: Some(Box::new(e)),
            hints: vec![],
        })?
        .account
        .ok_or_else(|| Error::General {
            ctx: "AWS did not return account ID".into(),
            cause: None,
            hints: vec![],
        })?;
    Ok((account, credentials))
}
