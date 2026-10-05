// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use mz_repr::CatalogItemId;
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::string_or_secret::StringOrSecret;

/// AWS connection configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsConnection {
    pub auth: AwsAuth,
    /// The AWS region to use.
    ///
    /// Uses the default region (looking at env vars, config files, etc) if not
    /// provided.
    pub region: Option<String>,
    /// The custom AWS endpoint to use, if any.
    pub endpoint: Option<String>,
}

/// Describes how to authenticate with AWS.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum AwsAuth {
    /// Authenticate with an access key.
    Credentials(AwsCredentials),
    //// Authenticate via assuming an IAM role.
    AssumeRole(AwsAssumeRole),
}

/// AWS credentials to access an AWS account using user access keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsCredentials {
    /// The AWS API Access Key required to connect to the AWS account.
    pub access_key_id: StringOrSecret,
    /// The Secret Access Key required to connect to the AWS account.
    pub secret_access_key: CatalogItemId,
    /// Optional session token to connect to the AWS account.
    pub session_token: Option<StringOrSecret>,
}

/// Describes an AWS IAM role to assume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsAssumeRole {
    /// The Amazon Resource Name of the role to assume.
    pub arn: String,
    /// The optional session name for the session.
    pub session_name: Option<String>,
}

impl AwsConnection {
    /// Returns a string describing the authentication method of this connection, for use in error messages.
    pub fn auth_method(&self) -> &'static str {
        match self.auth {
            AwsAuth::Credentials(_) => "credentials",
            AwsAuth::AssumeRole(_) => "assume_role",
        }
    }

    pub fn validate_by_default(&self) -> bool {
        false
    }
}
