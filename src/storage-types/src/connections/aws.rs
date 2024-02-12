// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use anyhow::{anyhow, bail};
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use aws_sdk_sts::error::SdkError;
use aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError;
use aws_types::region::Region;
use aws_types::SdkConfig;
use mz_ore::error::ErrorExt;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::GlobalId;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    configuration::StorageConfiguration,
    connections::{ConnectionContext, StringOrSecret},
};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.connections.aws.rs"
));

/// AWS connection configuration.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

impl RustType<ProtoAwsConnection> for AwsConnection {
    fn into_proto(&self) -> ProtoAwsConnection {
        let auth = match &self.auth {
            AwsAuth::Credentials(credentials) => {
                proto_aws_connection::Auth::Credentials(credentials.into_proto())
            }
            AwsAuth::AssumeRole(assume_role) => {
                proto_aws_connection::Auth::AssumeRole(assume_role.into_proto())
            }
        };

        ProtoAwsConnection {
            auth: Some(auth),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        }
    }

    fn from_proto(proto: ProtoAwsConnection) -> Result<Self, TryFromProtoError> {
        let auth = match proto.auth.expect("auth expected") {
            proto_aws_connection::Auth::Credentials(credentials) => {
                AwsAuth::Credentials(credentials.into_rust()?)
            }
            proto_aws_connection::Auth::AssumeRole(assume_role) => {
                AwsAuth::AssumeRole(assume_role.into_rust()?)
            }
        };

        Ok(AwsConnection {
            auth,
            region: proto.region,
            endpoint: proto.endpoint,
        })
    }
}

/// Describes how to authenticate with AWS.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum AwsAuth {
    /// Authenticate with an access key.
    Credentials(AwsCredentials),
    //// Authenticate via assuming an IAM role.
    AssumeRole(AwsAssumeRole),
}

/// AWS credentials to access an AWS account using user access keys.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsCredentials {
    /// The AWS API Access Key required to connect to the AWS account.
    pub access_key_id: StringOrSecret,
    /// The Secret Access Key required to connect to the AWS account.
    pub secret_access_key: GlobalId,
    /// Optional session token to connect to the AWS account.
    pub session_token: Option<StringOrSecret>,
}

impl AwsCredentials {
    /// Loads a credentials provider with the configured credentials.
    async fn load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
    ) -> Result<impl ProvideCredentials, anyhow::Error> {
        let secrets_reader = connection_context.secrets_reader.as_ref();
        Ok(Credentials::from_keys(
            self.access_key_id
                .get_string(secrets_reader)
                .await
                .map_err(|_| {
                    anyhow!("internal error: failed to read access key ID from secret store")
                })?,
            connection_context
                .secrets_reader
                .read_string(self.secret_access_key)
                .await
                .map_err(|_| {
                    anyhow!("internal error: failed to read secret access key from secret store")
                })?,
            match &self.session_token {
                Some(t) => {
                    let t = t.get_string(secrets_reader).await.map_err(|_| {
                        anyhow!("internal error: failed to read session token from secret store")
                    })?;
                    Some(t)
                }
                None => None,
            },
        ))
    }
}

impl RustType<ProtoAwsCredentials> for AwsCredentials {
    fn into_proto(&self) -> ProtoAwsCredentials {
        ProtoAwsCredentials {
            access_key_id: Some(self.access_key_id.into_proto()),
            secret_access_key: Some(self.secret_access_key.into_proto()),
            session_token: self.session_token.into_proto(),
        }
    }

    fn from_proto(proto: ProtoAwsCredentials) -> Result<Self, TryFromProtoError> {
        Ok(AwsCredentials {
            access_key_id: proto
                .access_key_id
                .into_rust_if_some("ProtoAwsCredentials::access_key_id")?,
            secret_access_key: proto
                .secret_access_key
                .into_rust_if_some("ProtoAwsCredentials::secret_access_key")?,
            session_token: proto.session_token.into_rust()?,
        })
    }
}

/// Describes an AWS IAM role to assume.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsAssumeRole {
    /// The Amazon Resource Name of the role to assume.
    pub arn: String,
    /// The optional session name for the session.
    pub session_name: Option<String>,
}

impl AwsAssumeRole {
    /// Loads a credentials provider that will assume the specified role
    /// with the appropriate external ID.
    async fn load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: GlobalId,
    ) -> Result<impl ProvideCredentials, anyhow::Error> {
        let external_id = self.external_id(connection_context, connection_id)?;
        // It's okay to use `dangerously_load_credentials_provider` here, as
        // this is the method that provides a safe wrapper by forcing use of the
        // correct external ID.
        self.dangerously_load_credentials_provider(
            connection_context,
            connection_id,
            Some(external_id),
        )
        .await
    }

    /// DANGEROUS: only for internal use!
    ///
    /// Like `load_credentials_provider`, but accepts an arbitrary external ID.
    /// Only for use in the internal implementation of AWS connections. Using
    /// this method incorrectly can result in violating our AWS security
    /// requirements.
    async fn dangerously_load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: GlobalId,
        external_id: Option<String>,
    ) -> Result<impl ProvideCredentials, anyhow::Error> {
        let Some(aws_connection_role_arn) = &connection_context.aws_connection_role_arn else {
            bail!("internal error: no AWS connection role configured");
        };

        // Load the default SDK configuration to use for the assume role
        // operations themselves.
        let assume_role_sdk_config = mz_aws_util::defaults().load().await;

        // The default session name identifies the environment and the
        // connection.
        let default_session_name =
            format!("{}-{}", &connection_context.environment_id, connection_id);

        // First we create a credentials provider that will assume the "jump
        // role" provided to this Materialize environment. This is the role that
        // we've told the end user to allow in their role trust policy. No need
        // to specify the external ID here as we're still within the Materialize
        // sphere of trust. The ambient AWS credentials provided to this
        // environment will be provided via the default credentials change and
        // allow us to assume the jump role. We always use the default session
        // name here, so that we can identify the specific environment and
        // connection ID that initiated the session in our internal CloudTrail
        // logs. This session isn't visible to the end user.
        let jump_credentials = AssumeRoleProvider::builder(aws_connection_role_arn)
            .configure(&assume_role_sdk_config)
            .session_name(default_session_name.clone())
            .build()
            .await;

        // Then we create the provider that will assume the end user's role.
        // Here, we *must* install the external ID, as we're using the jump role
        // to hop into the end user's AWS account, and the external ID is the
        // only thing that allows them to limit their trust of the jump role to
        // this specific Materialize environment and AWS connection. We also
        // respect the user's configured session name, if any, as this is the
        // session that will be visible to them.
        let mut credentials = AssumeRoleProvider::builder(&self.arn)
            .configure(&assume_role_sdk_config)
            .session_name(self.session_name.clone().unwrap_or(default_session_name));
        if let Some(external_id) = external_id {
            credentials = credentials.external_id(external_id);
        }
        Ok(credentials.build_from_provider(jump_credentials).await)
    }

    pub fn external_id(
        &self,
        connection_context: &ConnectionContext,
        connection_id: GlobalId,
    ) -> Result<String, anyhow::Error> {
        let Some(aws_external_id_prefix) = &connection_context.aws_external_id_prefix else {
            bail!("internal error: no AWS external ID prefix configured");
        };
        Ok(format!("mz_{}_{}", aws_external_id_prefix, connection_id))
    }

    pub fn example_trust_policy(
        &self,
        connection_context: &ConnectionContext,
        connection_id: GlobalId,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let Some(aws_connection_role_arn) = &connection_context.aws_connection_role_arn else {
            bail!("internal error: no AWS connection role configured");
        };
        Ok(json!(
            {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Principal": {
                  "AWS": aws_connection_role_arn
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                  "StringEquals": {
                    "sts:ExternalId": self.external_id(connection_context, connection_id)?
                  }
                }
              }
            ]
          }
        ))
    }
}

impl RustType<ProtoAwsAssumeRole> for AwsAssumeRole {
    fn into_proto(&self) -> ProtoAwsAssumeRole {
        ProtoAwsAssumeRole {
            arn: self.arn.clone(),
            session_name: self.session_name.clone(),
        }
    }

    fn from_proto(proto: ProtoAwsAssumeRole) -> Result<Self, TryFromProtoError> {
        Ok(AwsAssumeRole {
            arn: proto.arn,
            session_name: proto.session_name,
        })
    }
}

impl AwsConnection {
    /// Loads the AWS SDK configuration with the configuration specified on this
    /// object.
    pub async fn load_sdk_config(
        &self,
        connection_context: &ConnectionContext,
        connection_id: GlobalId,
    ) -> Result<SdkConfig, anyhow::Error> {
        let credentials = match &self.auth {
            AwsAuth::Credentials(credentials) => SharedCredentialsProvider::new(
                credentials
                    .load_credentials_provider(connection_context)
                    .await?,
            ),
            AwsAuth::AssumeRole(assume_role) => SharedCredentialsProvider::new(
                assume_role
                    .load_credentials_provider(connection_context, connection_id)
                    .await?,
            ),
        };
        self.load_sdk_config_from_credentials(credentials).await
    }

    async fn load_sdk_config_from_credentials(
        &self,
        credentials: impl ProvideCredentials + 'static,
    ) -> Result<SdkConfig, anyhow::Error> {
        let mut loader = mz_aws_util::defaults().credentials_provider(credentials);
        if let Some(region) = &self.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        Ok(loader.load().await)
    }

    pub(crate) async fn validate(
        &self,
        id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), AwsConnectionValidationError> {
        let aws_config = self
            .load_sdk_config(&storage_configuration.connection_context, id)
            .await?;
        let sts_client = aws_sdk_sts::Client::new(&aws_config);
        let _ = sts_client.get_caller_identity().send().await?;

        if let AwsAuth::AssumeRole(assume_role) = &self.auth {
            // Per AWS's recommendation, when validating a connection using
            // `AssumeRole` authentication, we should ensure that the
            // role rejects `AssumeRole` requests that don't specify an
            // external ID.
            let external_id = None;
            let credentials = assume_role
                .dangerously_load_credentials_provider(
                    &storage_configuration.connection_context,
                    id,
                    external_id,
                )
                .await?;
            let aws_config = self.load_sdk_config_from_credentials(credentials).await?;
            let sts_client = aws_sdk_sts::Client::new(&aws_config);
            if sts_client.get_caller_identity().send().await.is_ok() {
                return Err(AwsConnectionValidationError::RoleDoesNotRequireExternalId {
                    role_arn: assume_role.arn.clone(),
                });
            }
        }

        Ok(())
    }

    pub(crate) fn validate_by_default(&self) -> bool {
        false
    }
}

/// An error returned by `AwsConnection::validate`.
#[derive(thiserror::Error, Debug)]
pub enum AwsConnectionValidationError {
    #[error("role trust policy does not require an external ID")]
    RoleDoesNotRequireExternalId { role_arn: String },
    #[error("{}", .0.display_with_causes())]
    StsGetCallerIdentityError(#[from] SdkError<GetCallerIdentityError>),
    #[error("{}", .0.display_with_causes())]
    Other(#[from] anyhow::Error),
}

impl AwsConnectionValidationError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            AwsConnectionValidationError::RoleDoesNotRequireExternalId {
                role_arn
            } => Some(format!("The trust policy for the connection's role ({role_arn}) is insecure and allows any Materialize customer to assume the role.")),
            _ => None
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AwsConnectionValidationError::RoleDoesNotRequireExternalId { .. } => {
                Some("See: https://materialize.com/s/aws-connection-role-trust-policy".into())
            }
            _ => None,
        }
    }
}
