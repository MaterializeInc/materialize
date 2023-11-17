// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use anyhow::anyhow;
use aws_types::SdkConfig;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::{configuration::StorageConfiguration, connections::StringOrSecret};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.connections.aws.rs"
));
/// The materialize principal details
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct MaterializePrincipal {
    /// The role which Materialize will assume first to eventually
    /// assume the customer's role via role chaining.
    pub arn: String,
    /// The external ID prefix, will need the connection ID as a suffix
    /// to get the complete External ID to assume customer's role.
    pub external_id_prefix: String,
}

impl MaterializePrincipal {
    pub fn get_external_id(&self, aws_external_id_suffix: String) -> String {
        format!("mz_{}_{}", self.external_id_prefix, aws_external_id_suffix)
    }

    pub fn get_aws_example_trust_policy(
        &self,
        aws_external_id_suffix: String,
    ) -> serde_json::Value {
        serde_json::json!(
            {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Principal": {
                  "AWS": self.arn
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                  "StringEquals": {
                    "sts:ExternalId": self.get_external_id(aws_external_id_suffix)
                  }
                }
              }
            ]
          }
        )
    }
}

impl RustType<ProtoMaterializePrincipal> for MaterializePrincipal {
    fn into_proto(&self) -> ProtoMaterializePrincipal {
        ProtoMaterializePrincipal {
            arn: self.arn.clone(),
            external_id_prefix: self.external_id_prefix.clone(),
        }
    }

    fn from_proto(proto: ProtoMaterializePrincipal) -> Result<Self, TryFromProtoError> {
        Ok(MaterializePrincipal {
            arn: proto.arn,
            external_id_prefix: proto.external_id_prefix,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum AwsAuth {
    // Details for using AWS user's API keys
    Credentials(AwsCredentials),
    /// Details for the AWS role to assume.
    AssumeRole(AwsAssumeRole),
}

/// AWS configuration overrides for a source or sink.
///
/// This is a distinct type from any of the configuration types built into the
/// AWS SDK so that we can implement `Serialize` and `Deserialize`.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsConfig {
    pub auth: AwsAuth,
    /// The AWS region to use.
    ///
    /// Uses the default region (looking at env vars, config files, etc) if not provided.
    pub region: Option<String>,

    /// The custom AWS endpoint to use, if any.
    pub endpoint: Option<String>,
}

impl RustType<ProtoAwsConfig> for AwsConfig {
    fn into_proto(&self) -> ProtoAwsConfig {
        let auth = match &self.auth {
            AwsAuth::Credentials(credentials) => {
                proto_aws_config::Auth::Credentials(credentials.into_proto())
            }
            AwsAuth::AssumeRole(assume_role) => {
                proto_aws_config::Auth::AssumeRole(assume_role.into_proto())
            }
        };

        ProtoAwsConfig {
            auth: Some(auth),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        }
    }

    fn from_proto(proto: ProtoAwsConfig) -> Result<Self, TryFromProtoError> {
        let auth = match proto.auth.expect("auth expected") {
            proto_aws_config::Auth::Credentials(credentials) => {
                AwsAuth::Credentials(credentials.into_rust()?)
            }
            proto_aws_config::Auth::AssumeRole(assume_role) => {
                AwsAuth::AssumeRole(assume_role.into_rust()?)
            }
        };

        Ok(AwsConfig {
            auth,
            region: proto.region,
            endpoint: proto.endpoint,
        })
    }
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

/// Details required for Materialize to assume customer's role
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsAssumeRole {
    /// The Amazon Resource Name of the role to assume.
    pub arn: String,
    /// The optional session name for the assume role session.
    pub session_name: Option<String>,
    /// The Materialize AWS principal details.
    pub mz_principal: MaterializePrincipal,
}

impl RustType<ProtoAwsAssumeRole> for AwsAssumeRole {
    fn into_proto(&self) -> ProtoAwsAssumeRole {
        ProtoAwsAssumeRole {
            arn: self.arn.clone(),
            session_name: self.session_name.clone(),
            mz_principal: Some(self.mz_principal.into_proto()),
        }
    }

    fn from_proto(proto: ProtoAwsAssumeRole) -> Result<Self, TryFromProtoError> {
        Ok(AwsAssumeRole {
            arn: proto.arn,
            session_name: proto.session_name,
            mz_principal: proto
                .mz_principal
                .into_rust_if_some("ProtoAwsAssumeRole::mz_principal")?,
        })
    }
}

impl AwsConfig {
    /// Loads the AWS SDK configuration object from the environment, then
    /// applies the overrides from this object.
    pub async fn get_sdk_config(
        &self,
        secrets_reader: &dyn SecretsReader,
        external_id_suffix: Option<String>,
    ) -> Result<SdkConfig, anyhow::Error> {
        use aws_config::default_provider::region::DefaultRegionChain;
        use aws_config::sts::AssumeRoleProvider;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_types::region::Region;

        let region = match &self.region {
            Some(region) => Some(Region::new(region.clone())),
            _ => {
                let rc = DefaultRegionChain::builder();
                rc.build().region().await
            }
        };

        let cred_provider = match &self.auth {
            AwsAuth::Credentials(credentials) => {
                let AwsCredentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } = credentials;

                SharedCredentialsProvider::new(Credentials::from_keys(
                    access_key_id.get_string(secrets_reader).await.unwrap(),
                    secrets_reader
                        .read_string(*secret_access_key)
                        .await
                        .unwrap(),
                    match session_token {
                        Some(t) => Some(t.get_string(secrets_reader).await.unwrap()),
                        None => None,
                    },
                ))
            }
            AwsAuth::AssumeRole(assume_role) => {
                let sts_client = aws_sdk_sts::Client::new(&aws_config::from_env().load().await);
                let AwsAssumeRole {
                    arn,
                    session_name,
                    mz_principal,
                } = assume_role;
                let assume_role_response = sts_client
                    .assume_role()
                    .role_arn(mz_principal.arn.clone())
                    .role_session_name("materialize_assume_role")
                    .send()
                    .await?;

                let temp_credentials = assume_role_response.credentials().ok_or_else(|| {
                    anyhow!("Could not fetch temporary credentials with materialize role")
                })?;

                let access_key = temp_credentials
                    .access_key_id()
                    .ok_or_else(|| anyhow!("missing access_key"))?;
                let secret_key = temp_credentials
                    .secret_access_key()
                    .ok_or_else(|| anyhow!("missing secret key"))?;

                let credentials = aws_credential_types::Credentials::from_keys(
                    access_key,
                    secret_key,
                    temp_credentials.session_token().map(|t| t.to_string()),
                );

                let session_name = session_name.to_owned().unwrap_or("materialize".to_string());
                let mut role = AssumeRoleProvider::builder(arn).session_name(session_name);

                if let Some(region) = &region {
                    role = role.region(region.clone());
                }

                if let Some(external_id_suffix) = external_id_suffix {
                    role = role.external_id(mz_principal.get_external_id(external_id_suffix));
                }

                SharedCredentialsProvider::new(role.build(credentials))
            }
        };

        let mut loader = aws_config::from_env()
            .region(region)
            .credentials_provider(cred_provider);
        if let Some(endpoint) = &self.endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        Ok(loader.load().await)
    }

    pub(crate) async fn validate(
        &self,
        id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        match &self.auth {
            AwsAuth::Credentials(_) => {
                let aws_config = self
                    .get_sdk_config(
                        storage_configuration
                            .connection_context
                            .secrets_reader
                            .as_ref(),
                        None,
                    )
                    .await?;

                let sts_client = aws_sdk_sts::Client::new(&aws_config);
                let _ = sts_client.get_caller_identity().send().await?;
                Ok(())
            }
            AwsAuth::AssumeRole(_) => {
                let aws_config = self
                    .get_sdk_config(
                        storage_configuration
                            .connection_context
                            .secrets_reader
                            .as_ref(),
                        Some(id.to_string()),
                    )
                    .await?;

                let sts_client = aws_sdk_sts::Client::new(&aws_config);
                let _ = sts_client.get_caller_identity().send().await?;

                let aws_config_without_external_id = self
                    .get_sdk_config(
                        storage_configuration
                            .connection_context
                            .secrets_reader
                            .as_ref(),
                        None,
                    )
                    .await?;

                let sts_client = aws_sdk_sts::Client::new(&aws_config_without_external_id);
                if sts_client.get_caller_identity().send().await.is_ok() {
                    Err(anyhow!("Validate succeeded without external_id!"))
                } else {
                    Ok(())
                }
            }
        }
    }

    pub(crate) fn validate_by_default(&self) -> bool {
        false
    }
}
