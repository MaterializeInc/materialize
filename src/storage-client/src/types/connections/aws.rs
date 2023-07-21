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
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::types::connections::{ConnectionContext, StringOrSecret};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.connections.aws.rs"
));

/// AWS configuration overrides for a source or sink.
///
/// This is a distinct type from any of the configuration types built into the
/// AWS SDK so that we can implement `Serialize` and `Deserialize`.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsConfig {
    /// AWS Credentials, or where to find them
    pub credentials: AwsCredentials,
    /// The AWS region to use.
    ///
    /// Uses the default region (looking at env vars, config files, etc) if not provided.
    pub region: Option<String>,
    /// The AWS role to assume.
    pub role: Option<AwsAssumeRole>,
    /// The custom AWS endpoint to use, if any.
    pub endpoint: Option<String>,
}

impl RustType<ProtoAwsConfig> for AwsConfig {
    fn into_proto(&self) -> ProtoAwsConfig {
        ProtoAwsConfig {
            credentials: Some(self.credentials.into_proto()),
            region: self.region.clone(),
            role: self.role.into_proto(),
            endpoint: self.endpoint.clone(),
        }
    }

    fn from_proto(proto: ProtoAwsConfig) -> Result<Self, TryFromProtoError> {
        Ok(AwsConfig {
            credentials: proto
                .credentials
                .into_rust_if_some("ProtoAwsConfig::credentials")?,
            region: proto.region,
            role: proto.role.into_rust()?,
            endpoint: proto.endpoint,
        })
    }
}

/// AWS credentials for a source or sink.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsCredentials {
    pub access_key_id: StringOrSecret,
    pub secret_access_key: GlobalId,
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

/// A role for Materialize to assume when performing AWS API calls.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AwsAssumeRole {
    /// The Amazon Resource Name of the role to assume.
    pub arn: String,
}

impl RustType<ProtoAwsAssumeRole> for AwsAssumeRole {
    fn into_proto(&self) -> ProtoAwsAssumeRole {
        ProtoAwsAssumeRole {
            arn: self.arn.clone(),
        }
    }

    fn from_proto(proto: ProtoAwsAssumeRole) -> Result<Self, TryFromProtoError> {
        Ok(AwsAssumeRole { arn: proto.arn })
    }
}

impl AwsConfig {
    /// Loads the AWS SDK configuration object from the environment, then
    /// applies the overrides from this object.
    pub async fn load(
        &self,
        external_id_prefix: Option<&AwsExternalIdPrefix>,
        external_id_suffix: Option<&GlobalId>,
        secrets_reader: &dyn SecretsReader,
    ) -> aws_types::SdkConfig {
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

        let AwsCredentials {
            access_key_id,
            secret_access_key,
            session_token,
        } = &self.credentials;

        let mut cred_provider = SharedCredentialsProvider::new(Credentials::from_keys(
            access_key_id.get_string(secrets_reader).await.unwrap(),
            secrets_reader
                .read_string(*secret_access_key)
                .await
                .unwrap(),
            match session_token {
                Some(t) => Some(t.get_string(secrets_reader).await.unwrap()),
                None => None,
            },
        ));

        if let Some(AwsAssumeRole { arn }) = &self.role {
            let mut role = AssumeRoleProvider::builder(arn).session_name("materialize");
            // This affects which region to perform STS on, not where
            // anything else happens.
            if let Some(region) = &region {
                role = role.region(region.clone());
            }
            if let Some(external_id_prefix) = external_id_prefix {
                let external_id = if let Some(suffix) = external_id_suffix {
                    format!("{}-{}", external_id_prefix, suffix)
                } else {
                    external_id_prefix.to_string()
                };
                role = role.external_id(external_id);
            }
            cred_provider = SharedCredentialsProvider::new(role.build(cred_provider));
        }

        let mut loader = aws_config::from_env()
            .region(region)
            .credentials_provider(cred_provider);
        if let Some(endpoint) = &self.endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        loader.load().await
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn validate(
        &self,
        _id: GlobalId,
        _connection_context: &ConnectionContext,
    ) -> Result<(), anyhow::Error> {
        Err(anyhow!("Validating SSH connections is not supported yet"))
    }

    pub(crate) fn validate_by_default(&self) -> bool {
        false
    }
}
