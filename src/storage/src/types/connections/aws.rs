// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use http::Uri;
use mz_secrets::SecretsReader;
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::url::URL_PATTERN;
use mz_repr::GlobalId;

use super::StringOrSecret;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage.types.connections.aws.rs"
));

/// A wrapper for [`Uri`] that implements [`Serialize`] and `Deserialize`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SerdeUri(#[serde(with = "http_serde::uri")] pub Uri);

/// Generate a random `SerdeUri` based on an arbitrary URL
/// It doesn't cover the full spectrum of valid URIs, but just a wide enough sample
/// to test our Protobuf roundtripping logic.
fn any_serde_uri() -> impl Strategy<Value = SerdeUri> {
    URL_PATTERN.prop_map(|s| SerdeUri(s.parse().unwrap()))
}

impl Arbitrary for SerdeUri {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_serde_uri().boxed()
    }
}

impl RustType<ProtoSerdeUri> for SerdeUri {
    fn into_proto(&self) -> ProtoSerdeUri {
        ProtoSerdeUri {
            uri: self.0.to_string(),
        }
    }

    fn from_proto(proto: ProtoSerdeUri) -> Result<Self, TryFromProtoError> {
        Ok(SerdeUri(proto.uri.parse()?))
    }
}

/// A prefix for an [external ID] to use for all AWS AssumeRole operations.
/// It should be concatenanted with a non-user-provided suffix identifying the source or sink.
/// The ID used for the suffix should never be reused if the source or sink is deleted.
///
/// **WARNING:** it is critical for security that this ID is **not**
/// provided by end users of Materialize. It must be provided by the
/// operator of the Materialize service.
///
/// This type protects against accidental construction of an
/// `AwsExternalIdPrefix`. The only approved way to construct an `AwsExternalIdPrefix`
/// is via [`ConnectionContext::from_cli_args`].
///
/// [`ConnectionContext::from_cli_args`]: crate::types::connections::ConnectionContext::from_cli_args
/// [external ID]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsExternalIdPrefix(pub(super) String);

/// AWS configuration overrides for a source or sink.
///
/// This is a distinct type from any of the configuration types built into the
/// AWS SDK so that we can implement `Serialize` and `Deserialize`.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    pub endpoint: Option<SerdeUri>,
}

impl RustType<ProtoAwsConfig> for AwsConfig {
    fn into_proto(&self) -> ProtoAwsConfig {
        ProtoAwsConfig {
            credentials: Some((&self.credentials).into_proto()),
            region: self.region.clone(),
            role: self.role.into_proto(),
            endpoint: self.endpoint.into_proto(),
        }
    }

    fn from_proto(proto: ProtoAwsConfig) -> Result<Self, TryFromProtoError> {
        Ok(AwsConfig {
            credentials: proto
                .credentials
                .into_rust_if_some("ProtoAwsConfig::credentials")?,
            region: proto.region,
            role: proto.role.into_rust()?,
            endpoint: proto.endpoint.into_rust()?,
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
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
        use aws_smithy_http::endpoint::Endpoint;
        use aws_types::credentials::SharedCredentialsProvider;
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

        let mut cred_provider = SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
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
                    format!("{}-{}", external_id_prefix.0, suffix)
                } else {
                    external_id_prefix.0.to_owned()
                };
                role = role.external_id(external_id);
            }
            cred_provider = SharedCredentialsProvider::new(role.build(cred_provider));
        }

        let mut loader = aws_config::from_env()
            .region(region)
            .credentials_provider(cred_provider);
        if let Some(endpoint) = &self.endpoint {
            loader = loader.endpoint_resolver(Endpoint::immutable(endpoint.0.clone()));
        }
        loader.load().await
    }
}
