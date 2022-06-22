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
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_repr::proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::url::URL_PATTERN;
use mz_repr::GlobalId;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.types.connections.aws.rs"
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
/// [`ConnectionContext::from_cli_args`]: crate::connections::ConnectionContext::from_cli_args
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
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AwsCredentials {
    /// Look for credentials using the [default credentials chain][credchain]
    ///
    /// [credchain]: aws_config::default_provider::credentials::DefaultCredentialsChain
    Default,
    /// Load credentials using the given named profile
    Profile { profile_name: String },
    /// Use the enclosed static credentials
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

impl RustType<ProtoAwsCredentials> for AwsCredentials {
    fn into_proto(&self) -> ProtoAwsCredentials {
        use proto_aws_credentials::{Kind, ProtoStatic};
        ProtoAwsCredentials {
            kind: Some(match self {
                AwsCredentials::Default => Kind::Default(()),
                AwsCredentials::Profile { profile_name } => Kind::Profile(profile_name.clone()),
                AwsCredentials::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => Kind::Static(ProtoStatic {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    session_token: session_token.clone(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoAwsCredentials) -> Result<Self, TryFromProtoError> {
        use proto_aws_credentials::{Kind, ProtoStatic};
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoAwsCredentials::kind"))?;
        Ok(match kind {
            Kind::Default(()) => AwsCredentials::Default,
            Kind::Profile(profile_name) => AwsCredentials::Profile { profile_name },
            Kind::Static(ProtoStatic {
                access_key_id,
                secret_access_key,
                session_token,
            }) => AwsCredentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            },
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
    ) -> aws_types::SdkConfig {
        use aws_config::default_provider::credentials::DefaultCredentialsChain;
        use aws_config::default_provider::region::DefaultRegionChain;
        use aws_config::sts::AssumeRoleProvider;
        use aws_smithy_http::endpoint::Endpoint;
        use aws_types::credentials::SharedCredentialsProvider;
        use aws_types::region::Region;

        let region = match &self.region {
            Some(region) => Some(Region::new(region.clone())),
            _ => {
                let mut rc = DefaultRegionChain::builder();
                if let AwsCredentials::Profile { profile_name } = &self.credentials {
                    rc = rc.profile_name(profile_name);
                }
                rc.build().region().await
            }
        };

        let mut cred_provider = match &self.credentials {
            AwsCredentials::Default => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .region(region.clone())
                    .build()
                    .await,
            ),
            AwsCredentials::Profile { profile_name } => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .profile_name(profile_name)
                    .region(region.clone())
                    .build()
                    .await,
            ),
            AwsCredentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
                access_key_id,
                secret_access_key,
                session_token.clone(),
            )),
        };

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
