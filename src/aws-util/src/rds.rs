// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS RDS IAM database authentication.
//!
//! The tokens generated here are not specific to a database engine: the same
//! token authenticates against RDS and Aurora for MySQL and PostgreSQL. The
//! caller passes the token as the password when it connects, over TLS.

use aws_config::{Region, SdkConfig};
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_sigv4::http_request::{
    self, SignableBody, SignableRequest, SignatureLocation, SigningError, SigningSettings,
};
use aws_sigv4::sign::v4::SigningParams;
use aws_sigv4::sign::v4::signing_params::BuildError;
use mz_ore::secure::SecureString;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const AWS_TOKEN_EXPIRATION_SECONDS: u64 = 900;
const AWS_SERVICE: &str = "rds-db";
const DB_ACTION: &str = "connect";

#[derive(Debug, thiserror::Error)]
pub enum RdsTokenError {
    /// The AWS SdkConfig did not contain a credentials provider.
    #[error("Credentials provider required to sign RDS IAM request")]
    MissingCredentialsProvider,

    /// The supplied credentials provider failed to generate credentials for URL signing.
    #[error(transparent)]
    CredentialsError(#[from] CredentialsError),

    /// The signing parameters could not be created due to a missing required argument.
    #[error(transparent)]
    SigningParametersBuildError(#[from] BuildError),

    /// The URL could not be signed.
    #[error(transparent)]
    SigningError(#[from] SigningError),

    /// The host could not be parsed as a valid URL.
    #[error("invalid RDS endpoint: {0}")]
    InvalidEndpoint(#[from] url::ParseError),
}

/// Generates an RDS IAM authentication token for `username` connecting to the
/// database at `host:port`.
///
/// `host` and `port` must be the endpoint of the RDS instance or cluster
/// itself, not of a tunnel or proxy in front of it, because the token is a
/// signature over that endpoint. The token is valid for 15 minutes.
///
/// Every call asks `aws_config`'s credentials provider for credentials, and
/// nothing here caches them. If that provider assumes a role, each token
/// costs a round of AWS STS requests unless the provider caches.
///
/// This mirrors `aws_sdk_rds::auth_token`, without depending on `aws-sdk-rds`.
pub async fn rds_auth_token(
    host: &str,
    port: u16,
    username: &str,
    aws_config: &SdkConfig,
) -> Result<String, RdsTokenError> {
    let token = sign_rds_auth_token(host, port, username, aws_config).await?;
    Ok(token.token.unsecure().to_owned())
}

/// An RDS IAM authentication token.
#[derive(Debug, Clone)]
pub struct RdsAuthToken {
    /// The token, to use as the database password. It is a credential for as
    /// long as it is valid, so it is redacted in `Debug` output.
    pub token: SecureString,
    /// The access key ID of the credentials that signed the token.
    pub access_key_id: String,
}

/// Authenticates database connections to Amazon RDS with AWS IAM.
#[derive(Clone)]
pub struct RdsIamAuth {
    aws_config: SdkConfig,
    on_token_rejected: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl fmt::Debug for RdsIamAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RdsIamAuth")
            .field("aws_config", &self.aws_config)
            .field("on_token_rejected", &self.on_token_rejected.is_some())
            .finish()
    }
}

impl RdsIamAuth {
    /// Signs tokens with the credentials provider of `aws_config`.
    pub fn new(aws_config: SdkConfig) -> RdsIamAuth {
        RdsIamAuth {
            aws_config,
            on_token_rejected: None,
        }
    }

    /// Calls `hook` with the access key ID of the signing credentials whenever
    /// [`RdsIamAuth::token_rejected`] reports a rejected token.
    ///
    /// A provider that caches credentials uses this to drop credentials that
    /// AWS no longer honors, such as those of a revoked role session.
    pub fn on_token_rejected(mut self, hook: impl Fn(&str) + Send + Sync + 'static) -> RdsIamAuth {
        self.on_token_rejected = Some(Arc::new(hook));
        self
    }

    /// Like [`rds_auth_token`].
    pub async fn token(
        &self,
        host: &str,
        port: u16,
        username: &str,
    ) -> Result<RdsAuthToken, RdsTokenError> {
        sign_rds_auth_token(host, port, username, &self.aws_config).await
    }

    /// Reports that the database rejected `token` as an authentication
    /// failure.
    pub fn token_rejected(&self, token: &RdsAuthToken) {
        if let Some(hook) = &self.on_token_rejected {
            hook(&token.access_key_id);
        }
    }
}

async fn sign_rds_auth_token(
    host: &str,
    port: u16,
    username: &str,
    aws_config: &SdkConfig,
) -> Result<RdsAuthToken, RdsTokenError> {
    let credentials = aws_config
        .credentials_provider()
        .ok_or(RdsTokenError::MissingCredentialsProvider)?
        .provide_credentials()
        .await?;
    let access_key_id = credentials.access_key_id().to_owned();

    // mirror existing SDK behaviour: default to us-east-1,
    let region = aws_config
        .region()
        .cloned()
        .unwrap_or_else(|| Region::new("us-east-1"));

    // defaults to SystemTime
    let time_source = aws_config.time_source().unwrap_or_default();

    let mut signing_settings = SigningSettings::default();
    signing_settings.expires_in = Some(Duration::from_secs(AWS_TOKEN_EXPIRATION_SECONDS));
    signing_settings.signature_location = SignatureLocation::QueryParams;

    let identity = credentials.into();
    let signing_params = SigningParams::builder()
        .identity(&identity)
        .region(region.as_ref())
        .name(AWS_SERVICE)
        .time(time_source.now())
        .settings(signing_settings)
        .build()?;

    let mut url = url::Url::parse(&format!("https://{}:{}/", host, port))?;
    url.query_pairs_mut()
        .append_pair("Action", DB_ACTION)
        .append_pair("DBUser", username);

    let url_str = url.as_str().to_owned();
    let signable_req = SignableRequest::new(
        "GET",
        &url_str,
        std::iter::empty(),
        SignableBody::Bytes(&[]),
    )?;

    let (signing_instructions, _sig) =
        http_request::sign(signable_req, &signing_params.into())?.into_parts();

    for (key, val) in signing_instructions.params() {
        url.query_pairs_mut().append_pair(key, val);
    }

    Ok(RdsAuthToken {
        token: SecureString::from(&url.as_str()["https://".len()..]),
        access_key_id,
    })
}

#[cfg(test)]
mod test {
    use aws_credential_types::Credentials;
    use aws_types::sdk_config::{SharedCredentialsProvider, TimeSource};
    use std::time::SystemTime;

    use super::*;
    #[derive(Debug)]
    struct TestTimeSource {
        time: SystemTime,
    }
    impl TimeSource for TestTimeSource {
        fn now(&self) -> SystemTime {
            self.time.clone()
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `getauxval` on OS `linux`
    async fn test_signature() {
        let time_source = TestTimeSource {
            time: SystemTime::UNIX_EPOCH + Duration::from_secs(1740690000),
        };
        let aws_config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "drjekyll", "mrhyde", None, None, "test",
            )))
            .time_source(time_source)
            .build();
        let signature = rds_auth_token("mysql", 3306, "root", &aws_config)
            .await
            .unwrap();
        assert_eq!(
            &signature,
            "mysql:3306/?Action=connect&DBUser=root&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=drjekyll%2F20250227%2Fus-east-1%2Frds-db%2Faws4_request&X-Amz-Date=20250227T210000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=6eb04929394feeb9e070621ecafc731145914cc53e542d5302c251b705c4ac72"
        );

        // Usernames with URL-special characters must be percent-encoded in the
        // token, not interpolated raw into the query string.
        for (username, reject_key) in [("root&Foo=bar", Some("Foo")), ("root#admin", None)] {
            let token = rds_auth_token("mysql", 3306, username, &aws_config)
                .await
                .unwrap();
            let parsed = url::Url::parse(&format!("https://{token}")).unwrap();
            let pairs: std::collections::BTreeMap<_, _> = parsed.query_pairs().collect();
            assert_eq!(pairs.get("DBUser").unwrap(), username);
            if let Some(key) = reject_key {
                assert!(!pairs.contains_key(key));
            }
        }

        assert!(rds_auth_token("", 3306, "root", &aws_config).await.is_err());
    }
}
