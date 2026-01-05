// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::{Region, SdkConfig};
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_sigv4::http_request::{
    self, SignableBody, SignableRequest, SignatureLocation, SigningError, SigningSettings,
};
use aws_sigv4::sign::v4::SigningParams;
use aws_sigv4::sign::v4::signing_params::BuildError;
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
}

// Generate an RDS authentication token.  This should mirror what can be found
// in aws_sdk_rds::auth_token, but without restricted creates.
pub(crate) async fn rds_auth_token(
    host: &str,
    port: u16,
    username: &str,
    aws_config: &SdkConfig,
) -> Result<String, RdsTokenError> {
    let credentials = aws_config
        .credentials_provider()
        .ok_or(RdsTokenError::MissingCredentialsProvider)?
        .provide_credentials()
        .await?;

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

    let url = format!(
        "https://{}:{}/?Action={}&DBUser={}",
        host, port, DB_ACTION, username
    );

    let signable_req =
        SignableRequest::new("GET", &url, std::iter::empty(), SignableBody::Bytes(&[]))?;

    let (signing_instructions, _sig) =
        http_request::sign(signable_req, &signing_params.into())?.into_parts();

    let mut url = url::Url::parse(&url).unwrap_or_else(|_| panic!("expect to parse {url}"));
    for (key, val) in signing_instructions.params() {
        url.query_pairs_mut().append_pair(key, val);
    }

    Ok(url.to_string().split_off("https://".len()))
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
    }
}
