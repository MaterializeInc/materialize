//! AWS Secrets Manager secret provider.
//!
//! Resolves secret values by reading from AWS Secrets Manager. Requires
//! `aws_profile` to be set in `project.toml`.

use super::{SecretProvider, SecretResolveError};
use async_trait::async_trait;
use aws_sdk_secretsmanager::Client;
use tokio::sync::OnceCell;

/// Function name shared by both the real and unconfigured providers.
const PROVIDER_NAME: &str = "aws_secret";

/// Resolves secrets from AWS Secrets Manager.
///
/// Usage in SQL: `CREATE SECRET x AS aws_secret('my-secret-name')`
///
/// The AWS SDK config (including credential resolution) is loaded lazily
/// on the first `resolve()` call, so projects that set `aws_profile` but
/// never use `aws_secret()` pay no startup cost.
pub struct AwsSecretProvider {
    profile: String,
    client: OnceCell<Client>,
}

impl AwsSecretProvider {
    pub fn new(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let config = mz_aws_util::defaults()
                    .profile_name(&self.profile)
                    .load()
                    .await;
                Client::new(&config)
            })
            .await
    }
}

#[async_trait]
impl SecretProvider for AwsSecretProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn expected_args(&self) -> usize {
        1
    }

    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError> {
        let secret_name = &args[0];
        let client = self.client().await;
        let result = client
            .get_secret_value()
            .secret_id(secret_name)
            .send()
            .await
            .map_err(|e| SecretResolveError::ResolutionFailed {
                name: self.name().to_string(),
                reason: format!("failed to fetch secret '{}': {}", secret_name, e),
            })?;

        result
            .secret_string()
            .map(|s| s.to_string())
            .ok_or_else(|| SecretResolveError::ResolutionFailed {
                name: self.name().to_string(),
                reason: format!(
                    "secret '{}' is a binary secret; only text secrets are supported",
                    secret_name
                ),
            })
    }
}

/// Placeholder provider registered when `aws_profile` is not set in `project.toml`.
///
/// Always returns an error directing the user to configure `aws_profile`.
pub struct UnconfiguredAwsProvider;

#[async_trait]
impl SecretProvider for UnconfiguredAwsProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn expected_args(&self) -> usize {
        1
    }

    async fn resolve(&self, _args: &[String]) -> Result<String, SecretResolveError> {
        Err(SecretResolveError::ResolutionFailed {
            name: self.name().to_string(),
            reason: "AWS Secrets Manager is not configured. Set 'aws_profile' in project.toml to enable aws_secret().".to_string(),
        })
    }
}
