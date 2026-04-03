//! Environment variable secret provider.
//!
//! Resolves secret values by reading from process environment variables.

use super::{SecretProvider, SecretResolveError};
use async_trait::async_trait;

/// Resolves secrets from environment variables.
///
/// Usage in SQL: `CREATE SECRET x AS env_var('MY_ENV_VAR')`
pub struct EnvVarProvider;

#[async_trait]
impl SecretProvider for EnvVarProvider {
    fn name(&self) -> &str {
        "env_var"
    }

    fn expected_args(&self) -> usize {
        1
    }

    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError> {
        std::env::var(&args[0]).map_err(|_| SecretResolveError::ResolutionFailed {
            name: self.name().to_string(),
            reason: format!("environment variable '{}' is not set", args[0]),
        })
    }
}
