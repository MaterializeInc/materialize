// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Environment variable secret provider.
//!
//! Resolves secret values by reading from process environment variables.

use super::{SecretProvider, SecretResolveError};
use async_trait::async_trait;
use std::ops::RangeInclusive;

/// Resolves secrets from environment variables.
///
/// Usage in SQL: `CREATE SECRET x AS env_var('MY_ENV_VAR')`
pub(super) struct EnvVarProvider;

#[async_trait]
impl SecretProvider for EnvVarProvider {
    fn name(&self) -> &str {
        "env_var"
    }

    fn accepted_args(&self) -> RangeInclusive<usize> {
        1..=1
    }

    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError> {
        std::env::var(&args[0]).map_err(|_| SecretResolveError::ResolutionFailed {
            name: self.name().to_string(),
            reason: format!("environment variable '{}' is not set", args[0]),
        })
    }
}
