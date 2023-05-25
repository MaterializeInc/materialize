// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;
use url::ParseError;

/// A custom error type for `mz` extending the `Error` enums in
/// the `mz-frontegg-auth` and `cloud-api` crate.
#[derive(Error, Debug)]
pub enum Error {
    /// An authentication error from the [`mz_frontegg_client`] crate.
    #[error(transparent)]
    AdminError(#[from] mz_frontegg_client::error::Error),
    /// A Materialize Cloud API error from the [`cloud_api`] crate.
    #[error(transparent)]
    ApiError(#[from] mz_cloud_api::error::Error),
    /// Indicates an error parsing an endpoint.
    #[error("Error trying to parse the url: {0}")]
    UrlParseError(#[from] ParseError),
    /// Error parsing (serializing/deserializing) a JSON.
    #[error("Error parsing JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    /// Error parsing an App Password.
    #[error(transparent)]
    AppPasswordParseError(#[from] mz_frontegg_auth::AppPasswordParseError),
    /// Error indicating that a profile is missing the app-password.
    #[error("Error, missing app-password.")]
    AppPasswordMissing,
    /// Error indicating that the profiles are missing in the config file.
    #[error("Error, missing profiles.")]
    ProfilesMissing,
    /// Error indicating that the profile is missing in the config file.
    #[error("Error, missing profile.")]
    ProfileMissing,
    /// Error finding the region's cloud provider.
    #[error("Error finding the region's cloud provider.")]
    CloudProviderMissing,
    /// Error parsing TOML.
    #[error("Error parsing TOML file: {0}")]
    TomlParseError(#[from] toml_edit::de::Error),
    /// Error parsing TOML.
    #[error("Error serializing the profile: {0}")]
    TomlSerializingError(#[from] toml::ser::Error),
    /// Error parsing TOML.
    #[error(transparent)]
    TomlError(#[from] toml_edit::TomlError),
    /// Error parsing UUID.
    #[error(transparent)]
    UuidError(#[from] uuid::Error),
    /// Error trying to execute a command.
    #[error("Failed to execute command: {0}")]
    CommandExecutionError(String),
    /// Error when a command fails unexpectedly.
    #[error("Command failed: {0}")]
    CommandFailed(String),
    /// I/O Error
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// I/O Error
    #[error(transparent)]
    CSVParseError(#[from] csv::Error),
}
