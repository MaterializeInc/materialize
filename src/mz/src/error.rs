// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines custom error types and structs related to MZ.
//!
//! [Error::ApiError] is an error struct that represents an error returned by the
//! Materialize cloud API. It contains information about the HTTP status code and
//! a vector of error messages.
//!
//! [`Error`](`enum@Error`) is a custom error type containing multiple variants
//! for erros produced by the self crate, internal crates and external crates.

use hyper::header::{InvalidHeaderValue, ToStrError};
use thiserror::Error;
use url::ParseError;

/// A custom error type for `mz` extending the `Error` enums in
/// the internal crates `mz-frontegg-auth`, `cloud-api` and
/// `mz_frontegg_auth` and external crates like `serde_json`,
/// `toml_edit`, `uuid`, `std::io` and `csv`.
#[derive(Error, Debug)]
pub enum Error {
    /// An authentication error from the [`mz_frontegg_client`] crate.
    #[error(transparent)]
    AdminError(#[from] mz_frontegg_client::error::Error),
    /// A Materialize Cloud API error from the [mz_cloud_api] crate.
    #[error(transparent)]
    ApiError(#[from] mz_cloud_api::error::Error),
    /// A Frontegg authentication error.
    #[error(transparent)]
    AuthError(#[from] mz_frontegg_auth::Error),
    /// Indicates an error parsing an endpoint.
    #[error("Error parsing URL: {0}.\n\nTo resolve this issue, please verify the correctness of the URLs in the configuration file or the ones passed as parameters.")]
    UrlParseError(#[from] ParseError),
    /// Error parsing (serializing/deserializing) a JSON.
    #[error("Error parsing JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    /// Error parsing (serializing/deserializing) a JSON using reqwest.
    #[error("Error parsing request JSON: {0}")]
    ReqwestJsonParseError(#[from] reqwest::Error),
    /// Error parsing an App Password.
    #[error("Error: {0}. \n\nTo resolve this issue, please verify the correctness of the app-password in the configuration file.")]
    AppPasswordParseError(#[from] mz_frontegg_auth::AppPasswordParseError),
    /// Error indicating that a profile is missing the app-password.
    #[error("Error: The current profile does not have an app-password.")]
    AppPasswordMissing,
    /// Error indicating that the profiles are missing in the config file.
    #[error("Error: No profiles available in the configuration file. \n\nTo resolve this issue, you can add a new profile using the following command: `mz profile init`")]
    ProfilesMissing,
    /// Error indicating that the profile is missing in the config file.
    #[error("Error: The profile '{0}' is missing in the configuration file. \n\nTo resolve this issue, you can either: \n1. Add the missing profile using the command `mz profile --profile {0} init` \n2. Set another existing profile using the command: `mz config set profile <profile_name>`.")]
    ProfileMissing(String),
    /// Error finding the region's cloud provider.
    #[error("Cloud region not found.")]
    CloudRegionMissing,
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
    /// Error that happens when a user cancels a login from the console UI.
    #[error("Login canceled.")]
    LoginOperationCanceled,
    /// Error that raises when the clientid or secret are invalid.
    /// It is a simpler alternative for parsing errors.
    #[error("Invalid app-password.")]
    InvalidAppPassword,
    /// Error that raises when the region is enabled
    /// but not ready yet.
    #[error("The region is not ready yet.")]
    NotReadyRegion,
    /// Error that raises when the region is enabled
    /// but not resolvable yet.
    #[error("The region is not resolvable yet.")]
    NotResolvableRegion,
    /// Error that raises when a timeout is reached.
    #[error("Timeout reached. Error: {0}")]
    // It uses a Box<> to avoid recursion issues.
    TimeoutError(Box<Error>),
    /// Error that raises when the region is enabled and resolvable
    /// but `pg_isready` fails
    #[error("The region is not ready to accept SQL statements. `pg_isready` failed.")]
    NotPgReadyError,
    /// Error that raises when parsing semver.
    #[error("Error parsing semver. Description: {0}")]
    SemVerParseError(semver::Error),
    /// Error that raises when trying to get the current
    /// timestamp using `SystemTime::now().duration_since(UNIX_EPOCH)`
    #[error("Error retrieving the current timestamp.")]
    TimestampConversionError,
    /// Error parsing a header for a request.
    #[error("Error parsing header value: {0}")]
    HeaderParseError(InvalidHeaderValue),
    /// Error that raises when `dirs::cache_dir()` returns None.
    #[error("Error. Cache dir not found")]
    CacheDirNotFoundError,
    /// Error that raises turning a Header value into str.
    #[error("Error parsing a request header. Description: {0}")]
    HeaderToStrError(ToStrError),
    /// Error that raises when the request response
    /// is invalid. Chances are that the request is not a 301.
    #[error("Error the latest version header from the redirect request was not found. Verify the request is redirecting.")]
    LatestVersionHeaderMissingError,
    /// Error that occurs when attempting to find the home directory.
    #[error("An error occurred while trying to find the home directory.")]
    HomeDirNotFoundError,
    /// Error that raises when the security framework for macOS
    /// to retrieve or set passwords to  the keychain fails.
    #[error("Error using keychain. {0}")]
    MacOsSecurityError(String),
    /// Error that raises when the vault value from the config is invalid.
    #[error("Vault value for the profile is invalid.")]
    InvalidVaultError,
    /// Error that raises when the user tries to create a new profile with a name that already exists.
    #[error("The profile name '{0}' already exists. You can either use 'mz profile init -f' to replace it or 'mz profile init --profile <PROFILE>' to choose another name.")]
    ProfileNameAlreadyExistsError(String),
}
