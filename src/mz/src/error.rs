// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_frontegg_auth::AppPassword;
use thiserror::Error;

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
    /// Error parsing (serializing/deserializing) a JSON.
    #[error("Error parsing JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    /// Error parsing an App Password.
    #[error(transparent)]
    AppPasswordParseError(#[from] mz_frontegg_auth::AppPasswordParseError),
    /// Error parsing TOML.
    #[error("Error parsing TOML file: {0}")]
    TomlParseError(#[from] toml_edit::de::Error),
    /// Error parsing TOML.
    #[error(transparent)]
    TomlError(#[from] toml_edit::TomlError),
    /// I/O Error
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// I/O Error
    #[error(transparent)]
    CSVParseError(#[from] csv::Error),
}
