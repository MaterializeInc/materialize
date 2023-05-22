// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines custom error types and structs related to Frontegg API.
//!
//! [`ApiError`] is an error struct that represents an error returned by the
//! Frontegg API. It contains information about the HTTP status code and a
//! vector of error messages.
//!
//! [`Error`](`enum@Error`) is a custom error type that extends the
//! [`mz_frontegg_auth::Error`] enum.
//!
//! It contains three variants:
//! * [`Error::Auth`]: represents an authentication error from the
//!   [`mz-frontegg-auth`] crate.
//! * [`Error::Transport`]: represents a transport error from the `reqwest`
//!   crate during a network request.
//! * [`Error::Api`]: represents an Frontegg API error from a request.

use std::fmt;

use reqwest::StatusCode;
use thiserror::Error;

/// An error returned by the Frontegg API.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// The HTTP status code.
    pub status_code: StatusCode,
    /// A detailed message about the error conditions.
    pub messages: Vec<String>,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} (status {})",
            self.messages.join(","),
            self.status_code
        )
    }
}

impl std::error::Error for ApiError {}

/// A custom error type that extends the `Error` enum in the `mz-frontegg-auth`
/// crate.
#[derive(Error, Debug)]
pub enum Error {
    /// An authentication error from the [`mz_frontegg_auth`] crate.
    #[error(transparent)]
    Auth(#[from] mz_frontegg_auth::Error),
    /// A transport error from the `reqwest` crate during a network request.
    #[error("frontegg error: transport: {0}")]
    Transport(#[from] reqwest::Error),
    /// An Frontegg API error from a request.
    #[error("frontegg error: api: {0}")]
    Api(#[from] ApiError),
    /// An error indicating that the JWK set contains no keys.
    #[error("JWK set contained no keys.")]
    EmptyJwks,
    /// An error thrown after trying to fetch the JWKS from well-known endpoint.
    #[error("Error fetching JWKS.")]
    FetchingJwks,
    /// An error thrown after trying to build a [jsonwebtoken::DecodingKey] using JWKS
    #[error("Converting JWK into decoding key.")]
    ConvertingJwks,
    /// An error indicating that the claims are invalid, acoording to the structure
    /// or keys provided. If the error persists,
    /// check if the token claims matches the struct.
    #[error("Decoding JWT claims.")]
    DecodingClaims,
}
