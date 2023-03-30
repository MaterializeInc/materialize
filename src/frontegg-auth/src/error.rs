// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::StatusCode;
use std::fmt;
use thiserror::Error;

use crate::app_password::AppPasswordParseError;

/// An error returned by a [`Client`].
///
/// [`Client`]: crate::Client
#[derive(Debug)]
pub enum Error {
    /// An error in the underlying transport.
    Transport(reqwest::Error),
    /// An error returned by the API.
    Api(ApiError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "frontegg error: transport: {e}"),
            Error::Api(e) => write!(f, "frontegg error: api: {e}"),
        }
    }
}

impl std::error::Error for Error {}

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

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Error {
        Error::Transport(e)
    }
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Error {
        Error::Api(e)
    }
}

#[derive(Error, Debug)]
pub enum FronteggError {
    #[error(transparent)]
    InvalidPasswordFormat(#[from] AppPasswordParseError),
    #[error("invalid token format: {0}")]
    InvalidTokenFormat(#[from] jsonwebtoken::errors::Error),
    #[error("authentication token exchange failed: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("authentication token expired")]
    TokenExpired,
    #[error("unauthorized organization")]
    UnauthorizedTenant,
    #[error("email in access token did not match the expected email")]
    WrongEmail,
    #[error("request timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
}
