// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines custom error types and structs related to the Datadog API.
//!
//! [`ApiError`] is an error struct that represents an error returned by the
//! Datadog API. It contains information about the HTTP status code and
//! a vector of error messages.
//!
//! [`Error`](`enum@Error`) is a custom error type
//!
//! It contains three variants:
//! * [`Error::Transport`]: indicates a transport error from the `reqwest`
//!   crate during a network request.
//! * [`Error::Api`]: indicates a Datadog API error while
//!   processing the request.

use std::fmt;

use reqwest::StatusCode;
use thiserror::Error;

/// A custom error returned after an error at the Datadog API.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// The HTTP status code.
    pub status_code: StatusCode,
    /// A detailed message about the error conditions.
    pub errors: Vec<String>,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} (status {})",
            self.errors.join(","),
            self.status_code
        )
    }
}

impl std::error::Error for ApiError {}

/// A custom error type containing all the possible errors in the crate for the Datadog API.
#[derive(Error, Debug)]
pub enum Error {
    /// Indicates a transport error from the `reqwest`
    /// crate during a network request.
    #[error("Network error during a Datadog API request: {0}")]
    Transport(#[from] reqwest::Error),
    /// Indicates a Datadog API error from a request.
    #[error("API error during a Datadog API request: {0}")]
    Api(#[from] ApiError),
    /// Indicates the URL is cannot-be-a-base.
    #[error("Error while manipulating URL. The URL is cannot-be-a-base.")]
    UrlBaseError,
    /// Indicates that a timeout has been reached.
    #[error("Timeout reached.")]
    TimeoutError,
}
