// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use thiserror::Error;

use crate::AppPasswordParseError;

#[derive(Clone, Error, Debug)]
pub enum Error {
    #[error(transparent)]
    InvalidPasswordFormat(#[from] AppPasswordParseError),
    #[error("invalid token format: {0}")]
    InvalidTokenFormat(#[from] jsonwebtoken::errors::Error),
    #[error("authentication token exchange failed: {0}")]
    ReqwestError(Arc<reqwest::Error>),
    #[error("middleware programming error: {0}")]
    MiddlewareError(Arc<anyhow::Error>),
    #[error("authentication token missing claims")]
    MissingClaims,
    #[error("authentication token expired")]
    TokenExpired,
    #[error("unauthorized organization")]
    UnauthorizedTenant,
    #[error("the app password was not valid")]
    InvalidAppPassword,
    #[error("user in access token did not match the expected user")]
    WrongUser,
    #[error("user name too long")]
    UserNameTooLong,
    #[error("user declared by tenant access token cannot be an email address")]
    InvalidTenantApiTokenUser,
    #[error("request timeout")]
    Timeout(Arc<tokio::time::error::Elapsed>),
    #[error("internal error")]
    Internal(Arc<anyhow::Error>),
}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        Error::Internal(Arc::new(value))
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(value: tokio::time::error::Elapsed) -> Self {
        Error::Timeout(Arc::new(value))
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::ReqwestError(Arc::new(value))
    }
}

impl From<reqwest_middleware::Error> for Error {
    fn from(value: reqwest_middleware::Error) -> Self {
        match value {
            reqwest_middleware::Error::Middleware(e) => Error::MiddlewareError(Arc::new(e)),
            reqwest_middleware::Error::Reqwest(e) => Error::ReqwestError(Arc::new(e)),
        }
    }
}
