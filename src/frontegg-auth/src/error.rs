// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

use crate::AppPasswordParseError;

#[derive(Error, Debug)]
pub enum Error {
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
