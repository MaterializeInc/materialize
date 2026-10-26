// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use reqwest::StatusCode;
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

impl Error {
    /// Whether authentication failed because the client's credentials were rejected: Frontegg
    /// refused them, or they are malformed or belong to a different user or tenant.
    ///
    /// Every other failure is on the server side. That includes Frontegg throttling (`408`,
    /// `429`) past the client's retries, and a token Frontegg issued for valid credentials that
    /// does not validate against the configured key, the clock or the expected claims.
    pub fn is_credential_rejection(&self) -> bool {
        match self {
            Error::ReqwestError(e) => e
                .status()
                .is_some_and(|s| s == StatusCode::UNAUTHORIZED || s == StatusCode::FORBIDDEN),
            Error::InvalidPasswordFormat(_)
            | Error::InvalidAppPassword
            | Error::UnauthorizedTenant
            | Error::WrongUser
            | Error::UserNameTooLong
            | Error::InvalidTenantApiTokenUser => true,
            Error::InvalidTokenFormat(_)
            | Error::MissingClaims
            | Error::TokenExpired
            | Error::Timeout(_)
            | Error::MiddlewareError(_)
            | Error::Internal(_) => false,
        }
    }
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use axum::Router;
    use axum::http::StatusCode as AxumStatusCode;
    use axum::routing::post;
    use tokio::net::TcpListener;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn only_rejected_credentials_are_the_clients_fault() {
        // Answers every request with the status code in its path.
        let app = Router::new().route(
            "/{:status_code}",
            post(
                |axum::extract::Path(code): axum::extract::Path<u16>| async move {
                    AxumStatusCode::from_u16(code).unwrap()
                },
            ),
        );
        let tcp = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let addr = tcp.local_addr().unwrap();
        mz_ore::task::spawn(|| "test-server", async move {
            axum::serve(tcp, app.into_make_service()).await.unwrap();
        });

        for (status, rejection) in [
            (401, true),
            (403, true),
            (408, false),
            (429, false),
            (503, false),
        ] {
            let err = reqwest::Client::new()
                .post(format!("http://{addr}/{status}"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .expect_err("an error status");
            assert_eq!(
                Error::from(err).is_credential_rejection(),
                rejection,
                "status {status}"
            );
        }

        // Checks on the token Frontegg issued fail on the server side.
        assert!(!Error::TokenExpired.is_credential_rejection());
        assert!(!Error::MissingClaims.is_credential_rejection());
        // Checks on what the client sent do not.
        assert!(Error::WrongUser.is_credential_rejection());
        assert!(Error::UserNameTooLong.is_credential_rejection());
    }
}
