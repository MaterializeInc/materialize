// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for Materialize authentication.

use std::fmt::Debug;

use async_trait::async_trait;

/// A handle to an authentication session.
///
/// An authentication session represents a duration of time during which a
/// user's authentication is known to be valid.
///
/// [`OidcAuthSessionHandle::external_metadata_rx`] can be used to receive events if
/// the session's metadata is updated.
///
/// [`OidcAuthSessionHandle::expired`] can be used to learn if the session has
/// failed to refresh the validity of the API key.
#[async_trait]
pub trait OidcAuthSessionHandle: Debug + Send {
    /// Returns the name of the user that created the session.
    fn user(&self) -> &str;
    /// Completes when the authentication session has expired.
    async fn expired(&mut self);
}

#[async_trait]
pub trait OidcAuthenticator {
    /// The error type for the authenticator.
    type Error;
    /// The authenticator's session handle type.
    type SessionHandle: OidcAuthSessionHandle;
    /// Claims that have been validated by [`OidcAuthenticator::validate_access_token`].
    type ValidatedClaims;
    /// Establishes a new authentication session.
    /// If successful, returns a [`OidcAuthenticator::SessionHandle`] to the authentication session.
    /// Otherwise, returns [`OidcAuthenticator::Error`].
    async fn authenticate(
        &self,
        expected_user: &str,
        password: &str,
    ) -> Result<Self::SessionHandle, Self::Error>;

    /// Validates an access token, returning the validated claims.
    ///
    /// If `expected_user` is provided, the token's user name is additionally
    /// validated to match `expected_user`.
    async fn validate_access_token(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<Self::ValidatedClaims, Self::Error>;
}
