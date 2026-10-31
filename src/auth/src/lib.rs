// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

pub mod group_claims;
pub mod hash;
pub mod password;
pub mod user;

/// Identifies which authentication mechanism was used for a session.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthenticatorKind {
    /// Authenticated via Frontegg.
    Frontegg,
    /// Authenticated via internally stored password hashes.
    Password,
    /// Authenticated via SASL.
    Sasl,
    /// Authenticated via OIDC (JWT tokens).
    Oidc,
    /// No authentication performed.
    #[default]
    None,
}

/// A sentinel type signifying successful authentication.
///
/// This type is used to establish an authenticated Adapter client session,
/// and should only be constructed by authenticators to indicate that authentication
/// has succeeded. It may also be used when authentication is not required.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authenticated;

/// Decides if a given password is a JWT by checking if we can decode its
/// header.
///
/// This only routes a password to the right authenticator. It does not
/// validate the token.
pub fn is_jwt(password: &str) -> bool {
    jsonwebtoken::decode_header(password).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_is_jwt() {
        // A real JWT header decodes successfully.
        assert!(is_jwt("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature"));
        // Not JWTs: plain strings, wrong segment count, non-JSON headers.
        for s in [
            "",
            "secure_password",
            "p4ss.w0rd",
            "aaa.bbb.ccc",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig.extra",
        ] {
            assert!(!is_jwt(s), "is_jwt({s:?})");
        }
    }
}
