// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

pub mod hash;
pub mod password;

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
