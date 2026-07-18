// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod oidc;
pub mod talos;

use mz_adapter::Client as AdapterClient;
use mz_frontegg_auth::Authenticator as FronteggAuthenticator;

pub use oidc::{GenericOidcAuthenticator, OidcClaims, OidcError, ValidatedClaims};
pub use talos::{
    DeriveCredential, TalosAuthenticator, TalosConfig, TalosError, TalosSessionHandle,
    ValidatedTalosClaims,
};

use mz_auth::AuthenticatorKind;

#[derive(Debug, Clone)]
pub enum Authenticator {
    Frontegg(FronteggAuthenticator),
    Password(AdapterClient),
    Sasl(AdapterClient),
    Oidc(GenericOidcAuthenticator),
    Talos(TalosAuthenticator),
    None,
}

impl Authenticator {
    pub fn kind(&self) -> AuthenticatorKind {
        match self {
            Authenticator::Frontegg(_) => AuthenticatorKind::Frontegg,
            Authenticator::Password(_) => AuthenticatorKind::Password,
            Authenticator::Sasl(_) => AuthenticatorKind::Sasl,
            Authenticator::Oidc(_) => AuthenticatorKind::Oidc,
            Authenticator::Talos(_) => AuthenticatorKind::Talos,
            Authenticator::None => AuthenticatorKind::None,
        }
    }
}
