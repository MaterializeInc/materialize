// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod app_password;
mod auth;
mod client;
mod error;
mod metrics;

use std::path::PathBuf;

pub use auth::{
    Authenticator, AuthenticatorConfig, ClaimMetadata, ClaimTokenType, Claims,
    DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
pub use client::tokens::{ApiTokenArgs, ApiTokenResponse};
pub use client::Client;
pub use error::Error;
use uuid::Uuid;

pub use crate::app_password::{AppPassword, AppPasswordParseError};

/// Command line arguments for frontegg.
#[derive(Debug, Clone, clap::Parser)]
pub struct FronteggCliArgs {
    /// Enables Frontegg authentication for the specified tenant ID.
    #[clap(
        long,
        env = "FRONTEGG_TENANT",
        requires_all = &["frontegg_api_token_url", "frontegg_admin_role"],
        value_name = "UUID",
    )]
    frontegg_tenant: Option<Uuid>,
    /// JWK used to validate JWTs during Frontegg authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(long, env = "FRONTEGG_JWK", requires = "frontegg_tenant")]
    frontegg_jwk: Option<String>,
    /// Path to JWK used to validate JWTs during Frontegg authentication as a PEM public
    /// key.
    #[clap(long, env = "FRONTEGG_JWK_FILE", requires = "frontegg_tenant")]
    frontegg_jwk_file: Option<PathBuf>,
    /// The full URL (including path) to the Frontegg api-token endpoint.
    #[clap(long, env = "FRONTEGG_API_TOKEN_URL", requires = "frontegg_tenant")]
    frontegg_api_token_url: Option<String>,
    /// The name of the admin role in Frontegg.
    #[clap(long, env = "FRONTEGG_ADMIN_ROLE", requires = "frontegg_tenant")]
    frontegg_admin_role: Option<String>,
}
