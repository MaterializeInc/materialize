// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Manages a single Materialize environment.
//!
//! It listens for SQL connections on port 6875 (MTRL) and for HTTP connections
//! on port 6876.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Context;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_frontegg_mock::models::{UserConfig, UserRole};
use mz_frontegg_mock::server::FronteggMockServer;
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::now::SYSTEM_TIME;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(about = "Frontegg mock server", long_about = None)]
struct Args {
    /// Listen address for the server; supports DNS names.
    #[clap(long, value_name = "HOST:PORT")]
    listen_addr: String,
    /// Issuer to include in generated JWTs.
    #[clap(long)]
    issuer: String,
    /// RSA private key in PEM format for JWT encoding.
    #[clap(long)]
    encoding_key: Option<String>,
    /// File path for RSA private key in PEM format for JWT encoding.
    #[clap(long)]
    encoding_key_file: Option<PathBuf>,
    /// RSA public key in PEM format for JWT decoding.
    #[clap(long)]
    decoding_key: Option<String>,
    /// File path for RSA public key in PEM format for JWT decoding.
    #[clap(long)]
    decoding_key_file: Option<PathBuf>,
    /// User information.
    /// JSON of the form: `[{"email": "...", "password": "...", "tenant_id": "...", initial_api_tokens: [{"client_id": "...", "secret": "..."}], "roles": ["role1", "role2"]}]`
    #[clap(long)]
    users: String,
    /// Permissions assigned for specific roles.
    /// JSON of the form: `{"rolename": ["permission1", "permission2"]}`
    #[clap(long)]
    role_permissions: Option<String>,
    /// Roles information.
    /// JSON of the form: `[{"id":"1", "name": "Organization Admin"}, {"id":"2", "name": "Organization Member"}]`
    #[clap(long)]
    roles: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args: Args = cli::parse_args(CliConfig::default());
    let res = run(args).await;
    if let Err(err) = res {
        tracing::error!("frontegg-mock: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let mut addrs = tokio::net::lookup_host(&args.listen_addr)
        .await
        .unwrap_or_else(|_| panic!("could not resolve {}", args.listen_addr));
    let Some(addr) = addrs.next() else {
        panic!("{} did not resolve to any addresses", args.listen_addr);
    };
    let encoding_key = match (args.encoding_key, args.encoding_key_file) {
        (None, Some(path)) => {
            let key = std::fs::read(path)?;
            EncodingKey::from_rsa_pem(&key).with_context(|| "decoding --encoding-key-file")?
        }
        (Some(key), None) => {
            EncodingKey::from_rsa_pem(key.as_bytes()).with_context(|| "decoding --encoding-key")?
        }
        _ => anyhow::bail!("exactly one of --encoding-key or --encoding-key-file expected"),
    };
    let decoding_key = match (args.decoding_key, args.decoding_key_file) {
        (None, Some(path)) => {
            let key = std::fs::read(path)?;
            DecodingKey::from_rsa_pem(&key).with_context(|| "decoding --decoding-key-file")?
        }
        (Some(key), None) => {
            DecodingKey::from_rsa_pem(key.as_bytes()).with_context(|| "decoding --decoding-key")?
        }
        _ => anyhow::bail!("exactly one of --decoding-key or --decoding-key-file expected"),
    };
    let users: BTreeMap<String, UserConfig> = serde_json::from_str::<Vec<UserConfig>>(&args.users)
        .with_context(|| "decoding --users")?
        .into_iter()
        .map(|user| (user.email.clone(), user))
        .collect();
    let tenant_api_tokens = BTreeMap::new();
    let role_permissions = match &args.role_permissions {
        Some(s) => serde_json::from_str(s).with_context(|| "decoding --role-permissions")?,
        None => None,
    };
    let roles = match &args.roles {
        Some(s) => {
            Some(serde_json::from_str::<Vec<UserRole>>(s).with_context(|| "decoding --roles")?)
        }
        None => None,
    };
    let server = FronteggMockServer::start(
        Some(&addr),
        args.issuer,
        encoding_key,
        decoding_key,
        users,
        tenant_api_tokens,
        role_permissions,
        SYSTEM_TIME.clone(),
        500,
        None,
        roles,
    )
    .await?;

    println!("frontegg-mock listening...");
    println!(" HTTP address: {}", server.base_url);

    server.handle.await?;
    anyhow::bail!("serving tasks unexpectedly exited");
}
