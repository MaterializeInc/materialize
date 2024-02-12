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
use jsonwebtoken::EncodingKey;
use mz_frontegg_mock::FronteggMockServer;
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::now::SYSTEM_TIME;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, clap::Parser)]
#[clap(about = "Frontegg mock server", long_about = None)]
struct Args {
    /// Listen address for the server; supports DNS names.
    #[clap(long, value_name = "HOST:PORT")]
    listen_addr: String,
    /// RSA private key in PEM format for JWT encoding.
    #[clap(long)]
    encoding_key: Option<String>,
    /// File path for RSA private key in PEM format for JWT encoding.
    #[clap(long)]
    encoding_key_file: Option<PathBuf>,
    #[clap(long)]
    /// Tenant id.
    tenant_id: Uuid,
    #[clap(long)]
    /// Users and their client ids and passwords. JSON of the form: `{"u1": {"client": "client_id", "password": "password"}, "u2": {..}}`
    users: String,
    /// Roles to which users belong. JSON of the form: `{"u1": ["role1", "role2"], "u2": [..]}`
    #[clap(long)]
    roles: String,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig::default());
    let res = run(args).await;
    if let Err(err) = res {
        eprintln!("frontegg-mock: fatal: {}", err.display_with_causes());
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
    let users: BTreeMap<String, User> =
        serde_json::from_str(&args.users).with_context(|| "decoding --users")?;
    let users: BTreeMap<(String, String), String> = users
        .into_iter()
        .map(|(name, u)| ((u.client, u.password), name))
        .collect();
    let roles = serde_json::from_str(&args.roles).with_context(|| "decoding --roles")?;
    let server = FronteggMockServer::start(
        Some(&addr),
        encoding_key,
        args.tenant_id,
        users,
        roles,
        SYSTEM_TIME.clone(),
        500,
        None,
    )?;

    println!("frontegg-mock listening...");
    println!(" HTTP address: {}", server.url);

    server.handle.await??;
    anyhow::bail!("serving tasks unexpectedly exited");
}

#[derive(Deserialize)]
struct User {
    client: String,
    password: String,
}
