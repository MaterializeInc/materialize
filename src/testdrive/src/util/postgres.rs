// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use anyhow::{bail, Context};
use tokio::task::JoinHandle;
use tokio_postgres::config::Host;
use tokio_postgres::{Client, Config};
use url::Url;

use mz_ore::task;
use mz_postgres_util::make_tls;

/// Constructs a URL from PostgreSQL configuration parameters.
///
/// Returns an error if the set of configuration parameters is not representable
/// as a URL, e.g., if there are multiple hosts.
pub fn config_url(config: &Config) -> Result<Url, anyhow::Error> {
    let mut url = Url::parse("postgresql://").unwrap();

    let host = match config.get_hosts() {
        [] => "localhost".into(),
        [Host::Tcp(host)] => host.clone(),
        [Host::Unix(path)] => path.display().to_string(),
        _ => bail!("Materialize URL cannot contain multiple hosts"),
    };
    url.set_host(Some(&host))
        .context("parsing Materialize host")?;

    url.set_port(Some(match config.get_ports() {
        [] => 5432,
        [port] => *port,
        _ => bail!("Materialize URL cannot contain multiple ports"),
    }))
    .expect("known to be valid to set port");

    if let Some(user) = config.get_user() {
        url.set_username(user)
            .expect("known to be valid to set username");
    }

    Ok(url)
}

pub async fn postgres_client(
    url: &str,
) -> Result<(Client, JoinHandle<Result<(), tokio_postgres::Error>>), anyhow::Error> {
    let tls = make_tls(&Config::from_str(url)?)?;
    let (client, connection) = tokio_postgres::connect(url, tls)
        .await
        .context("connecting to postgres")?;

    println!("Connecting to PostgreSQL server at {}...", url);
    let handle = task::spawn(|| "postgres_client_task", connection);

    Ok((client, handle))
}
