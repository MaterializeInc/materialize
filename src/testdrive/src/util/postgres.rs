// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio_postgres::config::Host;
use tokio_postgres::Config;
use url::Url;

use crate::error::Error;

/// Constructs a URL from PostgreSQL configuration parameters.
///
/// Returns an error if the set of configuration parameters is not representable
/// as a URL, e.g., if there are multiple hosts.
pub fn config_url(config: &Config) -> Result<Url, Error> {
    let mut url = Url::parse("postgresql://").unwrap();

    let host = match config.get_hosts() {
        [] => "localhost".into(),
        [Host::Tcp(host)] => host.clone(),
        [Host::Unix(path)] => path.display().to_string(),
        _ => {
            return Err(Error::General {
                ctx: "materialized URL cannot contain multiple hosts".into(),
                cause: None,
                hints: vec![],
            })
        }
    };
    url.set_host(Some(&host)).map_err(|e| Error::General {
        ctx: "parsing materialized host".into(),
        cause: Some(Box::new(e)),
        hints: vec![],
    })?;

    url.set_port(Some(match config.get_ports() {
        [] => 5432,
        [port] => *port,
        _ => {
            return Err(Error::General {
                ctx: "materialized URL cannot contain multiple ports".into(),
                cause: None,
                hints: vec![],
            })
        }
    }))
    .expect("known to be valid to set port");

    if let Some(user) = config.get_user() {
        url.set_username(user)
            .expect("known to be valid to set username");
    }

    Ok(url)
}
