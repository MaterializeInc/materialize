// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS STS client and utilities.

use aws_sdk_sts::Client;

use crate::config::AwsConfig;
use crate::util;

/// Constructs a new AWS STS client that respects the
/// [system proxy configuration](mz_http_proxy#system-proxy-configuration).
pub fn client(config: &AwsConfig) -> Result<Client, anyhow::Error> {
    let mut builder = aws_sdk_sts::config::Builder::from(config.inner());
    if let Some(endpoint) = config.endpoint() {
        builder = builder.endpoint_resolver(endpoint.clone());
    }
    let conn = util::connector("STS")?;
    Ok(Client::from_conf_conn(builder.build(), conn))
}
