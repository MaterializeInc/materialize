// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_s3::config::Builder;
use aws_sdk_s3::Client;
use aws_types::sdk_config::SdkConfig;

/// Creates a new client from an [SDK config](aws_types::sdk_config::SdkConfig)
/// with Materialize-specific customizations.
///
/// Specifically, if the SDK config overrides the endpoint URL, the client
/// will be configured to use path-style addressing, as custom AWS endpoints
/// typically do not support virtual host-style addressing.
pub fn new_client(sdk_config: &SdkConfig) -> Client {
    let conf = Builder::from(sdk_config)
        .force_path_style(sdk_config.endpoint_url().is_some())
        .build();
    Client::from_conf(conf)
}
