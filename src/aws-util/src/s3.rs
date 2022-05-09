// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS S3 client and utilities.

use aws_sdk_s3::Client;
use aws_types::SdkConfig;

use crate::util;

/// Constructs a new AWS S3 client that respects the system proxy configuration.
pub fn client(config: &SdkConfig) -> Client {
    Client::from_conf_conn(config.into(), util::connector())
}
