// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::{BehaviorVersion, ConfigLoader};

#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod s3_uploader;

/// Creates an AWS SDK configuration loader with the defaults for the latest
/// behavior version plus some Materialize-specific overrides.
pub fn defaults() -> ConfigLoader {
    // Use the SDK's latest behavior version. We already pin the crate versions,
    // and CI puts version upgrades through rigorous testing, so we're happy to
    // take the latest behavior version. We can adjust this in the future as
    // necessary, if the AWS SDK ships a new behavior version that causes
    // trouble.
    let behavior_version = BehaviorVersion::latest();

    // This is the only method allowed to call `aws_config::defaults`.
    #[allow(clippy::disallowed_methods)]
    let loader = aws_config::defaults(behavior_version);

    // The AWS SDK's default HTTP client uses rustls, which aligns with our
    // FIPS 140-3 compliance strategy (aws-lc-rs as crypto backend).
    loader
}
