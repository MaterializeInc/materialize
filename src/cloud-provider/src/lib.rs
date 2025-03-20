// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

/// Identifies a supported cloud provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudProvider {
    /// A pseudo-provider value used by local development environments.
    Local,
    /// A pseudo-provider value used by Docker.
    Docker,
    /// A deprecated psuedo-provider value used by mzcompose.
    // TODO(benesch): remove once v0.39 ships.
    MzCompose,
    /// A pseudo-provider value used by cloudtest.
    Cloudtest,
    /// Amazon Web Services.
    Aws,
    /// Google Cloud Platform
    Gcp,
    /// Microsoft Azure
    Azure,
    /// Other generic cloud provider
    Generic,
}

impl CloudProvider {
    /// Returns true if this provider actually runs in the cloud
    pub fn is_cloud(&self) -> bool {
        matches!(self, Self::Aws | Self::Gcp | Self::Azure | Self::Generic)
    }
}

impl fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CloudProvider::Local => f.write_str("local"),
            CloudProvider::Docker => f.write_str("docker"),
            CloudProvider::MzCompose => f.write_str("mzcompose"),
            CloudProvider::Cloudtest => f.write_str("cloudtest"),
            CloudProvider::Aws => f.write_str("aws"),
            CloudProvider::Gcp => f.write_str("gcp"),
            CloudProvider::Azure => f.write_str("azure"),
            CloudProvider::Generic => f.write_str("generic"),
        }
    }
}

impl FromStr for CloudProvider {
    type Err = InvalidCloudProviderError;

    fn from_str(s: &str) -> Result<CloudProvider, InvalidCloudProviderError> {
        match s.to_lowercase().as_ref() {
            "local" => Ok(CloudProvider::Local),
            "docker" => Ok(CloudProvider::Docker),
            "mzcompose" => Ok(CloudProvider::MzCompose),
            "cloudtest" => Ok(CloudProvider::Cloudtest),
            "aws" => Ok(CloudProvider::Aws),
            "gcp" => Ok(CloudProvider::Gcp),
            "azure" => Ok(CloudProvider::Azure),
            "generic" => Ok(CloudProvider::Generic),
            _ => Err(InvalidCloudProviderError),
        }
    }
}

/// The error type for [`CloudProvider::from_str`].
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidCloudProviderError;

impl fmt::Display for InvalidCloudProviderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid cloud provider")
    }
}

impl std::error::Error for InvalidCloudProviderError {}
