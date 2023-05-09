// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements Materialize cloud API methods
//! to retrieve a region's information.
//!
//! The main usage for a region is only to retrieve the
//! environment controller and acknowledge if the
//! region is enabled.

use reqwest::Method;
use serde::Deserialize;
use url::Url;

use super::{cloud_provider::CloudProvider, Client, Error};

/// Represents the region
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Region {
    /// Represents the cluster name:
    ///
    /// E.g.: `mzcloud-production-eu-west-1-0`
    pub cluster: String,
    /// Represents the complete environment controller url.
    ///
    /// E.g.: `https://ec.0.eu-west-1.aws.cloud.materialize.com:443`
    pub environment_controller_url: Url,
}

impl Region {
    /// Returns the environment controller endpoint subdomain
    pub fn ec_subdomain(&self) -> String {
        let host = self.environment_controller_url.host().unwrap().to_string();
        let index = host.find("cloud.materialize.com").unwrap();
        let subdomain: String = host[..index - 1].to_string();

        subdomain
    }
}

impl Client {
    /// Get a region from a particular cloud provider for the current user.
    pub async fn get_region(&self, cloud_provider: CloudProvider) -> Result<Region, Error> {
        let subdomain = cloud_provider.rc_subdomain();

        let req = self
            .build_request(Method::GET, ["api", "environmentassignment"], &subdomain)
            .await?;

        let regions: Vec<Region> = self.send_request(req).await?;

        Ok(regions.get(0).ok_or_else(|| Error::InvalidEnvironmentAssignment)?.to_owned())
    }
}
