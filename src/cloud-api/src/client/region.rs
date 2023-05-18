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

use crate::client::cloud_provider::CloudProvider;
use crate::client::{Client, Error};

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

impl Client {
    /// Get a region from a particular cloud provider for the current user.
    pub async fn get_region(&self, cloud_provider: CloudProvider) -> Result<Region, Error> {
        let req = self
            .build_request(
                Method::GET,
                ["api", "environmentassignment"],
                cloud_provider.api_url,
            )
            .await?;

        let regions: Vec<Region> = self.send_request(req).await?;

        Ok(regions
            .get(0)
            .ok_or_else(|| Error::InvalidEnvironmentAssignment)?
            .to_owned())
    }
}
