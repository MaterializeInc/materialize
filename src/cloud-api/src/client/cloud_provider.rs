// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements Materialize cloud API sync endpoint to
//! list all the availables cloud providers and regions, represented
//! by [`CloudProvider`].
//! As utilities, the module implements the enum [`CloudProviderRegion`].
//! It is useful to validate and compare external input (e.g. user input)
//! with the results from the cloud API represented by [`CloudProvider`].
//!
//! Example:
//! ```ignore
//! // List all the available providers
//! let cloud_providers = client.list_cloud_providers().await?;
//!
//! // Search the cloud provider
//! let user_input = "aws/us-east-1";
//! let cloud_provider_region_selected: CloudProviderRegion = CloudProviderRegion::from_str(user_input)?;
//! let cloud_provider: CloudProvider = cloud_providers
//!    .iter()
//!    .find(|cp| cp.as_cloud_provider_region().unwrap() == cloud_provider_region_selected)
//!    .unwrap()
//!    .to_owned();
//! ```

use std::{fmt::Display, str::FromStr};

use crate::error::Error;

use super::Client;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use url::Url;

/// Holds all the information related to a cloud provider and a particular region.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CloudProvider {
    /// Contains the concatenation between cloud provider name and region:
    ///
    /// E.g.: `aws/us-east-1` or `aws/eu-west-1`
    pub id: String,
    /// Contains the region name:
    ///
    /// E.g.: `us-east-1` or `eu-west-1`
    pub name: String,
    /// Contains the complete region controller url.
    ///
    /// E..g: `https://rc.eu-west-1.aws.cloud.materialize.com`
    pub api_url: Url,
    /// Contains the cloud provider name.
    ///
    /// E.g.: `aws` or `gcp`
    pub cloud_provider: String,
}

impl CloudProvider {
    /// Converts the CloudProvider object to a CloudProviderRegion.
    pub fn as_cloud_provider_region(&self) -> Result<CloudProviderRegion, Error> {
        match self.id.as_str() {
            "aws/us-east-1" => Ok(CloudProviderRegion::AwsUsEast1),
            "aws/eu-west-1" => Ok(CloudProviderRegion::AwsEuWest1),
            _ => Err(Error::CloudProviderRegionParseError),
        }
    }

    /// Returns the region controller endpoint subdomain
    pub fn rc_subdomain(&self) -> String {
        let host = self.api_url.host().unwrap().to_string();
        let index = host.find("cloud.materialize.com").unwrap();
        let subdomain: String = host[..index - 1].to_string();

        subdomain
    }
}

/// Represents a cloud provider and a region.
/// Useful to transform a user input selecting a
/// cloud provider region to an enum and vice-versa.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CloudProviderRegion {
    /// Represents `aws/us-east-1` cloud provider and region
    #[serde(rename = "aws/us-east-1")]
    AwsUsEast1,
    /// Represents `aws/eu-west-1` cloud provider and region
    #[serde(rename = "aws/eu-west-1")]
    AwsEuWest1,
}

impl CloudProviderRegion {
    /// Converts a CloudProvider object to a CloudProviderRegion.
    pub fn from_cloud_provider(cloud_provider: CloudProvider) -> Result<Self, Error> {
        match cloud_provider.id.as_str() {
            "aws/us-east-1" => Ok(CloudProviderRegion::AwsUsEast1),
            "aws/eu-west-1" => Ok(CloudProviderRegion::AwsEuWest1),
            _ => Err(Error::CloudProviderRegionParseError),
        }
    }
}

impl Display for CloudProviderRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudProviderRegion::AwsUsEast1 => write!(f, "aws/us-east-1"),
            CloudProviderRegion::AwsEuWest1 => write!(f, "aws/eu-west-1"),
        }
    }
}

impl FromStr for CloudProviderRegion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aws/us-east-1" => Ok(CloudProviderRegion::AwsUsEast1),
            "aws/eu-west-1" => Ok(CloudProviderRegion::AwsEuWest1),
            _ => Err(Error::CloudProviderRegionParseError),
        }
    }
}

impl Client {
    /// List all the available cloud providers.
    ///
    /// E.g.: [us-east-1, eu-west-1]
    pub async fn list_cloud_providers(&self) -> Result<Vec<CloudProvider>, Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct CloudProviderResponse {
            data: Vec<CloudProvider>,
            #[serde(skip_serializing_if = "Option::is_none")]
            next_cursor: Option<String>,
        }
        let mut cloud_providers: Vec<CloudProvider> = vec![];
        let mut cursor: String = String::new();

        loop {
            let req = self
                .build_request(Method::GET, ["api", "cloud-regions"], format!("https://sync.{}", self.endpoint))
                .await?;

            let req = req.query(&[("limit", "50"), ("cursor", &cursor)]);

            let response: CloudProviderResponse = self.send_request(req).await?;
            cloud_providers.extend(response.data);

            if let Some(next_cursor) = response.next_cursor {
                cursor = next_cursor;
            } else {
                break;
            }
        }

        Ok(cloud_providers)
    }
}
