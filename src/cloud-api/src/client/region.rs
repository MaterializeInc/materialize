// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements Materialize cloud API methods
//! to GET, CREATE or DELETE a region.
//! To delete an region correctly make sure to
//! contact support.
//!
//! For a better experience retrieving all the available
//! environments, use [`Client::get_all_regions()`]

use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize};

use crate::client::cloud_provider::CloudProvider;
use crate::client::{Client, Error};

/// A customer region is represented in this structure
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Region {
    /// The connection info and metadata corresponding to this Region
    /// may not be set if the region is in the process
    /// of being created (see [RegionState] for details)
    pub region_info: Option<RegionInfo>,

    /// The state of this Region
    pub region_state: RegionState,
}

/// Connection details for an active region
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RegionInfo {
    /// Represents the environmentd PG wire protocol address.
    ///
    /// E.g.: 3es24sg5rghjku7josdcs5jd7.eu-west-1.aws.materialize.cloud:6875
    pub sql_address: String,
    /// Represents the environmentd HTTP address.
    ///
    /// E.g.: 3es24sg5rghjku7josdcs5jd7.eu-west-1.aws.materialize.cloud:443
    pub http_address: String,
    /// Indicates true if the address is resolvable by DNS.
    pub resolvable: bool,
    /// The time at which the region was enabled
    pub enabled_at: Option<DateTime<Utc>>,
}

/// The state of a customer region
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum RegionState {
    /// Enabled region
    Enabled,

    /// Enablement Pending
    /// [region_info][Region::region_info] field will be `null` while the region is in this state
    EnablementPending,

    /// Deletion Pending
    /// [region_info][Region::region_info] field will be `null` while the region is in this state
    DeletionPending,

    /// Soft deleted; Pending hard deletion
    /// [region_info][Region::region_info] field will be `null` while the region is in this state
    SoftDeleted,
}

impl Client {
    /// Get a customer region in a partciular cloud region for the current user.
    pub async fn get_region(&self, provider: CloudProvider) -> Result<Region, Error> {
        // Send request to the subdomain
        let req = self
            .build_region_request(Method::GET, ["api", "region"], None, &provider, Some(1))
            .await?;

        match self.send_request::<Region>(req).await {
            Ok(region) => match region.region_state {
                RegionState::SoftDeleted => Err(Error::EmptyRegion),
                RegionState::DeletionPending => Err(Error::EmptyRegion),
                RegionState::Enabled => Ok(region),
                RegionState::EnablementPending => Ok(region),
            },
            Err(Error::SuccesfullButNoContent) => Err(Error::EmptyRegion),
            Err(e) => Err(e),
        }
    }

    /// Get all the available customer regions for the current user.
    pub async fn get_all_regions(&self) -> Result<Vec<Region>, Error> {
        let cloud_providers: Vec<CloudProvider> = self.list_cloud_regions().await?;
        let mut regions: Vec<Region> = vec![];

        for cloud_provider in cloud_providers {
            match self.get_region(cloud_provider).await {
                Ok(region) => {
                    regions.push(region);
                }
                // Skip cloud regions with no customer region
                Err(Error::EmptyRegion) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(regions)
    }

    /// Creates a customer region in a particular cloud region for the current user
    pub async fn create_region(
        &self,
        version: Option<String>,
        environmentd_extra_args: Vec<String>,
        cloud_provider: CloudProvider,
    ) -> Result<Region, Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            #[serde(skip_serializing_if = "Option::is_none")]
            environmentd_image_ref: Option<String>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            environmentd_extra_args: Vec<String>,
        }

        let body = Body {
            environmentd_image_ref: version.map(|v| match v.split_once(':') {
                None => format!("materialize/environmentd:{v}"),
                Some((user, v)) => format!("{user}/environmentd:{v}"),
            }),
            environmentd_extra_args,
        };

        let req = self
            .build_region_request(
                Method::PATCH,
                ["api", "region"],
                None,
                &cloud_provider,
                Some(1),
            )
            .await?;
        let req = req.json(&body);
        // Creating a region can take some time
        let req = req.timeout(Duration::from_secs(60));
        self.send_request(req).await
    }

    /// Deletes a customer region in a particular cloud region for the current user.
    ///
    /// Soft deletes by default.
    ///
    /// NOTE that this operation is only available to Materialize employees
    /// This operation has a long duration, it can take
    /// several minutes to complete.
    pub async fn delete_region(
        &self,
        cloud_provider: CloudProvider,
        hard: bool,
    ) -> Result<(), Error> {
        /// A struct that deserializes nothing.
        ///
        /// Useful for deserializing empty response bodies.
        struct Empty;

        impl<'de> Deserialize<'de> for Empty {
            fn deserialize<D>(_: D) -> Result<Empty, D::Error>
            where
                D: Deserializer<'de>,
            {
                Ok(Empty)
            }
        }

        let query = if hard {
            Some([("hardDelete", "true")].as_slice())
        } else {
            None
        };

        let req = self
            .build_region_request(
                Method::DELETE,
                ["api", "region"],
                query,
                &cloud_provider,
                Some(1),
            )
            .await?;
        self.send_request::<Empty>(req).await?;

        // Wait for the environment to be fully deleted
        for _ in 0..600 {
            let req = self
                .build_region_request(
                    Method::GET,
                    ["api", "region"],
                    None,
                    &cloud_provider,
                    Some(1),
                )
                .await?;
            let res = self.send_request::<Region>(req).await;
            if hard {
                if let Err(Error::SuccesfullButNoContent) = res {
                    return Ok(());
                }
            } else {
                if let Ok(Region {
                    region_state: RegionState::SoftDeleted,
                    ..
                }) = res
                {
                    return Ok(());
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(Error::TimeoutError)
    }
}
