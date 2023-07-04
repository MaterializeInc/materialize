// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements Materialize cloud API methods
//! to GET, CREATE or DELETE an environment.
//! To delete an environment correctly make sure to
//! contact support.
//!
//! For a better experience retrieving all the available
//! environments, use [`Client::get_all_environments()`]

use std::time::Duration;

use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize};

use crate::client::cloud_provider::CloudProvider;
use crate::client::region::Region;
use crate::client::{Client, Error};

/// An environment is represented in this structure
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Environment {
    /// Represents the environmentd PG wire protocol address.
    ///
    /// E.g.: 3es24sg5rghjku7josdcs5jd7.eu-west-1.aws.materialize.cloud:6875
    pub environmentd_pgwire_address: String,
    /// Represents the environmentd PG wire protocol address.
    ///
    /// E.g.: 3es24sg5rghjku7josdcs5jd7.eu-west-1.aws.materialize.cloud:443
    pub environmentd_https_address: String,
    /// Indicates true if the address is resolvable by DNS.
    pub resolvable: bool,
}

impl Client {
    /// Get an environment in a partciular region for the current user.
    pub async fn get_environment(&self, region: Region) -> Result<Environment, Error> {
        // Send request to the subdomain
        let req = self
            .build_environment_request(Method::GET, ["api", "environment"], region)
            .await?;

        let environments: Vec<Environment> = self.send_request(req).await?;
        Ok(environments
            .get(0)
            .ok_or_else(|| Error::EmptyRegion)?
            .to_owned())
    }

    /// Get all the available environments for the current user.
    pub async fn get_all_environments(&self) -> Result<Vec<Environment>, Error> {
        let cloud_providers: Vec<CloudProvider> = self.list_cloud_providers().await?;
        let mut environments: Vec<Environment> = vec![];

        for cloud_provider in cloud_providers {
            // Skip regions with no environments
            match self.get_region(cloud_provider).await {
                Ok(region) => {
                    let environment = self.get_environment(region).await?;
                    environments.push(environment);
                }
                Err(Error::InvalidEnvironmentAssignment) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(environments)
    }

    /// Creates an environment in a particular region for the current user
    pub async fn create_environment(
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
                Method::POST,
                ["api", "environmentassignment"],
                cloud_provider,
            )
            .await?;
        let req = req.json(&body);
        self.send_request(req).await
    }

    /// Deletes an environment in a particular region for the current user.
    ///
    /// This operation has a long duration, it can take
    /// several minutes to complete.
    /// The first few requests will return a 504,
    /// indicating that the API is working on the deletion.
    /// A request returning a 202 indicates that
    /// no environment is available to delete (the delete request is complete.)
    pub async fn delete_environment(&self, cloud_provider: CloudProvider) -> Result<(), Error> {
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

        // We need to continuously try to delete the environment
        // until it succeeds (Status code: 202) or an unexpected error occurs.
        let mut loops = 0;
        loop {
            loops += 1;

            let req = self
                .build_region_request(
                    Method::DELETE,
                    ["api", "environmentassignment"],
                    cloud_provider.clone(),
                )
                .await?;

            // This timeout corresponds to the same in our cloud services tests.
            let req = req.timeout(Duration::from_secs(305));

            match self.send_request::<Empty>(req).await {
                Ok(_) => break Ok(()), // The request was successful, no environment is available to delete anymore.
                Err(Error::Api(err)) => {
                    if err.status_code != 504 {
                        // The error was not a timeout (status code 504), so it's unexpected and we should return it
                        return Err(Error::Api(err));
                    }
                    // If the error was a timeout, it means the API is still working on deleting the environment.
                }
                Err(Error::Transport(e)) => {
                    if !e.is_timeout() {
                        return Err(Error::Transport(e));
                    }
                }
                // The request failed with a non-API error, so we should return it
                Err(e) => return Err(e),
            }

            // Too many requests/timeouts were reached.
            if loops == 10 {
                return Err(Error::TimeoutError);
            }
        }
    }
}
