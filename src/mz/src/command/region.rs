// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementation of the `mz region` command.
//!
//! Consult the user-facing documentation for details.
//!
use std::time::Duration;

use crate::{context::RegionContext, error::Error};

use mz_cloud_api::client::{cloud_provider::CloudProvider, region::RegionState};
use mz_ore::retry::Retry;
use serde::{Deserialize, Serialize};
use tabled::Tabled;

/// Enable a region in the profile organization.
///
/// In cases where the organization has already enabled the region
/// the command will try to run a version update. Resulting
/// in a downtime for a short period.
pub async fn enable(
    cx: RegionContext,
    version: Option<String>,
    environmentd_extra_arg: Option<Vec<String>>,
    environmentd_cpu_allocation: Option<String>,
    environmentd_memory_allocation: Option<String>,
) -> Result<(), Error> {
    let loading_spinner = cx
        .output_formatter()
        .loading_spinner("Retrieving information...");
    let cloud_provider = cx.get_cloud_provider().await?;

    loading_spinner.set_message("Enabling the region...");

    let environmentd_extra_arg: Vec<String> = environmentd_extra_arg.unwrap_or_else(Vec::new);

    // Loop region creation.
    // After 6 minutes it will timeout.
    let _ = Retry::default()
        .max_duration(Duration::from_secs(720))
        .clamp_backoff(Duration::from_secs(1))
        .retry_async(|_| async {
            let _ = cx
                .cloud_client()
                .create_region(
                    version.clone(),
                    environmentd_extra_arg.clone(),
                    environmentd_cpu_allocation.clone(),
                    environmentd_memory_allocation.clone(),
                    cloud_provider.clone(),
                )
                .await?;
            Ok(())
        })
        .await
        .map_err(|e| Error::TimeoutError(Box::new(e)))?;

    loading_spinner.set_message("Waiting for the region to be online...");

    // Loop retrieving the region and checking the SQL connection for 6 minutes.
    // After 6 minutes it will timeout.
    let _ = Retry::default()
        .max_duration(Duration::from_secs(720))
        .clamp_backoff(Duration::from_secs(1))
        .retry_async(|_| async {
            let region = cx.get_region().await?;

            match region.region_state {
                RegionState::EnablementPending => {
                    loading_spinner.set_message("Waiting for the region to be ready...");
                    Err(Error::NotReadyRegion)
                }
                RegionState::DeletionPending => Err(Error::CommandExecutionError(
                    "This region is pending deletion!".to_string(),
                )),
                RegionState::SoftDeleted => Err(Error::CommandExecutionError(
                    "This region has been marked soft-deleted!".to_string(),
                )),
                RegionState::Enabled => match region.region_info {
                    Some(region_info) => {
                        loading_spinner.set_message("Waiting for the region to be resolvable...");
                        if region_info.resolvable {
                            let claims = cx.admin_client().claims().await?;
                            let user = claims.user()?;
                            if cx.sql_client().is_ready(&region_info, user)? {
                                return Ok(());
                            }
                            Err(Error::NotPgReadyError)
                        } else {
                            Err(Error::NotResolvableRegion)
                        }
                    }
                    None => Err(Error::NotReadyRegion),
                },
            }
        })
        .await
        .map_err(|e| Error::TimeoutError(Box::new(e)))?;

    loading_spinner.finish_with_message(format!("Region in {} is now online", cloud_provider.id));

    Ok(())
}

/// Disable a region in the profile organization.
///
/// This command can take several minutes to complete.
pub async fn disable(cx: RegionContext, hard: bool) -> Result<(), Error> {
    let loading_spinner = cx
        .output_formatter()
        .loading_spinner("Retrieving information...");

    let cloud_provider = cx.get_cloud_provider().await?;

    // The `delete_region` method retries disabling a region,
    // has an inner timeout, and manages a `504` response.
    // For any other type of error response, we handle it here
    // with a retry loop.
    Retry::default()
        .max_duration(Duration::from_secs(720))
        .clamp_backoff(Duration::from_secs(1))
        .retry_async(|_| async {
            loading_spinner.set_message("Disabling region...");
            cx.cloud_client()
                .delete_region(cloud_provider.clone(), hard)
                .await?;

            loading_spinner.finish_with_message("Region disabled.");
            Ok(())
        })
        .await
}

/// Lists all the available regions and their status.
pub async fn list(cx: RegionContext) -> Result<(), Error> {
    let output_formatter = cx.output_formatter();
    let loading_spinner = output_formatter.loading_spinner("Retrieving regions...");

    #[derive(Deserialize, Serialize, Tabled)]
    pub struct Region<'a> {
        #[tabled(rename = "Region")]
        region: String,
        #[tabled(rename = "Status")]
        status: &'a str,
    }

    let cloud_providers: Vec<CloudProvider> = cx.cloud_client().list_cloud_regions().await?;
    let mut regions: Vec<Region> = vec![];

    for cloud_provider in cloud_providers {
        match cx.cloud_client().get_region(cloud_provider.clone()).await {
            Ok(_) => regions.push(Region {
                region: cloud_provider.id,
                status: "enabled",
            }),
            Err(mz_cloud_api::error::Error::EmptyRegion) => regions.push(Region {
                region: cloud_provider.id,
                status: "disabled",
            }),
            Err(err) => {
                println!("Error: {:?}", err)
            }
        }
    }

    loading_spinner.finish_and_clear();
    output_formatter.output_table(regions)?;
    Ok(())
}

/// Shows the health of the profile region followed by the HTTP and SQL endpoints.
pub async fn show(cx: RegionContext) -> Result<(), Error> {
    // Sharing the reference of the context in multiple places makes
    // it necesarry to wrap in an `alloc::rc`.

    let output_formatter = cx.output_formatter();
    let loading_spinner = output_formatter.loading_spinner("Retrieving region...");

    let region_info = cx.get_region_info().await?;

    loading_spinner.set_message("Checking environment health...");
    let claims = cx.admin_client().claims().await?;
    let sql_client = cx.sql_client();
    let environment_health = match sql_client.is_ready(&region_info, claims.user()?) {
        Ok(healthy) => match healthy {
            true => "yes",
            _ => "no",
        },
        Err(_) => "no",
    };

    loading_spinner.finish_and_clear();
    output_formatter.output_scalar(Some(&format!("Healthy: \t{}", environment_health)))?;
    output_formatter.output_scalar(Some(&format!("SQL address: \t{}", region_info.sql_address)))?;
    output_formatter.output_scalar(Some(&format!("HTTP URL: \t{}", region_info.http_address)))?;

    Ok(())
}
