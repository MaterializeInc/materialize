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

use mz_cloud_api::client::{cloud_provider::CloudProvider, environment::Environment};
use serde::{Deserialize, Serialize};
use tabled::Tabled;
use tokio::time::{sleep};

/// Enable a region in the profile organization.
///
/// In cases where the organization has already enabled the region
/// the command will try to run a version update. Resulting
/// in a downtime for a short period.
pub async fn enable(cx: RegionContext) -> Result<(), Error> {
    let loading_spinner = cx
        .output_formatter()
        .loading_spinner("Retrieving information...");
    let cloud_provider = cx.get_cloud_provider().await?;

    loading_spinner.set_message("Enabling the region...");
    cx.cloud_client()
        .create_environment(None, vec![], cloud_provider.clone())
        .await?;

    loading_spinner.set_message("Waiting for the region to be online...");
    let region = cx.get_region().await?;

    let mut tries = 0;

    let environment: Environment = loop {
        tries += 1;
        match cx.get_environment(region.clone()).await {
            Ok(environment) => {
                if environment.resolvable {
                    break Ok(environment)
                }
            },
            Err(e) => {
                if tries == 10 {
                    break Err(e);
                }
            },
        }
        sleep(Duration::from_secs(10)).await;
    }?;

    loading_spinner.set_message("Waiting for the region to be ready...");
    loop {
        if cx
            .sql_client()
            .is_ready(&environment, cx.admin_client().claims().await?.email)?
        {
            break;
        }
    }
    loading_spinner.finish_with_message(format!("Region in {} is now online", cloud_provider.id));

    Ok(())
}

/// Disable a region in the profile organization.
///
/// This command can take several minutes to complete.
pub async fn disable(cx: RegionContext) -> Result<(), Error> {
    let loading_spinner = cx
        .output_formatter()
        .loading_spinner("Retrieving information...");
    let cloud_provider = cx.get_cloud_provider().await?;

    loading_spinner.set_message("Disabling region...");
    cx.cloud_client()
        .delete_environment(cloud_provider.clone())
        .await?;

    loading_spinner.finish_with_message("Region disabled.");

    Ok(())
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

    let cloud_providers: Vec<CloudProvider> = cx.cloud_client().list_cloud_providers().await?;
    let mut regions: Vec<Region> = vec![];

    for cloud_provider in cloud_providers {
        match cx.cloud_client().get_region(cloud_provider.clone()).await {
            Ok(_) => regions.push(Region {
                region: cloud_provider.id,
                status: "enabled",
            }),
            Err(mz_cloud_api::error::Error::InvalidEnvironmentAssignment) => regions.push(Region {
                region: cloud_provider.id,
                status: "disabled",
            }),
            // TODO: Handle error
            Err(_) => {}
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

    let region = cx.get_region().await?;

    loading_spinner.set_message("Checking environment health...");
    let environment = cx.get_environment(region.clone()).await?;
    let claims = cx.admin_client().claims().await?;
    let sql_client = cx.sql_client();
    let environment_health = match sql_client.is_ready(&environment, claims.email) {
        Ok(healthy) => match healthy {
            true => "yes",
            _ => "no",
        },
        Err(_) => "no",
    };

    loading_spinner.finish_and_clear();
    output_formatter.output_scalar(Some(&format!("Healthy: \t{}", environment_health)))?;
    output_formatter.output_scalar(Some(&format!(
        "SQL address: \t{}",
        environment.environmentd_pgwire_address
    )))?;
    output_formatter.output_scalar(Some(&format!(
        "HTTP URL: \t{}",
        environment.environmentd_https_address
    )))?;

    Ok(())
}
