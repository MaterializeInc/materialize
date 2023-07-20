// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use mz::api::{CloudProviderAndRegion, Region};
use mz::configuration::ValidProfile;

/// Prints if a region is enabled or not
///
/// E.g.: AWS/us-east-1  enabled
pub(crate) fn print_region_enabled(cloud_provider_and_region: &CloudProviderAndRegion) {
    let region = &cloud_provider_and_region.region;
    let cloud_provider = &cloud_provider_and_region.cloud_provider;

    match region {
        Some(_) => println!(
            "{:}/{:}  enabled",
            cloud_provider.cloud_provider, cloud_provider.name
        ),
        None => println!(
            "{:}/{:}  disabled",
            cloud_provider.cloud_provider, cloud_provider.name
        ),
    };
}

///
/// Prints a region status and addresses
///
/// Healthy:                {yes/no}
/// SQL address:            foo.materialize.cloud:6875
/// HTTPS address:          <https://foo.materialize.cloud>
/// Connection string:      postgres://{user}@{address}/materialize?sslmode=require
pub(crate) fn print_region_status(
    valid_profile: &ValidProfile,
    region: Region,
    health: bool,
) -> Result<()> {
    if health {
        println!("Healthy:\t\tyes");
    } else {
        println!("Healthy:\t\tno");
    }

    println!(
        "HTTPS address: \t\thttps://{}",
        &region.region_info.http_address[0..region.region_info.http_address.len() - 4]
    );
    println!("SQL connection string: \t{}", region.sql_url(valid_profile));

    Ok(())
}
