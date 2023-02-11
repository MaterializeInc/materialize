// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, Result};
use url::Url;

use mz::api::{CloudProviderAndRegion, Environment};
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
            cloud_provider.provider, cloud_provider.region
        ),
        None => println!(
            "{:}/{:}  disabled",
            cloud_provider.provider, cloud_provider.region
        ),
    };
}

///
/// Prints an environment's status and addresses
///
/// Healthy:                {yes/no}
/// SQL address:            foo.materialize.cloud:6875
/// HTTPS address:          <https://foo.materialize.cloud>
/// Connection string:      postgres://{user}@{address}/materialize?sslmode=require
pub(crate) fn print_environment_status(
    valid_profile: &ValidProfile,
    environment: Environment,
    health: bool,
) -> Result<()> {
    if health {
        println!("Healthy:\t\tyes");
    } else {
        println!("Healthy:\t\tno");
    }
    println!(
        "SQL address: \t\t{}",
        &environment.environmentd_pgwire_address
            [0..environment.environmentd_pgwire_address.len() - 5]
    );

    println!(
        "HTTPS address: \t\thttps://{}",
        &environment.environmentd_https_address
            [0..environment.environmentd_https_address.len() - 4]
    );

    let mut url = Url::parse(&format!(
        "postgres://{}",
        &environment.environmentd_pgwire_address
    ))
    .with_context(|| "Parsing URL.")?;

    url.set_username(valid_profile.profile.get_email()).unwrap();
    url.set_path("materialize");
    url.set_query(Some("sslmode=require"));

    println!("Connection string: \t{}", url.as_str());

    Ok(())
}
