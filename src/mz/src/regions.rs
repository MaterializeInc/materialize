// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::utils::CloudProviderRegion;
use crate::{
    CloudProvider, CloudProviderAndRegion, Environment, Region, ValidProfile, CLOUD_PROVIDERS_URL,
};
use anyhow::{ensure, Context, Result};

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Error};

/// Build the headers for reqwest request with the frontegg authorization.
fn build_region_request_headers(authorization: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(AUTHORIZATION, HeaderValue::from_str(authorization).unwrap());

    headers
}

/// Enables a particular cloud provider's region
pub(crate) async fn enable_region_environment(
    client: &Client,
    cloud_provider: &CloudProvider,
    valid_profile: &ValidProfile,
) -> Result<Region, reqwest::Error> {
    let authorization: String = format!(
        "Bearer {}",
        valid_profile.frontegg_auth_machine.access_token
    );

    let headers = build_region_request_headers(&authorization);
    let body: HashMap<char, char> = HashMap::new();

    client
        .post(
            format!(
                "{:}/api/environmentassignment",
                cloud_provider.region_controller_url
            )
            .as_str(),
        )
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<Region>()
        .await
}

//// Get a cloud provider's regions
pub(crate) async fn get_cloud_provider_region_details(
    client: &Client,
    cloud_provider_region: &CloudProvider,
    valid_profile: &ValidProfile,
) -> Result<Vec<Region>, anyhow::Error> {
    let authorization: String = format!(
        "Bearer {}",
        valid_profile.frontegg_auth_machine.access_token
    );
    let headers = build_region_request_headers(&authorization);
    let mut region_api_url = cloud_provider_region.region_controller_url.clone();
    region_api_url.push_str("/api/environmentassignment");

    let response = client.get(region_api_url).headers(headers).send().await?;
    ensure!(response.status().is_success());
    Ok(response.json::<Vec<Region>>().await?)
}

//// Get a cloud provider's region's environment
pub(crate) async fn region_environment_details(
    client: &Client,
    region: &Region,
    valid_profile: &ValidProfile,
) -> Result<Option<Vec<Environment>>, Error> {
    let authorization: String = format!(
        "Bearer {}",
        valid_profile.frontegg_auth_machine.access_token
    );
    let headers = build_region_request_headers(authorization.as_str());
    let mut region_api_url = region.environment_controller_url
        [0..region.environment_controller_url.len() - 4]
        .to_string();
    region_api_url.push_str("/api/environment");

    let response = client.get(region_api_url).headers(headers).send().await?;
    match response.content_length() {
        Some(length) => {
            if length > 0 {
                Ok(Some(response.json::<Vec<Environment>>().await?))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

/// List all the available regions for a list of cloud providers.
pub(crate) async fn list_regions(
    cloud_providers: &Vec<CloudProvider>,
    client: &Client,
    valid_profile: &ValidProfile,
) -> Result<Vec<CloudProviderAndRegion>> {
    // TODO: Run requests in parallel
    let mut cloud_providers_and_regions: Vec<CloudProviderAndRegion> = Vec::new();

    for cloud_provider in cloud_providers {
        let cloud_provider_region_details =
            get_cloud_provider_region_details(client, cloud_provider, valid_profile)
                .await
                .with_context(|| "Retrieving region details.")?;
        match cloud_provider_region_details.get(0) {
            Some(region) => cloud_providers_and_regions.push(CloudProviderAndRegion {
                cloud_provider: cloud_provider.clone(),
                region: Some(region.to_owned()),
            }),
            None => cloud_providers_and_regions.push(CloudProviderAndRegion {
                cloud_provider: cloud_provider.clone(),
                region: None,
            }),
        }
    }

    Ok(cloud_providers_and_regions)
}

/// List all the available cloud providers.
///
/// E.g.: [us-east-1, eu-west-1]
pub(crate) async fn list_cloud_providers(
    client: &Client,
    valid_profile: &ValidProfile,
) -> Result<Vec<CloudProvider>, Error> {
    let authorization: String = format!(
        "Bearer {}",
        valid_profile.frontegg_auth_machine.access_token
    );

    let headers = build_region_request_headers(&authorization);

    client
        .get(CLOUD_PROVIDERS_URL)
        .headers(headers)
        .send()
        .await?
        .json::<Vec<CloudProvider>>()
        .await
}

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
/// Healthy:         {yes/no}
/// SQL address:     foo.materialize.cloud:6875
/// HTTPS address:   <https://foo.materialize.cloud>
pub(crate) fn print_environment_status(environment: Environment, health: bool) {
    if health {
        println!("Healthy:\tyes");
    } else {
        println!("Healthy:\tno");
    }
    println!(
        "SQL address: \t{}",
        &environment.environmentd_pgwire_address
            [0..environment.environmentd_pgwire_address.len() - 5]
    );
    // Remove port from url
    println!(
        "HTTPS address: \thttps://{}",
        &environment.environmentd_https_address
            [0..environment.environmentd_https_address.len() - 4]
    );
}

pub(crate) async fn get_provider_by_region_name(
    client: &Client,
    valid_profile: &ValidProfile,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<CloudProvider> {
    let cloud_providers = list_cloud_providers(client, valid_profile)
        .await
        .with_context(|| "Retrieving cloud providers.")?;

    // Create a vec with only one region
    let cloud_provider: CloudProvider = cloud_providers
        .into_iter()
        .find(|provider| provider.region == cloud_provider_region.region_name())
        .with_context(|| "Retriving cloud provider from list.")?;

    Ok(cloud_provider)
}

pub(crate) async fn get_provider_region(
    client: &Client,
    valid_profile: &ValidProfile,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<Region> {
    let cloud_provider = get_provider_by_region_name(client, valid_profile, cloud_provider_region)
        .await
        .with_context(|| "Retrieving cloud provider.")?;

    let cloud_provider_region_details =
        get_cloud_provider_region_details(client, &cloud_provider, valid_profile)
            .await
            .with_context(|| "Retrieving region details.")?;

    let region = cloud_provider_region_details
        .get(0)
        .with_context(|| "Region unavailable")?;

    Ok(region.to_owned())
}

pub(crate) async fn get_region_environment(
    client: &Client,
    valid_profile: &ValidProfile,
    region: &Region,
) -> Result<Environment> {
    let environment_details = region_environment_details(client, region, valid_profile)
        .await
        .with_context(|| "Environment unavailable")?;
    let environment_list = environment_details.with_context(|| "Environment unlisted")?;
    let environment = environment_list
        .get(0)
        .with_context(|| "Missing environment")?;

    Ok(environment.to_owned())
}

pub(crate) async fn get_provider_region_environment(
    client: &Client,
    valid_profile: &ValidProfile,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<Environment> {
    let region = get_provider_region(client, valid_profile, cloud_provider_region)
        .await
        .with_context(|| "Retrieving region data.")?;

    let environment = get_region_environment(client, valid_profile, &region)
        .await
        .with_context(|| "Retrieving environment data")?;

    Ok(environment)
}
