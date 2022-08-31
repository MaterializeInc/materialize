// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::exit_with_fail_message;
use crate::{
    CloudProvider, CloudProviderAndRegion, CloudProviderRegion, ExitMessage, FronteggAuthMachine,
    Region, CLOUD_PROVIDERS_URL,
};

use std::collections::HashMap;

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Error};

/// ----------------------------
///  Regions commands
/// ----------------------------

/// Format cloud provider region url to interact with.
///
/// TODO: ec.0 is dynamic.
fn format_region_url(cloud_provider_region: CloudProviderRegion) -> String {
    format!(
        "https://ec.0.{}.aws.cloud.materialize.com/api/environment",
        cloud_provider_region.region_name()
    )
}

/// Build the headers for reqwest request with the frontegg authorization.
fn build_region_request_headers(authorization: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(AUTHORIZATION, HeaderValue::from_str(authorization).unwrap());

    headers
}

/// Enables a particular cloud provider's region
pub(crate) async fn enable_region(
    client: Client,
    cloud_provider_region: CloudProviderRegion,
    frontegg_auth_machine: FronteggAuthMachine,
) -> Result<Region, reqwest::Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);
    let region_url: String = format_region_url(cloud_provider_region);

    let headers = build_region_request_headers(&authorization);
    let mut body = HashMap::new();
    body.insert("environmentd_image_ref", &"materialize/environmentd:latest");

    client
        .post(region_url)
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<Region>()
        .await
}

//// Get a cloud provider's regions
pub(crate) async fn cloud_provider_region_details(
    client: &Client,
    cloud_provider_region: &CloudProvider,
    frontegg_auth_machine: &FronteggAuthMachine,
) -> Result<Option<Vec<Region>>, Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);
    let headers = build_region_request_headers(&authorization);
    let mut region_api_url = cloud_provider_region.environment_controller_url.clone();
    region_api_url.push_str("/api/environment");

    let response = client.get(region_api_url).headers(headers).send().await?;

    match response.content_length() {
        Some(length) => {
            if length > 0 {
                Ok(Some(response.json::<Vec<Region>>().await?))
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
    frontegg_auth_machine: &FronteggAuthMachine,
) -> Vec<CloudProviderAndRegion> {
    // TODO: Run requests in parallel
    let mut cloud_providers_and_regions: Vec<CloudProviderAndRegion> = Vec::new();

    for cloud_provider in cloud_providers {
        match cloud_provider_region_details(client, cloud_provider, frontegg_auth_machine).await {
            Ok(Some(mut region)) => match region.pop() {
                Some(region) => cloud_providers_and_regions.push(CloudProviderAndRegion {
                    cloud_provider: cloud_provider.clone(),
                    region: Some(region),
                }),
                None => cloud_providers_and_regions.push(CloudProviderAndRegion {
                    cloud_provider: cloud_provider.clone(),
                    region: None,
                }),
            },
            Err(error) => {
                exit_with_fail_message(ExitMessage::String(format!(
                    "Error retrieving region details: {:?}",
                    error
                )));
            }
            _ => {}
        }
    }

    cloud_providers_and_regions
}

/// List all the available cloud providers.
///
/// E.g.: [us-east-1, eu-west-1]
pub(crate) async fn list_cloud_providers(
    client: &Client,
    frontegg_auth_machine: &FronteggAuthMachine,
) -> Result<Vec<CloudProvider>, Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);

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
/// Prints a region's status and addresses
///
/// Healthy:         {yes/no}
/// SQL address:     foo.materialize.cloud:6875
/// HTTPS address:   <https://foo.materialize.cloud>
pub(crate) fn print_region_status(region: Region, health: bool) {
    if health {
        println!("Healthy:\tyes");
    } else {
        println!("Healthy:\tno");
    }
    println!("SQL address: \t{}", region.environmentd_pgwire_address);
    // Remove port from url
    println!(
        "HTTPS address: \thttps://{}",
        &region.environmentd_https_address[0..region.environmentd_https_address.len() - 4]
    );
}
