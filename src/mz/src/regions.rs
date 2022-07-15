/// ----------------------------
///  Regions commands
/// ----------------------------

use crate::{CLOUD_PROVIDERS_URL, CloudProvider, CloudProviderRegion, FronteggAuthMachine, Region};
use crate::utils:: {trim_newline};

use std::{io::{Write}, collections::HashMap};

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT, AUTHORIZATION};
use reqwest::{Client, Error, Response};

fn parse_cloud_provider_region(cloud_provider_region: CloudProviderRegion) -> String {
    match cloud_provider_region {
        CloudProviderRegion::usEast_1 => "us-east-1".to_string(),
        CloudProviderRegion::euWest_1 => "eu-west-1".to_string()
    }
}

// TODO: ec.0 is dynamic.
fn format_region_url(cloud_provider_region: CloudProviderRegion) -> String {
    format!("https://ec.0.{}.aws.staging.cloud.materialize.com/api/environment", parse_cloud_provider_region(cloud_provider_region))
}

fn build_region_request_headers(authorization: String) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(AUTHORIZATION, HeaderValue::from_str(authorization.as_str()).unwrap());

    headers
}

pub(crate) async fn enable_region(client: Client, cloud_provider_region: CloudProviderRegion, frontegg_auth_machine: FronteggAuthMachine) -> Result<Region, reqwest::Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);
    let region_url: String = format_region_url(cloud_provider_region);

    let headers = build_region_request_headers(authorization);
    let mut body = HashMap::new();
    // TODO: Version is dynamic
    body.insert("coordd_image_ref", &"materialize/environmentd:unstable-45e2acde087c27a661b5e67db587375c0b628fde");

    client.post(region_url)
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<Region>()
        .await
}

pub(crate) async fn warning_delete_region(cloud_provider_region: CloudProviderRegion) -> bool {
    let region = parse_cloud_provider_region(cloud_provider_region);

    println!();
    println!("**** WARNING ****");
    println!("Are you sure? Deleting a region is irreversible.");
    println!("Enter {:?} to proceed:", region);

    // Handle user input
    let mut region_input = String::new();
    let _ = std::io::stdout().flush();

    match std::io::stdin().read_line(&mut region_input) {
        Ok(_) => {
            trim_newline(&mut region_input);
            if region_input == region {
                true
            } else {
                println!("The region's name doesn't match.");
                false
            }
        }
        Err(error) => panic!("Problem parsing the region input: {:?}", error)
    }
}

pub(crate) async fn delete_region(client: Client, cloud_provider_region: CloudProviderRegion, frontegg_auth_machine: FronteggAuthMachine) -> Result<Response, Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);
    let region_url: String = format_region_url(cloud_provider_region);

    let headers = build_region_request_headers(authorization);

    client.delete(region_url)
        .headers(headers)
        .send()
        .await
}

pub(crate) async fn cloud_provider_region_details(
    client: Client,
    cloud_provider_region: CloudProvider,
    frontegg_auth_machine: FronteggAuthMachine
) -> Result<Option<Vec<Region>>, Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);
    let headers = build_region_request_headers(authorization);
    let mut regionApiUrl = cloud_provider_region.environment_controller_url.clone();
    regionApiUrl.push_str("/api/environment");

    let response = client.get(regionApiUrl)
        .headers(headers)
        .send()
        .await?;

    match response.content_length() {
        Some(length) => {
            if length > 0 {
                return Ok(Some(response.json::<Vec<Region>>().await?))
            } else {
                Ok(None)
            }
        }
        None => Ok(None)
    }
}

pub(crate) async fn list_regions(cloud_providers: Vec<CloudProvider>, client: Client, frontegg_auth_machine: FronteggAuthMachine) -> Vec<Region> {
    // TODO: Run request in parallel
    let mut regions: Vec<Region> = Vec::new();

    // TODO: Use iterators
    for cloud_provider in  cloud_providers {
        match cloud_provider_region_details(
            client.clone(),
            cloud_provider,
            frontegg_auth_machine.clone()
        ).await {
            Ok(Some(mut region)) => {
                match region.pop() {
                    Some(region) => regions.push(region),
                    None => {}
                }
            }
            Err(error) => { panic!("Error retrieving region details: {:?}", error); }
            _ => {}
        }
    }

    regions
}

pub(crate) async fn list_cloud_providers(client: Client, frontegg_auth_machine: FronteggAuthMachine) -> Result<Vec<CloudProvider>, Error> {
    let authorization: String = format!("Bearer {}", frontegg_auth_machine.access_token);

    let headers = build_region_request_headers(authorization);

    client.get(CLOUD_PROVIDERS_URL)
        .headers(headers)
        .send()
        .await?
        .json::<Vec<CloudProvider>>()
        .await
}
