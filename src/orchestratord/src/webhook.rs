// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use axum::routing::{get, post};
use axum::{Json, Router};
use http::StatusCode;
use kube::core::Status;
use kube::core::conversion::{ConversionRequest, ConversionResponse, ConversionReview};
use kube::core::response::reason;

use mz_cloud_resources::crd::materialize::{v1alpha1, v1alpha2};
use tracing::warn;

pub fn router() -> Router {
    Router::new()
        .route("/convert", post(post_convert))
        .route("/healthz", get(get_health))
}

#[derive(Clone, Copy)]
enum SupportedVersion {
    V1alpha1,
    V1alpha2,
}

impl TryFrom<&str> for SupportedVersion {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "materialize.cloud/v1alpha1" => Ok(SupportedVersion::V1alpha1),
            "materialize.cloud/v1alpha2" => Ok(SupportedVersion::V1alpha2),
            _ => Err(anyhow!("unexpected version: {}", value)),
        }
    }
}

fn convert(
    desired_version: SupportedVersion,
    value: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let from_version = SupportedVersion::try_from(
        value
            .get("apiVersion")
            .and_then(|version| version.as_str())
            .ok_or_else(|| anyhow!("missing version"))?,
    )?;
    match (from_version, desired_version) {
        (SupportedVersion::V1alpha1, SupportedVersion::V1alpha1) => Ok(value),
        (SupportedVersion::V1alpha1, SupportedVersion::V1alpha2) => serde_json::from_value::<
            v1alpha1::Materialize,
        >(value)
        .and_then(|mz_v1alpha1| serde_json::to_value(v1alpha2::Materialize::from(mz_v1alpha1)))
        .map_err(|e| e.into()),
        (SupportedVersion::V1alpha2, SupportedVersion::V1alpha1) => serde_json::from_value::<
            v1alpha2::Materialize,
        >(value)
        .and_then(|mz_v1alpha2| serde_json::to_value(v1alpha1::Materialize::from(mz_v1alpha2)))
        .map_err(|e| e.into()),
        (SupportedVersion::V1alpha2, SupportedVersion::V1alpha2) => Ok(value),
    }
}

async fn post_convert(
    Json(conversion_review): Json<ConversionReview>,
) -> (StatusCode, Json<ConversionReview>) {
    let Ok(request) = ConversionRequest::from_review(conversion_review) else {
        warn!("missing request");
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                ConversionResponse::invalid(Status::failure("missing request", reason::INVALID))
                    .into_review(),
            ),
        );
    };

    let desired_version = match SupportedVersion::try_from(request.desired_api_version.as_str()) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    ConversionResponse::for_request(request)
                        .failure(Status::failure(&e.to_string(), reason::BAD_REQUEST))
                        .into_review(),
                ),
            );
        }
    };

    let converted_objects: Result<Vec<serde_json::Value>, anyhow::Error> = request
        .objects
        .iter()
        .cloned()
        .map(|value| convert(desired_version, value))
        .collect();
    match converted_objects {
        Ok(converted_objects) => (
            StatusCode::OK,
            Json(
                ConversionResponse::for_request(request)
                    .success(converted_objects)
                    .into_review(),
            ),
        ),
        Err(e) => {
            warn!("error when converting: {:?}\n{:?}", &e, request.objects);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    ConversionResponse::for_request(request)
                        .failure(Status::failure(&e.to_string(), reason::UNKNOWN))
                        .into_review(),
                ),
            )
        }
    }
}

async fn get_health() -> StatusCode {
    StatusCode::OK
}
