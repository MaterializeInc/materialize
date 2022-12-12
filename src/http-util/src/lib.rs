// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP utilities.

use askama::Template;
use axum::http::status::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::Json;
use axum::TypedHeader;
use headers::ContentType;
use mz_ore::metrics::MetricsRegistry;
use prometheus::Encoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing_subscriber::filter::Targets;

/// Renders a template into an HTTP response.
pub fn template_response<T>(template: T) -> Html<String>
where
    T: Template,
{
    Html(template.render().expect("template rendering cannot fail"))
}

#[macro_export]
/// Generates a `handle_static` function that serves static content for HTTP servers.
/// Expects three arguments: an `include_dir::Dir` object where the static content is served,
/// and two strings representing the (crate-local) paths to the production and development
/// static files.
macro_rules! make_handle_static {
    ($static_dir:expr, $prod_base_path:expr, $dev_base_path:expr) => {
        #[allow(clippy::unused_async)]
        pub async fn handle_static(
            path: ::axum::extract::Path<String>,
        ) -> impl ::axum::response::IntoResponse {
            #[cfg(not(feature = "dev-web"))]
            const STATIC_DIR: ::include_dir::Dir = $static_dir;

            #[cfg(not(feature = "dev-web"))]
            fn get_static_file(path: &str) -> Option<&'static [u8]> {
                STATIC_DIR.get_file(path).map(|f| f.contents())
            }

            #[cfg(feature = "dev-web")]
            fn get_static_file(path: &str) -> Option<Vec<u8>> {
                use ::std::fs;

                #[cfg(not(debug_assertions))]
                compile_error!("cannot enable insecure `dev-web` feature in release mode");

                // Prefer the unminified files in static-dev, if they exist.
                let dev_path =
                    format!("{}/{}/{}", env!("CARGO_MANIFEST_DIR"), $dev_base_path, path);
                let prod_path = format!(
                    "{}/{}/{}",
                    env!("CARGO_MANIFEST_DIR"),
                    $prod_base_path,
                    path
                );
                match fs::read(dev_path).or_else(|_| fs::read(prod_path)) {
                    Ok(contents) => Some(contents),
                    Err(e) => {
                        ::tracing::debug!("dev-web failed to load static file: {}: {}", path, e);
                        None
                    }
                }
            }
            let path = path.strip_prefix('/').unwrap_or(&path);
            let content_type = match ::std::path::Path::new(path)
                .extension()
                .and_then(|e| e.to_str())
            {
                Some("js") => Some(::axum::TypedHeader(::headers::ContentType::from(
                    ::mime::TEXT_JAVASCRIPT,
                ))),
                Some("css") => Some(::axum::TypedHeader(::headers::ContentType::from(
                    ::mime::TEXT_CSS,
                ))),
                None | Some(_) => None,
            };
            match get_static_file(path) {
                Some(body) => Ok((content_type, body)),
                None => Err((::http::StatusCode::NOT_FOUND, "not found")),
            }
        }
    };
}

/// Serves a basic liveness check response
#[allow(clippy::unused_async)]
pub async fn handle_liveness_check() -> impl IntoResponse {
    (StatusCode::OK, "Liveness check successful!")
}

/// Serves metrics from the selected metrics registry variant.
#[allow(clippy::unused_async)]
pub async fn handle_prometheus(registry: &MetricsRegistry) -> impl IntoResponse {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    encoder
        .encode(&registry.gather(), &mut buffer)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok::<_, (StatusCode, String)>((TypedHeader(ContentType::text()), buffer))
}

#[derive(Serialize, Deserialize)]
pub struct DynamicFilterTarget {
    targets: String,
}

/// Allows dynamic control of the stderr log filter
#[allow(clippy::unused_async)]
pub async fn handle_modify_filter_target(
    callback: mz_ore::tracing::DynamicTargetsCallback,
    Json(cfg): Json<DynamicFilterTarget>,
) -> impl IntoResponse {
    match cfg.targets.parse::<Targets>() {
        Ok(targets) => match callback.call(targets) {
            Ok(()) => (StatusCode::OK, cfg.targets.to_string()),
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
        },
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
    }
}

/// Returns information about the current status of tracing.
#[allow(clippy::unused_async)]
pub async fn handle_tracing() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "current_level_filter": tracing::level_filters::LevelFilter::current().to_string()
        })),
    )
}
