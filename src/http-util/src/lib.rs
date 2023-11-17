// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

//! HTTP utilities.

use askama::Template;
use axum::http::status::StatusCode;
use axum::http::HeaderValue;
use axum::response::{Html, IntoResponse};
use axum::{Json, TypedHeader};
use headers::ContentType;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use prometheus::Encoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::AllowOrigin;
use tracing_subscriber::EnvFilter;

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

/// Dynamically reloads a filter for a tracing layer.
#[allow(clippy::unused_async)]
pub async fn handle_reload_tracing_filter(
    handle: &TracingHandle,
    reload: fn(&TracingHandle, EnvFilter) -> Result<(), anyhow::Error>,
    Json(cfg): Json<DynamicFilterTarget>,
) -> impl IntoResponse {
    match cfg.targets.parse::<EnvFilter>() {
        Ok(targets) => match reload(handle, targets) {
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

/// Construct a CORS policy to allow origins to query us via HTTP. If any bare
/// '*' is passed, this allows any origin; otherwise, allows a list of origins,
/// which can include wildcard subdomains. If the allowed origin starts with a
/// '*', allow anything from that glob. Otherwise check for an exact match.
pub fn build_cors_allowed_origin<'a, I>(allowed: I) -> AllowOrigin
where
    I: IntoIterator<Item = &'a HeaderValue>,
{
    let allowed = allowed.into_iter().cloned().collect::<Vec<HeaderValue>>();
    if allowed.iter().any(|o| o.as_bytes() == b"*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::predicate(move |origin: &HeaderValue, _request_parts: _| {
            for val in &allowed {
                if (val.as_bytes().starts_with(b"*.")
                    && origin.as_bytes().ends_with(&val.as_bytes()[1..]))
                    || origin == val
                {
                    return true;
                }
            }
            false
        })
    }
}

#[cfg(test)]
mod tests {
    use http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN};
    use http::{HeaderValue, Method, Request, Response};
    use hyper::Body;
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::cors::CorsLayer;

    #[mz_ore::test(tokio::test)]
    async fn test_cors() {
        async fn test_request(cors: &CorsLayer, origin: &HeaderValue) -> Option<HeaderValue> {
            let mut service = ServiceBuilder::new()
                .layer(cors)
                .service_fn(|_| async { Ok::<_, anyhow::Error>(Response::new(Body::empty())) });
            let request = Request::builder()
                .header(ORIGIN, origin)
                .body(Body::empty())
                .unwrap();
            let response = service.ready().await.unwrap().call(request).await.unwrap();
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).cloned()
        }

        #[derive(Default)]
        struct TestCase {
            /// The allowed origins to provide as input.
            allowed_origins: Vec<HeaderValue>,
            /// Request origins that are expected to be mirrored back in the
            /// response.
            mirrored_origins: Vec<HeaderValue>,
            /// Request origins that are expected to be allowed via a `*`
            /// response.
            wildcard_origins: Vec<HeaderValue>,
            /// Request origins that are expected to be rejected.
            invalid_origins: Vec<HeaderValue>,
        }

        let test_cases = [
            TestCase {
                allowed_origins: vec![HeaderValue::from_static("https://example.org")],
                mirrored_origins: vec![HeaderValue::from_static("https://example.org")],
                invalid_origins: vec![HeaderValue::from_static("https://wrong.com")],
                wildcard_origins: vec![],
            },
            TestCase {
                allowed_origins: vec![HeaderValue::from_static("*.example.org")],
                mirrored_origins: vec![
                    HeaderValue::from_static("https://foo.example.org"),
                    HeaderValue::from_static("https://bar.example.org"),
                    HeaderValue::from_static("https://baz.bar.foo.example.org"),
                ],
                wildcard_origins: vec![],
                invalid_origins: vec![
                    HeaderValue::from_static("https://example.org"),
                    HeaderValue::from_static("https://wrong.com"),
                ],
            },
            TestCase {
                allowed_origins: vec![
                    HeaderValue::from_static("*.example.org"),
                    HeaderValue::from_static("https://other.com"),
                ],
                mirrored_origins: vec![
                    HeaderValue::from_static("https://foo.example.org"),
                    HeaderValue::from_static("https://bar.example.org"),
                    HeaderValue::from_static("https://baz.bar.foo.example.org"),
                    HeaderValue::from_static("https://other.com"),
                ],
                wildcard_origins: vec![],
                invalid_origins: vec![HeaderValue::from_static("https://example.org")],
            },
            TestCase {
                allowed_origins: vec![HeaderValue::from_static("*")],
                mirrored_origins: vec![],
                wildcard_origins: vec![
                    HeaderValue::from_static("literally"),
                    HeaderValue::from_static("https://anything.com"),
                ],
                invalid_origins: vec![],
            },
            TestCase {
                allowed_origins: vec![
                    HeaderValue::from_static("*"),
                    HeaderValue::from_static("https://iwillbeignored.com"),
                ],
                mirrored_origins: vec![],
                wildcard_origins: vec![
                    HeaderValue::from_static("literally"),
                    HeaderValue::from_static("https://anything.com"),
                ],
                invalid_origins: vec![],
            },
        ];

        for test_case in test_cases {
            let allowed_origins = &test_case.allowed_origins;
            let cors = CorsLayer::new()
                .allow_methods([Method::GET])
                .allow_origin(super::build_cors_allowed_origin(allowed_origins));
            for valid in &test_case.mirrored_origins {
                let header = test_request(&cors, valid).await;
                assert_eq!(
                    header.as_ref(),
                    Some(valid),
                    "origin {valid:?} unexpectedly not mirrored\n\
                     allowed_origins={allowed_origins:?}",
                );
            }
            for valid in &test_case.wildcard_origins {
                let header = test_request(&cors, valid).await;
                assert_eq!(
                    header.as_ref(),
                    Some(&HeaderValue::from_static("*")),
                    "origin {valid:?} unexpectedly not allowed\n\
                     allowed_origins={allowed_origins:?}",
                );
            }
            for invalid in &test_case.invalid_origins {
                let header = test_request(&cors, invalid).await;
                assert_eq!(
                    header, None,
                    "origin {invalid:?} unexpectedly not allowed\n\
                     allowed_origins={allowed_origins:?}",
                );
            }
        }
    }
}
