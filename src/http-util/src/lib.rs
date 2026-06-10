// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP utilities.

use std::io::{Read, Write};

use askama::Template;
use axum::Json;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Uri;
use axum::http::status::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum_extra::TypedHeader;
use base64::prelude::*;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use headers::ContentType;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use prometheus::Encoder;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::AllowOrigin;
use tracing_subscriber::EnvFilter;

/// MIME type used for the Prometheus protobuf scrape format.
/// <https://prometheus.io/docs/instrumenting/content_negotiation/#protocol-headers>
pub const PROMETHEUS_PROTOBUF_CONTENT_TYPE: &str = "application/vnd.google.protobuf; \
     proto=io.prometheus.client.MetricFamily; \
     encoding=delimited";

/// Request header sent by callers that understand and want
/// [`MATERIALIZE_ENRICH_RULES_HEADER`] in the response.
pub const MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER: &str = "x-materialize-accept-enrich-rules";

/// Response header listing the [`mz_ore::metrics::Rule`]s registered on the
/// metrics registry, as gzipped-then-base64-encoded JSON (see
/// [`encode_enrich_rules`]). Emitted by [`handle_prometheus`] only when the
/// caller opts in via [`MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER`].
pub const MATERIALIZE_ENRICH_RULES_HEADER: &str = "x-materialize-enrich-rules";

fn wants_prometheus_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.contains(PROMETHEUS_PROTOBUF_CONTENT_TYPE))
}

fn wants_enrich_rules(headers: &HeaderMap) -> bool {
    headers.contains_key(MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER)
}

/// Serializes `value` as JSON, gzips it, and base64-encodes the result for
/// transport in [`MATERIALIZE_ENRICH_RULES_HEADER`].
///
/// The same handful of enrichment rules repeat across nearly every metric, so
/// the JSON is highly compressible. Gzipping keeps the header well under the
/// typical 8-16KB header-size limit even with hundreds of metrics; we then
/// base64-encode because HTTP header values must be printable ASCII.
pub fn encode_enrich_rules<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let json = serde_json::to_vec(value)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json)?;
    Ok(BASE64_STANDARD.encode(encoder.finish()?))
}

pub fn decode_enrich_rules<T: DeserializeOwned>(value: &str) -> anyhow::Result<T> {
    let compressed = BASE64_STANDARD.decode(value)?;
    let mut json = Vec::new();
    GzDecoder::new(&compressed[..]).read_to_end(&mut json)?;
    Ok(serde_json::from_slice(&json)?)
}

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
    (
        dir_1: $dir_1:expr,
        $(dir_2: $dir_2:expr,)?
        prod_base_path: $prod_base_path:expr,
        dev_base_path: $dev_base_path:expr$(,)?
    ) => {
        #[allow(clippy::unused_async)]
        pub async fn handle_static(
            path: ::axum::extract::Path<String>,
        ) -> impl ::axum::response::IntoResponse {
            #[cfg(not(feature = "dev-web"))]
            const DIR_1: ::include_dir::Dir = $dir_1;
            $(
                #[cfg(not(feature = "dev-web"))]
                const DIR_2: ::include_dir::Dir = $dir_2;
            )?


            #[cfg(not(feature = "dev-web"))]
            fn get_static_file(path: &str) -> Option<&'static [u8]> {
                DIR_1.get_file(path).or_else(|| DIR_2.get_file(path)).map(|f| f.contents())
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
                Some("js") => Some(::axum_extra::TypedHeader(::headers::ContentType::from(
                    ::mime::TEXT_JAVASCRIPT,
                ))),
                Some("css") => Some(::axum_extra::TypedHeader(::headers::ContentType::from(
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
///
/// If the caller's `Accept` header advertises support for the Prometheus
/// protobuf format (`application/vnd.google.protobuf`), the response is a
/// length-delimited stream of `io.prometheus.client.MetricFamily` messages.
/// Otherwise the standard Prometheus text format is returned.
#[allow(clippy::unused_async)]
pub async fn handle_prometheus(
    registry: &MetricsRegistry,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let families = registry.gather();
    let mut buf = Vec::new();
    let content_type = if wants_prometheus_protobuf(&headers) {
        prometheus::ProtobufEncoder::new()
            .encode(&families, &mut buf)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        ContentType::from(
            PROMETHEUS_PROTOBUF_CONTENT_TYPE
                .parse::<mime::Mime>()
                .map_err(|e: mime::FromStrError| {
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                })?,
        )
    } else {
        prometheus::TextEncoder::new()
            .encode(&families, &mut buf)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        ContentType::text()
    };

    let mut resp = (TypedHeader(content_type), buf).into_response();
    if wants_enrich_rules(&headers) {
        let rules_by_metric = registry.rules_by_metric();
        if !rules_by_metric.is_empty() {
            let encoded = encode_enrich_rules(&rules_by_metric)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            resp.headers_mut().insert(
                MATERIALIZE_ENRICH_RULES_HEADER,
                HeaderValue::from_str(&encoded)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
            );
        }
    }
    Ok(resp)
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

/// Returns true if `origin` matches any entry in `allowed`. Supports bare `*`
/// (any origin), exact match, and wildcard subdomains (`*.example.com`).
pub fn origin_is_allowed(origin: &HeaderValue, allowed: &[HeaderValue]) -> bool {
    fn wildcard_origin_match(origin: &HeaderValue, wildcard: &[u8]) -> bool {
        let Some(origin) = origin.to_str().ok() else {
            return false;
        };
        let Ok(origin) = origin.parse::<Uri>() else {
            return false;
        };
        let Some(host) = origin.host() else {
            return false;
        };

        host.as_bytes().ends_with(wildcard)
    }

    if allowed.iter().any(|o| o.as_bytes() == b"*") {
        return true;
    }
    for val in allowed {
        if (val.as_bytes().starts_with(b"*.")
            && wildcard_origin_match(origin, &val.as_bytes()[1..]))
            || origin == val
        {
            return true;
        }
    }
    false
}

/// Construct a CORS policy to allow origins to query us via HTTP. If any bare
/// '*' is passed, this allows any origin; otherwise, allows a list of origins,
/// which can include wildcard subdomains. If the allowed origin starts with a
/// '*', allow anything from that glob. Otherwise check for an exact match.
pub fn build_cors_allowed_origin<'a, I>(allowed: I) -> AllowOrigin
where
    I: IntoIterator<Item = &'a HeaderValue>,
{
    fn wildcard_origin_match(origin: &HeaderValue, wildcard: &[u8]) -> bool {
        let Some(origin) = origin.to_str().ok() else {
            return false;
        };
        let Ok(origin) = origin.parse::<Uri>() else {
            return false;
        };
        let Some(host) = origin.host() else {
            return false;
        };

        host.as_bytes().ends_with(wildcard)
    }

    let allowed = allowed.into_iter().cloned().collect::<Vec<HeaderValue>>();
    if allowed.iter().any(|o| o.as_bytes() == b"*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::predicate(move |origin: &HeaderValue, _request_parts: _| {
            for val in &allowed {
                if (val.as_bytes().starts_with(b"*.")
                    && wildcard_origin_match(origin, &val.as_bytes()[1..]))
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
    use std::collections::BTreeMap;

    use axum::http::HeaderMap;
    use http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN};
    use http::{HeaderValue, Method, Request, Response};
    use mz_ore::metric;
    use mz_ore::metrics::{MetricsRegistry, Rule};
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::cors::CorsLayer;

    use super::{
        MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER, MATERIALIZE_ENRICH_RULES_HEADER, handle_prometheus,
    };

    fn registry_with_rules() -> MetricsRegistry {
        let registry = MetricsRegistry::new();
        let _: prometheus::IntCounter = registry.register(metric!(
            name: "mz_test_handle_prometheus_metric",
            help: "test metric carrying a per-metric enrichment rule",
            rules: [
                Rule::ClusterNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    output_label: "cluster_name".into(),
                },
            ],
        ));
        registry
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function on OS `linux`
    async fn handle_prometheus_emits_rules_header_when_opted_in() {
        let registry = registry_with_rules();
        let mut headers = HeaderMap::new();
        headers.insert(
            MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER,
            HeaderValue::from_static("1"),
        );
        let resp = handle_prometheus(&registry, headers).await.unwrap();
        let value = resp
            .headers()
            .get(MATERIALIZE_ENRICH_RULES_HEADER)
            .expect("rules header present");
        let parsed: BTreeMap<String, Vec<Rule>> =
            super::decode_enrich_rules(value.to_str().unwrap()).unwrap();
        assert_eq!(parsed, registry.rules_by_metric());
    }

    #[mz_ore::test(tokio::test)]
    async fn handle_prometheus_omits_header_without_opt_in() {
        let registry = registry_with_rules();
        let resp = handle_prometheus(&registry, HeaderMap::new())
            .await
            .unwrap();
        assert!(
            resp.headers()
                .get(MATERIALIZE_ENRICH_RULES_HEADER)
                .is_none()
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn handle_prometheus_omits_header_when_no_rules() {
        let registry = MetricsRegistry::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER,
            HeaderValue::from_static("1"),
        );
        let resp = handle_prometheus(&registry, headers).await.unwrap();
        assert!(
            resp.headers()
                .get(MATERIALIZE_ENRICH_RULES_HEADER)
                .is_none()
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn test_cors() {
        async fn test_request(cors: &CorsLayer, origin: &HeaderValue) -> Option<HeaderValue> {
            let mut service = ServiceBuilder::new()
                .layer(cors)
                .service_fn(|_| async { Ok::<_, anyhow::Error>(Response::new("")) });
            let request = Request::builder().header(ORIGIN, origin).body("").unwrap();
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
                    HeaderValue::from_static("https://foo.example.org:8443"),
                    HeaderValue::from_static("https://bar.example.org"),
                    HeaderValue::from_static("https://baz.bar.foo.example.org"),
                ],
                wildcard_origins: vec![],
                invalid_origins: vec![
                    HeaderValue::from_static("https://example.org"),
                    HeaderValue::from_static("https://wrong.com"),
                    HeaderValue::from_static("https://wrong.com:8443"),
                ],
            },
            TestCase {
                allowed_origins: vec![
                    HeaderValue::from_static("*.example.org"),
                    HeaderValue::from_static("https://other.com"),
                ],
                mirrored_origins: vec![
                    HeaderValue::from_static("https://foo.example.org"),
                    HeaderValue::from_static("https://foo.example.org:8443"),
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
