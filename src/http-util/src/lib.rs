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
use axum::response::Html;

/// Renders a template into an HTTP response.
pub fn template_response<T>(template: T) -> Html<String>
where
    T: Template,
{
    Html(template.render().expect("template rendering cannot fail"))
}

#[macro_export]
macro_rules! make_handle_static {
    ($static_dir:expr, $prod_base_path:expr, $dev_base_path:expr) => {      
        pub async fn handle_static(path: RequestPath<String>) -> impl IntoResponse {
            let path = path.strip_prefix('/').unwrap_or(&path);
            let content_type = match Path::new(path).extension().and_then(|e| e.to_str()) {
                Some("js") => Some(TypedHeader(ContentType::from(mime::TEXT_JAVASCRIPT))),
                Some("css") => Some(TypedHeader(ContentType::from(mime::TEXT_CSS))),
                None | Some(_) => None,
            };
            match get_static_file(path) {
                Some(body) => Ok((content_type, body)),
                None => Err((StatusCode::NOT_FOUND, "not found")),
            }
        }
        
        #[cfg(not(feature = "dev-web"))]
        const STATIC_DIR: include_dir::Dir = $static_dir;

        #[cfg(not(feature = "dev-web"))]
        fn get_static_file(path: &str) -> Option<&'static [u8]> {
            STATIC_DIR.get_file(path).map(|f| f.contents())
        }

        #[cfg(feature = "dev-web")]
        fn get_static_file(path: &str) -> Option<Vec<u8>> {
            use std::fs;

            #[cfg(not(debug_assertions))]
            compile_error!("cannot enable insecure `dev-web` feature in release mode");

            // Prefer the unminified files in static-dev, if they exist.
            let dev_path = format!(
                "{}/{}/{}",
                env!("CARGO_MANIFEST_DIR"),
                $dev_base_path,
                path
            );
            let prod_path = format!("{}/{}/{}", env!("CARGO_MANIFEST_DIR"), $prod_base_path, path);
            match fs::read(dev_path).or_else(|_| fs::read(prod_path)) {
                Ok(contents) => Some(contents),
                Err(e) => {
                    debug!("dev-web failed to load static file: {}: {}", path, e);
                    None
                }
            }
        }        
    }
}
