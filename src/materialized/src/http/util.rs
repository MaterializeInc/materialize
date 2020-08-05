// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP utilities.

use askama::Template;
use hyper::{Body, Response, StatusCode};

/// Renders a template into an HTTP response.
pub fn template_response<T>(template: T) -> Response<Body>
where
    T: Template,
{
    let contents = template.render().expect("template rendering cannot fail");
    Response::new(Body::from(contents))
}

/// Renders a status code and error message into an HTTP response.
pub fn error_response<S>(code: StatusCode, message: S) -> Response<Body>
where
    S: Into<String>,
{
    Response::builder()
        .status(code)
        .body(Body::from(message.into()))
        .unwrap()
}
