// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Catalog introspection HTTP endpoints.

use hyper::{header, Body, Request, Response};

pub async fn handle_internal_catalog(
    _: Request<Body>,
    coord_client: &mut mz_coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    let dump = coord_client.dump_catalog().await?;
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(dump))
        .unwrap())
}
