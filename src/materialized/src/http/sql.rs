// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::bail;
use hyper::{header, Body, Request, Response, StatusCode};
use url::form_urlencoded;

use crate::http::util;

pub async fn handle_sql(
    req: Request<Body>,
    coord_client: &mut mz_coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    let res = async {
        let body = hyper::body::to_bytes(req).await?;
        let body: HashMap<_, _> = form_urlencoded::parse(&body).collect();
        let sql = match body.get("sql") {
            Some(sql) => sql,
            None => bail!("expected `sql` parameter"),
        };
        let res = coord_client.simple_execute(sql).await?;
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&res)?))
            .unwrap())
    }
    .await;
    match res {
        Ok(res) => Ok(res),
        Err(e) => Ok(util::error_response(StatusCode::BAD_REQUEST, e.to_string())),
    }
}
