// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Catalog introspection HTTP endpoints.

use std::future::Future;

use futures::sink::SinkExt;
use hyper::{header, Body, Request, Response};

use crate::http::Server;

impl Server {
    pub fn handle_internal_catalog(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut cmdq_tx = self.cmdq_tx.clone();
        async move {
            cmdq_tx.send(coord::Command::DumpCatalog { tx }).await?;
            let dump = rx.await?;
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(dump))
                .unwrap())
        }
    }
}
