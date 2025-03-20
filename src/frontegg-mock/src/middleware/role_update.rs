// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::Request;
use axum::{extract::State, middleware::Next, response::IntoResponse};

use crate::server::Context;
use std::sync::Arc;

pub async fn role_update_middleware(
    State(context): State<Arc<Context>>,
    request: Request,
    next: Next,
) -> impl IntoResponse {
    {
        let mut role_updates_rx = context.role_updates_rx.lock().unwrap();
        while let Ok((email, roles)) = role_updates_rx.try_recv() {
            let mut users = context.users.lock().unwrap();
            users.get_mut(&email).unwrap().roles = roles;
        }
    }
    next.run(request).await
}
