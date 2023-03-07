// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::header::AUTHORIZATION;
use reqwest::RequestBuilder;

use crate::configuration::FronteggAuth;

/// Extension methods for building API requests
pub trait RequestBuilderExt {
    /// Authenticate the client with frontegg
    fn authenticate(self, auth: &FronteggAuth) -> Self;
}

impl RequestBuilderExt for RequestBuilder {
    fn authenticate(self, auth: &FronteggAuth) -> Self {
        let authorization = format!("Bearer {}", auth.access_token);
        self.header(AUTHORIZATION, &authorization)
    }
}
