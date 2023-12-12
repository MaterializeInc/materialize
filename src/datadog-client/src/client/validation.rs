// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::Method;

use crate::client::{Client, Error};

impl<'a> Client<'a> {
    /// Validates that the provided set of key(s) is valid.
    pub async fn validate(&self) -> Result<(), Error> {
        // Send request to the subdomain
        let req = self.build_request(Method::GET, ["v1", "validate"])?;
        self.send_request::<()>(req).await
    }
}
