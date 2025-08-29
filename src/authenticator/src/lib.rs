// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_adapter::Client as AdapterClient;
use mz_frontegg_auth::Authenticator as FronteggAuthenticator;

#[derive(Debug, Clone)]
pub enum Authenticator {
    Frontegg(FronteggAuthenticator),
    Password(AdapterClient),
    Sasl(AdapterClient),
    None,
}
