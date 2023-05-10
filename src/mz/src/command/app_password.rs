// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementation of the `mz app-password` command.
//!
//! Consult the user-facing documentation for details.

use mz_frontegg_auth::AppPassword as AuthAppPassword;
use mz_frontegg_client::client::app_password::{AppPassword, CreateAppPasswordRequest};

use crate::context::ProfileContext;
use crate::error::Error;

pub struct CreateArgs<'a> {
    pub description: &'a str,
}

pub async fn create(
    cx: &mut ProfileContext,
    params: CreateAppPasswordRequest<'_>,
) -> Result<AuthAppPassword, Error> {
    Ok(cx.admin_client().create_app_password(params).await?)
}

pub async fn list(cx: &mut ProfileContext) -> Result<Vec<AppPassword>, Error> {
    Ok(cx.admin_client().list_app_passwords().await?)
}
