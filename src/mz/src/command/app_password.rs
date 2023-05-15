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

use mz_frontegg_client::client::app_password::CreateAppPasswordRequest;
use serde::{Deserialize, Serialize};
use tabled::Tabled;

use crate::context::ProfileContext;
use crate::error::Error;

pub struct CreateArgs<'a> {
    pub description: &'a str,
}

pub async fn create(
    cx: &mut ProfileContext,
    params: CreateAppPasswordRequest<'_>,
) -> Result<(), Error> {
    let app_password = cx.admin_client().create_app_password(params).await?;

    println!("{}", app_password);
    Ok(())
}

pub async fn list(cx: &mut ProfileContext) -> Result<(), Error> {
    #[derive(Deserialize, Serialize, Tabled)]
    pub struct AppPassword {
        #[tabled(rename = "Name")]
        description: String,
        #[tabled(rename = "Created At")]
        created_at: String,
    }

    let passwords = cx.admin_client().list_app_passwords().await?;
    let output_formatter = cx.output_formatter();
    output_formatter.output_table(passwords.iter().map(|x| AppPassword {
        description: x.description.clone(),
        created_at: x.created_at.clone(),
    }))?;

    Ok(())
}
