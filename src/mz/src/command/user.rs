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

//! Implementation of the `mz user` command.
//!
//! Consult the user-facing documentation for details.

use mz_frontegg_client::client::user::{CreateUserRequest, RemoveUserRequest};
use serde::{Deserialize, Serialize};
use tabled::Tabled;

use crate::{context::ProfileContext, error::Error};

pub struct CreateArgs<'a> {
    pub email: &'a str,
    pub name: &'a str,
}

pub async fn create(
    cx: &mut ProfileContext,
    CreateArgs { email, name }: CreateArgs<'_>,
) -> Result<(), Error> {
    let roles = cx.admin_client().list_roles().await?;
    let role_ids = roles.into_iter().map(|role| role.id).collect();

    cx.admin_client()
        .create_user(CreateUserRequest {
            email: email.to_string(),
            name: name.to_string(),
            provider: "local".to_string(),
            role_ids,
        })
        .await?;

    Ok(())
}

pub async fn list(cx: &mut ProfileContext) -> Result<(), Error> {
    #[derive(Deserialize, Serialize, Tabled)]
    pub struct User {
        #[tabled(rename = "Email")]
        email: String,
        #[tabled(rename = "Name")]
        name: String,
    }

    let users = cx.admin_client().list_users().await?;
    let output_formatter = cx.output_formatter();
    output_formatter.output_table(users.into_iter().map(|x| User {
        email: x.email,
        name: x.name,
    }))?;

    Ok(())
}

pub struct RemoveArgs<'a> {
    pub email: &'a str,
}

pub async fn remove(
    cx: &mut ProfileContext,
    RemoveArgs { email }: RemoveArgs<'_>,
) -> Result<(), Error> {
    let users = cx.admin_client().list_users().await?;
    let user = users
        .into_iter()
        .find(|x| x.email == email)
        .expect("email not found.");

    cx.admin_client()
        .remove_user(RemoveUserRequest { user_id: user.id })
        .await?;

    Ok(())
}
