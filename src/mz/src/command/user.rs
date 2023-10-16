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

/// Represents the structure to create a user in the profile organization.
pub struct CreateArgs<'a> {
    /// Represents the new user email to add into the profile organization.
    /// This value must be unique in the profile organization.
    pub email: &'a str,
    /// Represents the new user name to add into the profile organization.
    pub name: &'a str,
}

/// Creates a user in the profile organization.
pub async fn create(
    cx: &ProfileContext,
    CreateArgs { email, name }: CreateArgs<'_>,
) -> Result<(), Error> {
    let loading_spinner = cx.output_formatter().loading_spinner("Creating user...");
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

    loading_spinner.finish_with_message("User created.");
    Ok(())
}

/// Lists all the users in the profile organization.
pub async fn list(cx: &ProfileContext) -> Result<(), Error> {
    let output_formatter = cx.output_formatter();

    let loading_spinner = output_formatter.loading_spinner("Retrieving users...");
    #[derive(Deserialize, Serialize, Tabled)]
    pub struct User {
        #[tabled(rename = "Email")]
        email: String,
        #[tabled(rename = "Name")]
        name: String,
    }

    let users = cx.admin_client().list_users().await?;

    loading_spinner.finish_and_clear();
    output_formatter.output_table(users.into_iter().map(|x| User {
        email: x.email,
        name: x.name,
    }))?;

    Ok(())
}

/// Represents the args structure to remove a user from Materialize.
pub struct RemoveArgs<'a> {
    /// Represents the email of the user to remove.
    pub email: &'a str,
}

/// Removes a user from the profile context using the admin client.
pub async fn remove(
    cx: &ProfileContext,
    RemoveArgs { email }: RemoveArgs<'_>,
) -> Result<(), Error> {
    let loading_spinner = cx.output_formatter().loading_spinner("Removing user...");

    let users = cx.admin_client().list_users().await?;
    let user = users
        .into_iter()
        .find(|x| x.email == email)
        .expect("email not found.");

    cx.admin_client()
        .remove_user(RemoveUserRequest { user_id: user.id })
        .await?;

    loading_spinner.finish_with_message(format!("User {} removed.", email));
    Ok(())
}
