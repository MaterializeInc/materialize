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

//! Implementation of the `mz secret` command.
//!
//! Consult the user-facing documentation for details.

use std::{
    io::{self, Write},
    os::unix::process::CommandExt,
};

use crate::{context::RegionContext, error::Error};

pub struct CreateArgs<'a> {
    pub database: Option<&'a str>,
    pub schema: Option<&'a str>,
    pub name: &'a str,
    pub force: bool,
}

pub async fn create(
    cx: &mut RegionContext,
    CreateArgs {
        database,
        schema,
        name,
        force,
    }: CreateArgs<'_>,
) -> Result<(), Error> {
    let mut buffer = String::new();

    // Ask the user to write the secret
    print!("Secret: ");
    let _ = std::io::stdout().flush();
    io::stdin().read_line(&mut buffer)?;
    buffer = buffer.trim().to_string();

    // Retrieve information to open the psql shell sessions.
    let claims = cx.admin_client().claims();
    let region = cx.get_region().await?;
    let enviornment = cx.get_environment(region).await?;
    let email = claims.await?.email;

    let mut client = cx.sql_client().shell(enviornment, email);

    // Build the queries to create the secret.
    let mut commands: Vec<String> = vec![];

    if let Some(database) = database {
        client.args(vec!["-d", database]);
    }

    if let Some(schema) = schema {
        commands.push(format!("SET search_path TO {}", schema));
    }

    // The most common ways to write a secret are the following ways:
    // 1. Decode function: decode('c2VjcmV0Cg==', 'base64')
    // 2. ASCII: 13de2601-24b4-4d8f-9931-375c0b2b5cd4
    // For case 2) we want to scape the value for a better experience.
    if !buffer.starts_with("decode") {
        buffer = format!("'{}'", buffer);
    }

    if force {
        // Rather than checking if the SECRET exists, do an upsert.
        // Unfortunately the `-c` command in psql runs inside a transaction
        // and CREATE and ALTER SECRET cannot be run inside a transaction block.
        // The alternative is passing two `-c` commands to psql.

        // Otherwise if the SECRET exists `psql` will display a NOTICE message.
        commands.push("SET client_min_messages TO WARNING;".to_string());
        commands.push(format!(
            "CREATE SECRET IF NOT EXISTS {} AS {};",
            name, buffer
        ));
        commands.push(format!("ALTER SECRET {} AS {};", name, buffer));
    } else {
        commands.push(format!("CREATE SECRET {} AS {};", name, buffer));
    }

    commands.iter().for_each(|c| {
        client.args(vec!["-c", c]);
    });
    let _error = client.arg("-q").exec();

    Ok(())
}
