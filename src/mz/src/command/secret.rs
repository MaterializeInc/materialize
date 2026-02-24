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

use std::io::{self, Write};

use mz_postgres_util::{Sql, sql};

use crate::{context::RegionContext, error::Error};

/// Represents the args needed to create a secret
pub struct CreateArgs<'a> {
    /// Represents the database where the secret
    /// is going to be created.
    pub database: Option<&'a str>,
    /// Represents the schema where the secret
    /// is going to be created.
    pub schema: Option<&'a str>,
    /// Represents the secret name.
    pub name: &'a str,
    /// If force is set to true, the secret will be overwritten if it exists.
    ///
    /// If force is set to false, the command will fail if the secret exists.
    pub force: bool,
}

/// Creates a secret in the profile environment.
/// Behind the scenes this command uses the `psql` to run
/// the SQL commands.
pub async fn create(
    cx: &RegionContext,
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
    let loading_spinner = cx.output_formatter().loading_spinner("Creating secret...");

    let claims = cx.admin_client().claims().await?;
    let region_info = cx.get_region_info().await?;
    let user = claims.user()?;

    let mut client = cx.sql_client().shell(&region_info, user, None);

    // Build the queries to create the secret.
    let mut commands: Vec<Sql> = vec![];
    let name = Sql::ident(name);

    if let Some(database) = database {
        client.args(vec!["-d", database]);
    }

    if let Some(schema) = schema {
        commands.push(sql!("SET search_path TO {}", Sql::ident(schema)));
    }

    // Treat stdin as a literal value to avoid command injection through `psql -c`.
    let value = Sql::literal(&buffer);

    if force {
        // Rather than checking if the SECRET exists, do an upsert.
        // Unfortunately the `-c` command in psql runs inside a transaction
        // and CREATE and ALTER SECRET cannot be run inside a transaction block.
        // The alternative is passing two `-c` commands to psql.

        // Otherwise if the SECRET exists `psql` will display a NOTICE message.
        commands.push(sql!("SET client_min_messages TO WARNING;"));
        commands.push(sql!(
            "CREATE SECRET IF NOT EXISTS {} AS {};",
            name.clone(),
            value.clone()
        ));
        commands.push(sql!("ALTER SECRET {} AS {};", name.clone(), value.clone()));
    } else {
        commands.push(sql!("CREATE SECRET {} AS {};", name, value));
    }

    commands.iter().for_each(|c| {
        client.args(vec!["-c", c.as_str()]);
    });

    let output = client
        .arg("-q")
        .output()
        .map_err(|err| Error::CommandExecutionError(err.to_string()))?;

    if !output.status.success() {
        let error_message = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(Error::CommandFailed(error_message));
    }

    loading_spinner.finish_and_clear();
    Ok(())
}
