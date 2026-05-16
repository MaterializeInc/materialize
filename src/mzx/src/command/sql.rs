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

//! Implementation of the `mz sql` command.
//!
//! Consult the user-facing documentation for details.

use std::os::unix::process::CommandExt;

use crate::{context::RegionContext, error::Error};

/// Represents the structure containing the args to run the SQL shell.
pub struct RunArgs {
    /// The cluster name to use in the connection.
    pub cluster: Option<String>,
    /// Contains all the arguments to pass into the shell.
    /// Take into account that they are added at the end of the statement.
    pub psql_args: Vec<String>,
}

/// Creates an interactive SQL shell connection to the profile context environment.
/// The SQL shell command is running `psql` behind the scenes.
pub async fn run(cx: &RegionContext, RunArgs { cluster, psql_args }: RunArgs) -> Result<(), Error> {
    let sql_client = cx.sql_client();
    let claims = cx.admin_client().claims().await?;
    let region_info = cx.get_region_info().await?;
    let user = claims.user()?;

    let _error = sql_client
        .shell(&region_info, user, cluster)
        .args(psql_args)
        .exec();

    Ok(())
}
