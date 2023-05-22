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

use mz_cloud_api::client::environment::Environment;

use crate::{context::RegionContext, error::Error};

pub struct RunArgs {
    pub psql_args: Vec<String>,
}

pub async fn run(cx: &mut RegionContext, RunArgs { psql_args }: RunArgs) -> Result<(), Error> {
    let sql_client = cx.sql_client();
    let claims = cx.admin_client().claims();
    let region = cx.get_region().await?;
    let enviornment = cx.get_environment(region).await?;
    let email = claims.await?.email;

    let _error = sql_client.shell(enviornment, email).args(psql_args).exec();

    Ok(())
}

/// Runs pg_isready to check if an environment is healthy
pub(crate) async fn check_environment_health(
    cx: &mut RegionContext,
    enviornment: Environment,
) -> Result<bool, Error> {
    let sql_client = cx.sql_client();
    let claims = cx.admin_client().claims();
    let email = claims.await?.email;

    Ok(sql_client
        .is_ready(enviornment, email)
        .output()?
        .status
        .success())
}
