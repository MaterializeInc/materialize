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

use anyhow::bail;

use crate::context::RegionContext;

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
) -> Result<(), anyhow::Error> {
    bail!("TODO")
}
