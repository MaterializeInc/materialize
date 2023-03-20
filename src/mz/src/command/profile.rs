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

//! Implementation of the `mz profile` command.
//!
//! Consult the user-facing documentation for details.

use anyhow::bail;

use crate::context::ProfileContext;

pub async fn init(cx: &mut ProfileContext) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub async fn list(cx: &mut ProfileContext) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub async fn remove(cx: &mut ProfileContext) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub struct ConfigGetArgs<'a> {
    pub name: &'a str,
}

pub async fn config_get(cx: &mut ProfileContext, ConfigGetArgs { name }: ConfigGetArgs<'_>) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub async fn config_list(cx: &mut ProfileContext) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub struct ConfigSetArgs<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

pub async fn config_set(
    cx: &mut ProfileContext,
    ConfigSetArgs { name, value }: ConfigSetArgs<'_>,
) -> Result<(), anyhow::Error> {
    bail!("TODO")
}

pub struct ConfigRemoveArgs<'a> {
    pub name: &'a str,
}

pub async fn config_remove(
    cx: &mut ProfileContext,
    ConfigRemoveArgs { name }: ConfigRemoveArgs<'_>,
) -> Result<(), anyhow::Error> {
    bail!("TODO")
}
