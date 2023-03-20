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

//! Implementation of the `mz config` command.
//!
//! Consult the user-facing documentation for details.

use serde::{Deserialize, Serialize};
use tabled::Tabled;

use crate::context::Context;
use crate::ui::OptionalStr;

pub struct GetArgs<'a> {
    pub name: &'a str,
}

pub fn get(cx: &mut Context, GetArgs { name }: GetArgs<'_>) -> Result<(), anyhow::Error> {
    let value = cx.config_file().get_param(name)?;
    cx.output_formatter().output_scalar(value)
}

pub fn list(cx: &mut Context) -> Result<(), anyhow::Error> {
    #[derive(Deserialize, Serialize, Tabled)]
    pub struct ConfigParam<'a> {
        #[tabled(rename = "Name")]
        name: &'a str,
        #[tabled(rename = "Value")]
        value: OptionalStr<'a>,
    }

    let values = cx
        .config_file()
        .list_params()
        .into_iter()
        .map(|(name, value)| ConfigParam {
            name,
            value: OptionalStr(value),
        });
    cx.output_formatter().output_table(values)
}

pub struct SetArgs<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

pub async fn set(
    cx: &mut Context,
    SetArgs { name, value }: SetArgs<'_>,
) -> Result<(), anyhow::Error> {
    cx.config_file().set_param(name, Some(value)).await
}

pub struct RemoveArgs<'a> {
    pub name: &'a str,
}

pub async fn remove(
    cx: &mut Context,
    RemoveArgs { name }: RemoveArgs<'_>,
) -> Result<(), anyhow::Error> {
    cx.config_file().set_param(name, None).await
}
