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
use crate::error::Error;
use crate::ui::OptionalStr;

/// Represents the args to retrieve a *global* configuration value.
pub struct GetArgs<'a> {
    /// Represents the configuration field name.
    pub name: &'a str,
}

/// Shows the value of a *global* configuration field.
pub fn get(cx: &Context, GetArgs { name }: GetArgs<'_>) -> Result<(), Error> {
    let value = cx.config_file().get_param(name)?;
    cx.output_formatter().output_scalar(value)
}

/// Shows all the possible field and its values in the *global* configuration.
pub fn list(cx: &Context) -> Result<(), Error> {
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

/// Represents the args to set the value of a *global* configuration field.
pub struct SetArgs<'a> {
    /// Represents the name of the field to set the value.
    pub name: &'a str,
    /// Represents the new value of the field.
    pub value: &'a str,
}

/// Sets the value of a global configuration field.
pub async fn set(cx: &Context, SetArgs { name, value }: SetArgs<'_>) -> Result<(), Error> {
    cx.config_file().set_param(name, Some(value)).await
}

/// Represents the args to remove the value of a *global* configuration field.
pub struct RemoveArgs<'a> {
    /// Represents the name of the field to remove.
    pub name: &'a str,
}

/// Removes the value from a *global* configuration field.
pub async fn remove(cx: &Context, RemoveArgs { name }: RemoveArgs<'_>) -> Result<(), Error> {
    cx.config_file().set_param(name, None).await
}
