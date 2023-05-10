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

use crate::{context::ProfileContext, error::Error};

pub struct CreateArgs<'a> {
    pub email: &'a str,
    pub name: &'a str,
}

pub async fn create(
    cx: &mut ProfileContext,
    CreateArgs { email, name }: CreateArgs<'_>,
) -> Result<(), Error> {
    todo!()
}

pub async fn list(cx: &mut ProfileContext) -> Result<(), Error> {
    todo!()
}

pub struct RemoveArgs<'a> {
    pub email: &'a str,
}

pub async fn remove(
    cx: &mut ProfileContext,
    RemoveArgs { email }: RemoveArgs<'_>,
) -> Result<(), Error> {
    todo!()
}
