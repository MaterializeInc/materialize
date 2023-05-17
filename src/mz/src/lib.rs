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

//! `mz` is the Materialize command-line interface (CLI).

use mz_build_info::{build_info, BuildInfo};
use once_cell::sync::Lazy;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.semver_version().to_string());

pub mod command;
pub mod config_file;
pub mod context;
pub mod error;
mod server;
mod sql_client;
pub mod ui;
