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

//! Process environment utilities.

use std::env;
use std::ffi::OsStr;

/// Reports whether the environment variable `key` is set to a truthy value in
/// the current process's environment.
///
/// The empty string and the string "0" are considered false. All other values
/// are considered true.
pub fn is_var_truthy<K>(key: K) -> bool
where
    K: AsRef<OsStr>,
{
    match env::var_os(key) {
        None => false,
        Some(val) => val != "0" && val != "",
    }
}

/// Whether the environment requests no color.
///
/// See: <https://no-color.org>
pub fn no_color() -> bool {
    match env::var_os("NO_COLOR") {
        // We don't use `is_var_truthy` here in order to comply with the
        // `NO_COLOR` specification, which requires that `NO_COLOR=0` disable
        // color.
        Some(val) => !val.is_empty(),
        None => false,
    }
}
