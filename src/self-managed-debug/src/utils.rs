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

use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};

pub fn format_base_path(date_time: DateTime<Utc>) -> PathBuf {
    let mut path = PathBuf::from("mz-debug");
    path = path.join(date_time.format("%Y-%m-%dT%H:%MZ").to_string());
    path
}

pub fn validate_pg_connection_string(connection_string: &str) -> Result<String, String> {
    tokio_postgres::Config::from_str(connection_string)
        .map(|_| connection_string.to_string())
        .map_err(|e| format!("Invalid PostgreSQL connection string: {}", e))
}
