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

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{copy, BufWriter};
use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

static DEBUG_FOLDER: &str = "mz-debug";

pub fn format_base_path(date_time: DateTime<Utc>) -> PathBuf {
    let path = PathBuf::from(DEBUG_FOLDER).join(date_time.format("%Y-%m-%dT%H:%MZ").to_string());
    path
}

pub fn validate_pg_connection_string(connection_string: &str) -> Result<String, String> {
    tokio_postgres::Config::from_str(connection_string)
        .map(|_| connection_string.to_string())
        .map_err(|e| format!("Invalid PostgreSQL connection string: {}", e))
}

pub fn create_tracing_log_file(date_time: DateTime<Utc>) -> Result<File, std::io::Error> {
    let dir = format_base_path(date_time);
    let log_file = dir.join("tracing.log");
    if log_file.exists() {
        remove_dir_all(&log_file)?;
    }
    create_dir_all(&dir)?;
    let file = File::create(&log_file);
    file
}

/// Zips a folder and removes it after the zip is created.
pub fn zip_debug_folder(zip_file_name: PathBuf) -> std::io::Result<()> {
    // Delete the zip file if it already exists
    if zip_file_name.exists() {
        std::fs::remove_file(&zip_file_name)?;
    }
    let zip_file = File::create(&zip_file_name)?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(zip_file));

    for entry in walkdir::WalkDir::new(&DEBUG_FOLDER) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            zip_writer.start_file(path.to_string_lossy(), SimpleFileOptions::default())?;
            let mut file = File::open(path)?;
            copy(&mut file, &mut zip_writer)?;
        }
    }

    remove_dir_all(&DEBUG_FOLDER)?;

    zip_writer.finish()?;
    Ok(())
}
