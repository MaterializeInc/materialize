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
use anyhow::Context as AnyhowContext;
use std::fs::{File, create_dir_all, remove_dir_all};
use std::io::{BufWriter, copy};
use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use kube::api::ListParams;
use kube::{Api, Client};
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_server_core::listeners::AuthenticatorKind;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use crate::{AuthMode, PasswordAuthCredentials};

/// Formats the base path for the output of the debug tool.
pub fn format_base_path(date_time: DateTime<Utc>) -> PathBuf {
    PathBuf::from(format!("mz_debug_{}", date_time.format("%Y-%m-%dT%H:%MZ")))
}

pub fn validate_pg_connection_string(connection_string: &str) -> Result<String, String> {
    tokio_postgres::Config::from_str(connection_string)
        .map(|_| connection_string.to_string())
        .map_err(|e| format!("Invalid PostgreSQL connection string: {}", e))
}

pub fn create_tracing_log_file(dir: PathBuf) -> Result<File, std::io::Error> {
    let log_file = dir.join("tracing.log");
    if log_file.exists() {
        remove_dir_all(&log_file)?;
    }
    create_dir_all(&dir)?;
    let file = File::create(&log_file);
    file
}

/// Zips a folder
pub fn zip_debug_folder(zip_file_name: PathBuf, folder_path: &PathBuf) -> std::io::Result<()> {
    // Delete the zip file if it already exists
    if zip_file_name.exists() {
        std::fs::remove_file(&zip_file_name)?;
    }
    let zip_file = File::create(&zip_file_name)?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(zip_file));

    for entry in walkdir::WalkDir::new(folder_path) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            zip_writer.start_file(path.to_string_lossy(), SimpleFileOptions::default())?;
            let mut file = File::open(path)?;
            copy(&mut file, &mut zip_writer)?;
        }
    }

    zip_writer.finish()?;
    Ok(())
}

pub async fn get_k8s_auth_mode(
    mz_username: Option<String>,
    mz_password: Option<String>,
    k8s_client: &Client,
    k8s_namespaces: &Vec<String>,
) -> Result<AuthMode, anyhow::Error> {
    for namespace in k8s_namespaces.iter() {
        let materialize_api = Api::<Materialize>::namespaced(k8s_client.clone(), namespace);
        let object_list = materialize_api
            .list(&ListParams::default())
            .await
            .with_context(|| format!("Failed to get Materialize CR in namespace: {}", namespace))?;

        if !object_list.items.is_empty() {
            let materialize_cr = &object_list.items[0];
            let authenticator_kind = materialize_cr.spec.authenticator_kind;

            match authenticator_kind {
                AuthenticatorKind::None => return Ok(AuthMode::None),
                AuthenticatorKind::Password => {
                    if let (Some(mz_username), Some(mz_password)) = (&mz_username, &mz_password) {
                        return Ok(AuthMode::Password(PasswordAuthCredentials {
                            username: mz_username.clone(),
                            password: mz_password.clone(),
                        }));
                    } else {
                        return Err(anyhow::anyhow!(
                            "mz_username and mz_password are required for password authentication"
                        ));
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported authenticator kind: {:?}",
                        authenticator_kind
                    ));
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "Could not find AuthenticatorKind in Materialize CR"
    ))
}
