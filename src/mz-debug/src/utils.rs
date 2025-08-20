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
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::{File, create_dir_all, remove_dir_all};
use std::io::{BufWriter, copy};
use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use kube::api::{ListParams, ObjectList};
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
    k8s_namespace: &String,
    mz_instance_name: &Option<String>,
) -> Result<AuthMode, anyhow::Error> {
    let materialize_api = Api::<Materialize>::namespaced(k8s_client.clone(), k8s_namespace);
    let mut object_list = materialize_api
        .list(&ListParams::default())
        .await
        .with_context(|| {
            format!(
                "Failed to get Materialize CR in namespace: {}",
                k8s_namespace
            )
        })?;

    let materialize_cr = if let Some(mz_instance_name) = mz_instance_name {
        object_list
            .items
            .into_iter()
            .find(|item| item.metadata.name.as_ref() == Some(mz_instance_name))
            .with_context(|| {
                format!(
                    "Could not find Materialize CR with name: {}",
                    mz_instance_name
                )
            })?
    } else {
        // Sort the list by creation timestamp, newest first
        sort_k8s_object_list_by_creation_timestamp_desc(&mut object_list);
        object_list.items.first().cloned().with_context(|| {
            format!(
                "Could not find Materialize CR in namespace: {}",
                k8s_namespace
            )
        })?
    };

    let authenticator_kind = materialize_cr.spec.authenticator_kind;

    match authenticator_kind {
        AuthenticatorKind::None => Ok(AuthMode::None),
        AuthenticatorKind::Password => {
            if let (Some(mz_username), Some(mz_password)) = (&mz_username, &mz_password) {
                Ok(AuthMode::Password(PasswordAuthCredentials {
                    username: mz_username.clone(),
                    password: mz_password.clone(),
                }))
            } else {
                Err(anyhow::anyhow!(
                    "mz_username and mz_password are required for password authentication"
                ))
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported authenticator kind: {:?}",
            authenticator_kind
        )),
    }
}

pub fn sort_k8s_object_list_by_creation_timestamp_desc<K>(object_list: &mut ObjectList<K>)
where
    K: kube::Resource<DynamicType = ()> + Clone + Serialize + DeserializeOwned,
{
    object_list.items.sort_by(|a, b| {
        let a_creation_timestamp = a.meta().creation_timestamp.as_ref();
        let b_creation_timestamp = b.meta().creation_timestamp.as_ref();

        b_creation_timestamp.cmp(&a_creation_timestamp)
    });
}
