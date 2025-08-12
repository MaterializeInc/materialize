// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;

use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::{
    Error, ErrorKind, Namespace, NamespaceIdent, TableIdent, TableRequirement, TableUpdate,
};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CatalogConfig {
    pub(super) overrides: BTreeMap<String, String>,
    pub(super) defaults: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ErrorResponse {
    error: ErrorModel,
}

impl From<ErrorResponse> for Error {
    fn from(resp: ErrorResponse) -> Error {
        resp.error.into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ErrorModel {
    pub(super) message: String,
    pub(super) r#type: String,
    pub(super) code: u16,
    pub(super) stack: Option<Vec<String>>,
}

impl From<ErrorModel> for Error {
    fn from(value: ErrorModel) -> Self {
        let mut error = Error::new(ErrorKind::DataInvalid, value.message)
            .with_context("type", value.r#type)
            .with_context("code", format!("{}", value.code));

        if let Some(stack) = value.stack {
            error = error.with_context("stack", stack.join("\n"));
        }

        error
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct OAuthError {
    pub(super) error: String,
    pub(super) error_description: Option<String>,
    pub(super) error_uri: Option<String>,
}

impl From<OAuthError> for Error {
    fn from(value: OAuthError) -> Self {
        let mut error = Error::new(
            ErrorKind::DataInvalid,
            format!("OAuthError: {}", value.error),
        );

        if let Some(desc) = value.error_description {
            error = error.with_context("description", desc);
        }

        if let Some(uri) = value.error_uri {
            error = error.with_context("uri", uri);
        }

        error
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TokenResponse {
    pub(super) access_token: String,
    pub(super) token_type: String,
    pub(super) expires_in: Option<u64>,
    pub(super) issued_token_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct NamespaceSerde {
    pub(super) namespace: Vec<String>,
    pub(super) properties: Option<BTreeMap<String, String>>,
}

impl TryFrom<NamespaceSerde> for Namespace {
    type Error = Error;
    fn try_from(value: NamespaceSerde) -> std::result::Result<Self, Self::Error> {
        Ok(Namespace::with_properties(
            NamespaceIdent::from_vec(value.namespace)?,
            value.properties.unwrap_or_default().into_iter().collect(),
        ))
    }
}

impl From<&Namespace> for NamespaceSerde {
    fn from(value: &Namespace) -> Self {
        Self {
            namespace: value.name().as_ref().clone(),
            properties: Some(
                value
                    .properties()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct ListNamespaceResponse {
    pub(super) namespaces: Vec<Vec<String>>,
    #[serde(default)]
    pub(super) next_page_token: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct UpdateNamespacePropsRequest {
    removals: Option<Vec<String>>,
    updates: Option<BTreeMap<String, String>>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct UpdateNamespacePropsResponse {
    updated: Vec<String>,
    removed: Vec<String>,
    missing: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct ListTableResponse {
    pub(super) identifiers: Vec<TableIdent>,
    #[serde(default)]
    pub(super) next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RenameTableRequest {
    pub(super) source: TableIdent,
    pub(super) destination: TableIdent,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct LoadTableResponse {
    pub(super) metadata_location: Option<String>,
    pub(super) metadata: TableMetadata,
    pub(super) config: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct CreateTableRequest {
    pub(super) name: String,
    pub(super) location: Option<String>,
    pub(super) schema: Schema,
    pub(super) partition_spec: Option<UnboundPartitionSpec>,
    pub(super) write_order: Option<SortOrder>,
    pub(super) stage_create: Option<bool>,
    pub(super) properties: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct CommitTableRequest {
    pub(super) identifier: TableIdent,
    pub(super) requirements: Vec<TableRequirement>,
    pub(super) updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct CommitTableResponse {
    pub(super) metadata_location: String,
    pub(super) metadata: TableMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct RegisterTableRequest {
    pub(super) name: String,
    pub(super) metadata_location: String,
    pub(super) overwrite: Option<bool>,
}
