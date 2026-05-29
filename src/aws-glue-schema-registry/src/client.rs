// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Glue Schema Registry client.

use aws_sdk_glue::error::{DisplayErrorContext, SdkError};
use aws_sdk_glue::operation::get_registry::GetRegistryError as SdkGetRegistryError;
use aws_sdk_glue::operation::get_schema_version::{
    GetSchemaVersionError as SdkGetSchemaVersionError, GetSchemaVersionOutput,
};
use aws_sdk_glue::types::{
    DataFormat as SdkDataFormat, RegistryId, RegistryStatus as SdkRegistryStatus, SchemaId,
    SchemaVersionNumber, SchemaVersionStatus as SdkSchemaVersionStatus,
};
use aws_types::SdkConfig;
use thiserror::Error;
use uuid::Uuid;

/// An API client for the AWS Glue Schema Registry.
///
/// `Client` is cheap to clone — internally it wraps an [`aws_sdk_glue::Client`],
/// which is itself a clone-friendly handle backed by a shared connection pool.
#[derive(Clone, Debug)]
pub struct Client {
    inner: aws_sdk_glue::Client,
}

impl Client {
    pub(crate) fn from_sdk_config(sdk_config: SdkConfig) -> Self {
        Client {
            inner: aws_sdk_glue::Client::new(&sdk_config),
        }
    }

    /// Look up a registry by name.
    ///
    /// Returns [`GetRegistryError::NotFound`] if the registry does not exist
    /// in the configured account and region. Other errors (auth failures,
    /// throttling, transport) surface as [`GetRegistryError::Other`].
    pub async fn get_registry(&self, name: &str) -> Result<Registry, GetRegistryError> {
        let id = RegistryId::builder().registry_name(name).build();
        let output = self
            .inner
            .get_registry()
            .registry_id(id)
            .send()
            .await
            .map_err(classify_get_registry_error)?;
        Ok(Registry {
            name: output.registry_name,
            arn: output.registry_arn,
            description: output.description,
            lifecycle_status: output.status.map(RegistryLifecycleStatus::from_sdk),
        })
    }

    /// Fetch a schema version by its UUID.
    ///
    /// This is the source-decode path: the UUID is read from the Glue
    /// wire-format header, and the returned `SchemaVersion::definition`
    /// carries the writer schema (Avro JSON, for our usage).
    ///
    /// Glue schema-version UUIDs are globally unique within an AWS account
    /// and this call does **not** scope to any registry — it returns the
    /// matching version from anywhere the configured credentials can read.
    /// Returns [`GetSchemaVersionError::NotFound`] only when no schema
    /// version with this UUID exists in any visible registry. Callers that
    /// need to enforce a specific registry must check the returned
    /// `SchemaArn` themselves.
    ///
    /// The AWS `GetSchemaVersion` API accepts *either* a `SchemaVersionId`
    /// (the UUID, registry-agnostic) *or* `SchemaId + SchemaVersionNumber`,
    /// never both — which is why looking up by name is a separate method,
    /// [`Client::get_schema_version_latest_by_name`].
    pub async fn get_schema_version_by_id(
        &self,
        id: Uuid,
    ) -> Result<SchemaVersion, GetSchemaVersionError> {
        let output = self
            .inner
            .get_schema_version()
            .schema_version_id(id.to_string())
            .send()
            .await
            .map_err(classify_get_schema_version_error)?;
        Ok(SchemaVersion::from_sdk(output))
    }

    /// Fetch the latest version of a schema by `(registry_name, schema_name)`.
    ///
    /// This is the DDL-planning path: at `CREATE SOURCE` time we don't yet
    /// know any per-record UUIDs, so we pin the reader schema to whatever
    /// the registry currently calls "latest". Runtime schema resolution
    /// for individual records still goes through
    /// [`Client::get_schema_version_by_id`] using the UUID in each Kafka
    /// payload's Glue header.
    pub async fn get_schema_version_latest_by_name(
        &self,
        registry_name: &str,
        schema_name: &str,
    ) -> Result<SchemaVersion, GetSchemaVersionError> {
        let schema_id = SchemaId::builder()
            .registry_name(registry_name)
            .schema_name(schema_name)
            .build();
        let version_number = SchemaVersionNumber::builder().latest_version(true).build();
        let output = self
            .inner
            .get_schema_version()
            .schema_id(schema_id)
            .schema_version_number(version_number)
            .send()
            .await
            .map_err(classify_get_schema_version_error)?;
        Ok(SchemaVersion::from_sdk(output))
    }
}

/// A Glue Schema Registry, as returned by [`Client::get_registry`].
///
/// Only the fields Materialize currently cares about are surfaced; the full
/// SDK type carries a few additional timestamps that we ignore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    pub name: Option<String>,
    pub arn: Option<String>,
    pub description: Option<String>,
    pub lifecycle_status: Option<RegistryLifecycleStatus>,
}

/// Lifecycle status of a Glue registry.
///
/// Mirrors `aws_sdk_glue::types::RegistryStatus`, with `Unknown(String)` as
/// the forward-compat escape hatch for variants AWS may add later. Keeping
/// our own enum means callers get exhaustive matching without taking a
/// direct dependency on the SDK type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryLifecycleStatus {
    Available,
    Deleting,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl RegistryLifecycleStatus {
    fn from_sdk(status: SdkRegistryStatus) -> Self {
        match &status {
            SdkRegistryStatus::Available => RegistryLifecycleStatus::Available,
            SdkRegistryStatus::Deleting => RegistryLifecycleStatus::Deleting,
            _ => RegistryLifecycleStatus::Unknown(status.as_str().to_string()),
        }
    }
}

/// Errors returned by [`Client::get_registry`].
#[derive(Debug, Error)]
pub enum GetRegistryError {
    /// The named registry does not exist in the configured account/region.
    /// Maps from Glue's `EntityNotFoundException`.
    #[error("registry not found")]
    NotFound,
    /// Anything else: auth failure, throttling, transport error, etc.
    /// The wrapped message preserves the upstream SDK's diagnostic.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_registry_error(err: SdkError<SdkGetRegistryError>) -> GetRegistryError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            SdkGetRegistryError::EntityNotFoundException(_)
        )
    {
        return GetRegistryError::NotFound;
    }
    GetRegistryError::Other(DisplayErrorContext(&err).to_string())
}

/// A Glue schema version, as returned by [`Client::get_schema_version_by_id`]
/// and [`Client::get_schema_version_latest_by_name`].
///
/// `definition` is the format-specific schema text; for Avro it is a JSON
/// document the Avro parser can ingest directly. The remaining fields are
/// informational and exist for debug logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaVersion {
    pub schema_version_id: Option<String>,
    pub schema_arn: Option<String>,
    /// The format-specific schema text (Avro JSON, JSON Schema, etc.).
    pub definition: Option<String>,
    /// Glue's data-format tag — `AVRO`, `JSON`, or `PROTOBUF`. Mirrors the
    /// SDK enum so callers get exhaustive matching without depending on the
    /// SDK type directly.
    pub data_format: Option<DataFormat>,
    pub version_number: Option<i64>,
    pub lifecycle_status: Option<SchemaVersionLifecycleStatus>,
}

impl SchemaVersion {
    fn from_sdk(output: GetSchemaVersionOutput) -> Self {
        SchemaVersion {
            schema_version_id: output.schema_version_id,
            schema_arn: output.schema_arn,
            definition: output.schema_definition,
            data_format: output.data_format.map(DataFormat::from_sdk),
            version_number: output.version_number,
            lifecycle_status: output.status.map(SchemaVersionLifecycleStatus::from_sdk),
        }
    }
}

/// Data format of a Glue schema.
///
/// Mirrors `aws_sdk_glue::types::DataFormat`, with `Unknown(String)` as the
/// forward-compat escape hatch for variants AWS may add later. See
/// [`RegistryLifecycleStatus`] for the rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataFormat {
    Avro,
    Json,
    Protobuf,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl DataFormat {
    fn from_sdk(format: SdkDataFormat) -> Self {
        match &format {
            SdkDataFormat::Avro => DataFormat::Avro,
            SdkDataFormat::Json => DataFormat::Json,
            SdkDataFormat::Protobuf => DataFormat::Protobuf,
            _ => DataFormat::Unknown(format.as_str().to_string()),
        }
    }

    /// Returns the canonical Glue data-format string (`AVRO`, `JSON`,
    /// `PROTOBUF`), or the raw SDK-reported value for `Unknown`.
    pub fn as_str(&self) -> &str {
        match self {
            DataFormat::Avro => "AVRO",
            DataFormat::Json => "JSON",
            DataFormat::Protobuf => "PROTOBUF",
            DataFormat::Unknown(s) => s,
        }
    }
}

/// Lifecycle status of a Glue schema version.
///
/// Mirrors `aws_sdk_glue::types::SchemaVersionStatus`. See
/// [`RegistryLifecycleStatus`] for the rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaVersionLifecycleStatus {
    Available,
    Pending,
    Failure,
    Deleting,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl SchemaVersionLifecycleStatus {
    fn from_sdk(status: SdkSchemaVersionStatus) -> Self {
        match &status {
            SdkSchemaVersionStatus::Available => SchemaVersionLifecycleStatus::Available,
            SdkSchemaVersionStatus::Pending => SchemaVersionLifecycleStatus::Pending,
            SdkSchemaVersionStatus::Failure => SchemaVersionLifecycleStatus::Failure,
            SdkSchemaVersionStatus::Deleting => SchemaVersionLifecycleStatus::Deleting,
            _ => SchemaVersionLifecycleStatus::Unknown(status.as_str().to_string()),
        }
    }
}

/// Errors returned by [`Client::get_schema_version_by_id`] and
/// [`Client::get_schema_version_latest_by_name`].
#[derive(Debug, Error)]
pub enum GetSchemaVersionError {
    /// No matching schema version exists in the configured account/region.
    /// Maps from Glue's `EntityNotFoundException`.
    #[error("schema version not found")]
    NotFound,
    /// Anything else: auth failure, throttling, transport error, etc.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_schema_version_error(
    err: SdkError<SdkGetSchemaVersionError>,
) -> GetSchemaVersionError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            SdkGetSchemaVersionError::EntityNotFoundException(_)
        )
    {
        return GetSchemaVersionError::NotFound;
    }
    GetSchemaVersionError::Other(DisplayErrorContext(&err).to_string())
}
