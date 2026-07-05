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
use aws_sdk_glue::operation::create_schema::CreateSchemaError as SdkCreateSchemaError;
use aws_sdk_glue::operation::get_registry::GetRegistryError as SdkGetRegistryError;
use aws_sdk_glue::operation::get_schema::GetSchemaError as SdkGetSchemaError;
use aws_sdk_glue::operation::get_schema_version::{
    GetSchemaVersionError as SdkGetSchemaVersionError, GetSchemaVersionOutput,
};
// Module aliases: these two operation error types share names with this crate's
// own error enums, so they must be qualified, and their fully-aliased `use`
// lines overflow the format width. A short module alias sidesteps both.
use aws_sdk_glue::operation::get_schema_by_definition as sdk_get_by_def;
use aws_sdk_glue::operation::register_schema_version as sdk_register;
use aws_sdk_glue::types::{
    Compatibility as SdkCompatibility, DataFormat as SdkDataFormat, RegistryId,
    RegistryStatus as SdkRegistryStatus, SchemaId, SchemaStatus as SdkSchemaStatus,
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

    /// Wraps an existing SDK client.
    ///
    /// Exists so tests can inject a mocked SDK client (e.g. via
    /// `aws_smithy_mocks`). Production callers construct clients through
    /// [`ClientConfig`](crate::ClientConfig) instead, which is why this is
    /// gated behind the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn from_sdk_client(inner: aws_sdk_glue::Client) -> Self {
        Client { inner }
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

    /// Look up the schema version whose definition byte-for-byte matches
    /// `definition`.
    ///
    /// This is the sink reuse path: before registering a new version, callers
    /// check whether the exact definition is already registered so that a sink
    /// restart does not create a duplicate version. Returns
    /// [`GetSchemaByDefinitionError::NotFound`] if the schema does not exist or
    /// has no version matching `definition`.
    ///
    /// The match is by definition only: Glue also matches versions whose
    /// lifecycle status is `Failure` or `Deleting`, so callers must check the
    /// returned status before reusing the version's id.
    pub async fn get_schema_by_definition(
        &self,
        registry_name: &str,
        schema_name: &str,
        definition: &str,
    ) -> Result<RegisteredSchemaVersion, GetSchemaByDefinitionError> {
        let schema_id = SchemaId::builder()
            .registry_name(registry_name)
            .schema_name(schema_name)
            .build();
        let output = self
            .inner
            .get_schema_by_definition()
            .schema_id(schema_id)
            .schema_definition(definition)
            .send()
            .await
            .map_err(classify_get_schema_by_definition_error)?;
        let id = parse_schema_version_id(output.schema_version_id)
            .map_err(GetSchemaByDefinitionError::Other)?;
        Ok(RegisteredSchemaVersion {
            id,
            lifecycle_status: output.status.map(SchemaVersionLifecycleStatus::from_sdk),
        })
    }

    /// Register `definition` as a new version of an existing schema.
    ///
    /// The schema `(registry_name, schema_name)` must already exist. Returns
    /// [`RegisterSchemaVersionError::SchemaNotFound`] if it does not, in which
    /// case the caller should create it with [`Client::create_schema`].
    /// Registering a definition identical to an existing version is idempotent
    /// on Glue's side and returns that version.
    ///
    /// Glue runs the compatibility check asynchronously: a newly registered
    /// version comes back `Pending` and only later transitions to `Available`
    /// or `Failure`. Callers must not use the version's id until they have
    /// observed it `Available`, polling via
    /// [`Client::get_schema_version_by_id`].
    pub async fn register_schema_version(
        &self,
        registry_name: &str,
        schema_name: &str,
        definition: &str,
    ) -> Result<RegisteredSchemaVersion, RegisterSchemaVersionError> {
        let schema_id = SchemaId::builder()
            .registry_name(registry_name)
            .schema_name(schema_name)
            .build();
        let output = self
            .inner
            .register_schema_version()
            .schema_id(schema_id)
            .schema_definition(definition)
            .send()
            .await
            .map_err(classify_register_schema_version_error)?;
        let id = parse_schema_version_id(output.schema_version_id)
            .map_err(RegisterSchemaVersionError::Other)?;
        Ok(RegisteredSchemaVersion {
            id,
            lifecycle_status: output.status.map(SchemaVersionLifecycleStatus::from_sdk),
        })
    }

    /// Create a schema in `registry_name` with `definition` as its first version
    /// and `compatibility` as its evolution policy, returning that first
    /// version.
    ///
    /// A first version has no prior version to be compatible with, so it is
    /// usually `Available` immediately, but callers should still confirm the
    /// returned status before using the version's id.
    ///
    /// Glue sets a schema's compatibility only at creation. This crate exposes
    /// no way to change it afterward, matching the sink's set-if-unset policy:
    /// an existing schema's compatibility is read (via [`Client::get_schema`])
    /// and warned on, never overwritten.
    ///
    /// Returns [`CreateSchemaError::AlreadyExists`] if the schema already exists
    /// and [`CreateSchemaError::RegistryNotFound`] if the registry does not.
    pub async fn create_schema(
        &self,
        registry_name: &str,
        schema_name: &str,
        data_format: DataFormat,
        compatibility: Compatibility,
        definition: &str,
    ) -> Result<RegisteredSchemaVersion, CreateSchemaError> {
        let registry_id = RegistryId::builder().registry_name(registry_name).build();
        let output = self
            .inner
            .create_schema()
            .registry_id(registry_id)
            .schema_name(schema_name)
            .data_format(data_format.to_sdk())
            .compatibility(compatibility.to_sdk())
            .schema_definition(definition)
            .send()
            .await
            .map_err(classify_create_schema_error)?;
        let id =
            parse_schema_version_id(output.schema_version_id).map_err(CreateSchemaError::Other)?;
        Ok(RegisteredSchemaVersion {
            id,
            lifecycle_status: output
                .schema_version_status
                .map(SchemaVersionLifecycleStatus::from_sdk),
        })
    }

    /// Fetch a schema's metadata by `(registry_name, schema_name)`.
    ///
    /// The sink uses this to read the current compatibility policy so it can
    /// warn on a mismatch without overwriting it, and to detect whether the
    /// schema already exists. Returns [`GetSchemaError::NotFound`] if the schema
    /// does not exist.
    pub async fn get_schema(
        &self,
        registry_name: &str,
        schema_name: &str,
    ) -> Result<Schema, GetSchemaError> {
        let schema_id = SchemaId::builder()
            .registry_name(registry_name)
            .schema_name(schema_name)
            .build();
        let output = self
            .inner
            .get_schema()
            .schema_id(schema_id)
            .send()
            .await
            .map_err(classify_get_schema_error)?;
        Ok(Schema {
            compatibility: output.compatibility.map(Compatibility::from_sdk),
            lifecycle_status: output.schema_status.map(SchemaLifecycleStatus::from_sdk),
        })
    }
}

/// Parse a schema-version UUID from a Glue response, mapping a missing or
/// malformed id to a diagnostic string for the caller to wrap.
fn parse_schema_version_id(id: Option<String>) -> Result<Uuid, String> {
    let id = id.ok_or_else(|| "Glue response missing schema version id".to_string())?;
    Uuid::parse_str(&id).map_err(|e| format!("invalid Glue schema version id {id:?}: {e}"))
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

/// A schema version's identity and lifecycle status, as returned by the write
/// path methods [`Client::get_schema_by_definition`],
/// [`Client::register_schema_version`], and [`Client::create_schema`].
///
/// Glue validates new versions asynchronously, so `lifecycle_status` is often
/// `Pending` here. A version is only usable for framing records once it is
/// `Available`. Callers holding a non-`Available` status must poll
/// [`Client::get_schema_version_by_id`] until it resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchemaVersion {
    pub id: Uuid,
    pub lifecycle_status: Option<SchemaVersionLifecycleStatus>,
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

    fn to_sdk(&self) -> SdkDataFormat {
        match self {
            DataFormat::Avro => SdkDataFormat::Avro,
            DataFormat::Json => SdkDataFormat::Json,
            DataFormat::Protobuf => SdkDataFormat::Protobuf,
            DataFormat::Unknown(s) => SdkDataFormat::from(s.as_str()),
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

/// A schema's evolution policy, as accepted by [`Client::create_schema`] and
/// returned by [`Client::get_schema`].
///
/// Mirrors `aws_sdk_glue::types::Compatibility`, with `Unknown(String)` as the
/// forward-compat escape hatch. The `*All` variants are Glue's transitive
/// modes (a new schema must be compatible with every prior version, not just
/// the latest). `Disabled` turns off enforcement entirely and has no Confluent
/// analogue. See [`RegistryLifecycleStatus`] for the mirroring rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compatibility {
    None,
    Disabled,
    Backward,
    BackwardAll,
    Forward,
    ForwardAll,
    Full,
    FullAll,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl Compatibility {
    fn from_sdk(compatibility: SdkCompatibility) -> Self {
        match &compatibility {
            SdkCompatibility::None => Compatibility::None,
            SdkCompatibility::Disabled => Compatibility::Disabled,
            SdkCompatibility::Backward => Compatibility::Backward,
            SdkCompatibility::BackwardAll => Compatibility::BackwardAll,
            SdkCompatibility::Forward => Compatibility::Forward,
            SdkCompatibility::ForwardAll => Compatibility::ForwardAll,
            SdkCompatibility::Full => Compatibility::Full,
            SdkCompatibility::FullAll => Compatibility::FullAll,
            _ => Compatibility::Unknown(compatibility.as_str().to_string()),
        }
    }

    fn to_sdk(&self) -> SdkCompatibility {
        match self {
            Compatibility::None => SdkCompatibility::None,
            Compatibility::Disabled => SdkCompatibility::Disabled,
            Compatibility::Backward => SdkCompatibility::Backward,
            Compatibility::BackwardAll => SdkCompatibility::BackwardAll,
            Compatibility::Forward => SdkCompatibility::Forward,
            Compatibility::ForwardAll => SdkCompatibility::ForwardAll,
            Compatibility::Full => SdkCompatibility::Full,
            Compatibility::FullAll => SdkCompatibility::FullAll,
            Compatibility::Unknown(s) => SdkCompatibility::from(s.as_str()),
        }
    }
}

/// A Glue schema's metadata, as returned by [`Client::get_schema`].
///
/// Only the fields the sink needs are surfaced. `compatibility` drives the
/// warn-on-mismatch check; `lifecycle_status` distinguishes a live schema from
/// one mid-deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub compatibility: Option<Compatibility>,
    pub lifecycle_status: Option<SchemaLifecycleStatus>,
}

/// Lifecycle status of a Glue schema.
///
/// Mirrors `aws_sdk_glue::types::SchemaStatus`. Distinct from
/// [`SchemaVersionLifecycleStatus`], which tracks an individual version. See
/// [`RegistryLifecycleStatus`] for the mirroring rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaLifecycleStatus {
    Available,
    Pending,
    Deleting,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl SchemaLifecycleStatus {
    fn from_sdk(status: SdkSchemaStatus) -> Self {
        match &status {
            SdkSchemaStatus::Available => SchemaLifecycleStatus::Available,
            SdkSchemaStatus::Pending => SchemaLifecycleStatus::Pending,
            SdkSchemaStatus::Deleting => SchemaLifecycleStatus::Deleting,
            _ => SchemaLifecycleStatus::Unknown(status.as_str().to_string()),
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

/// Errors returned by [`Client::get_schema_by_definition`].
#[derive(Debug, Error)]
pub enum GetSchemaByDefinitionError {
    /// No schema version with the given definition exists (either the schema is
    /// absent or none of its versions match). Maps from Glue's
    /// `EntityNotFoundException`.
    #[error("schema version for definition not found")]
    NotFound,
    /// Anything else: auth failure, throttling, transport error, or a malformed
    /// version id in the response.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_schema_by_definition_error(
    err: SdkError<sdk_get_by_def::GetSchemaByDefinitionError>,
) -> GetSchemaByDefinitionError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            sdk_get_by_def::GetSchemaByDefinitionError::EntityNotFoundException(_)
        )
    {
        return GetSchemaByDefinitionError::NotFound;
    }
    GetSchemaByDefinitionError::Other(DisplayErrorContext(&err).to_string())
}

/// Errors returned by [`Client::register_schema_version`].
#[derive(Debug, Error)]
pub enum RegisterSchemaVersionError {
    /// The target schema does not exist, so there is nothing to add a version
    /// to. The caller should create it with [`Client::create_schema`]. Maps
    /// from Glue's `EntityNotFoundException`.
    #[error("schema not found")]
    SchemaNotFound,
    /// Anything else: auth failure, throttling, transport error, or a malformed
    /// version id in the response.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_register_schema_version_error(
    err: SdkError<sdk_register::RegisterSchemaVersionError>,
) -> RegisterSchemaVersionError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            sdk_register::RegisterSchemaVersionError::EntityNotFoundException(_)
        )
    {
        return RegisterSchemaVersionError::SchemaNotFound;
    }
    RegisterSchemaVersionError::Other(DisplayErrorContext(&err).to_string())
}

/// Errors returned by [`Client::create_schema`].
#[derive(Debug, Error)]
pub enum CreateSchemaError {
    /// A schema with this name already exists in the registry. The caller
    /// should reuse it via [`Client::get_schema_by_definition`] and
    /// [`Client::register_schema_version`]. Maps from Glue's
    /// `AlreadyExistsException`.
    #[error("schema already exists")]
    AlreadyExists,
    /// The target registry does not exist. Maps from Glue's
    /// `EntityNotFoundException`.
    #[error("registry not found")]
    RegistryNotFound,
    /// Anything else: auth failure, throttling, transport error, or a malformed
    /// version id in the response.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_create_schema_error(err: SdkError<SdkCreateSchemaError>) -> CreateSchemaError {
    if let SdkError::ServiceError(service_err) = &err {
        match service_err.err() {
            SdkCreateSchemaError::AlreadyExistsException(_) => {
                return CreateSchemaError::AlreadyExists;
            }
            SdkCreateSchemaError::EntityNotFoundException(_) => {
                return CreateSchemaError::RegistryNotFound;
            }
            _ => {}
        }
    }
    CreateSchemaError::Other(DisplayErrorContext(&err).to_string())
}

/// Errors returned by [`Client::get_schema`].
#[derive(Debug, Error)]
pub enum GetSchemaError {
    /// The schema does not exist. Maps from Glue's `EntityNotFoundException`.
    #[error("schema not found")]
    NotFound,
    /// Anything else: auth failure, throttling, transport error, etc.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_schema_error(err: SdkError<SdkGetSchemaError>) -> GetSchemaError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            SdkGetSchemaError::EntityNotFoundException(_)
        )
    {
        return GetSchemaError::NotFound;
    }
    GetSchemaError::Other(DisplayErrorContext(&err).to_string())
}

#[cfg(test)]
mod tests {
    use aws_sdk_glue::operation::create_schema::CreateSchemaOutput;
    use aws_sdk_glue::operation::get_schema_by_definition::GetSchemaByDefinitionOutput;
    use aws_sdk_glue::operation::register_schema_version::RegisterSchemaVersionOutput;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    use super::*;

    const VERSION_ID: &str = "12345678-1234-5678-1234-567812345678";

    /// The write methods must surface the lifecycle status from each response:
    /// Glue validates versions asynchronously, so callers gate on it before
    /// using a version's id.
    #[mz_ore::test(tokio::test)]
    async fn write_methods_surface_lifecycle_status() {
        let register = mock!(aws_sdk_glue::Client::register_schema_version).then_output(|| {
            RegisterSchemaVersionOutput::builder()
                .schema_version_id(VERSION_ID)
                .status(SdkSchemaVersionStatus::Pending)
                .build()
        });
        let by_definition =
            mock!(aws_sdk_glue::Client::get_schema_by_definition).then_output(|| {
                GetSchemaByDefinitionOutput::builder()
                    .schema_version_id(VERSION_ID)
                    .status(SdkSchemaVersionStatus::Failure)
                    .build()
            });
        let create = mock!(aws_sdk_glue::Client::create_schema).then_output(|| {
            CreateSchemaOutput::builder()
                .schema_version_id(VERSION_ID)
                .schema_version_status(SdkSchemaVersionStatus::Available)
                .build()
        });
        let client = Client {
            inner: mock_client!(
                aws_sdk_glue,
                RuleMode::MatchAny,
                &[&register, &by_definition, &create]
            ),
        };

        let id = Uuid::parse_str(VERSION_ID).expect("valid uuid literal");
        assert_eq!(
            client
                .register_schema_version("registry", "schema", "{}")
                .await
                .expect("mocked register succeeds"),
            RegisteredSchemaVersion {
                id,
                lifecycle_status: Some(SchemaVersionLifecycleStatus::Pending),
            }
        );
        assert_eq!(
            client
                .get_schema_by_definition("registry", "schema", "{}")
                .await
                .expect("mocked lookup succeeds"),
            RegisteredSchemaVersion {
                id,
                lifecycle_status: Some(SchemaVersionLifecycleStatus::Failure),
            }
        );
        assert_eq!(
            client
                .create_schema(
                    "registry",
                    "schema",
                    DataFormat::Avro,
                    Compatibility::Backward,
                    "{}",
                )
                .await
                .expect("mocked create succeeds"),
            RegisteredSchemaVersion {
                id,
                lifecycle_status: Some(SchemaVersionLifecycleStatus::Available),
            }
        );
    }

    #[mz_ore::test]
    fn parse_schema_version_id_valid() {
        let uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        assert_eq!(parse_schema_version_id(Some(uuid.to_string())), Ok(uuid));
    }

    #[mz_ore::test]
    fn parse_schema_version_id_missing() {
        let err = parse_schema_version_id(None).unwrap_err();
        assert!(err.contains("missing schema version id"), "{err}");
    }

    #[mz_ore::test]
    fn parse_schema_version_id_malformed() {
        let err = parse_schema_version_id(Some("not-a-uuid".to_string())).unwrap_err();
        assert!(err.contains("invalid Glue schema version id"), "{err}");
    }

    #[mz_ore::test]
    fn compatibility_sdk_round_trip() {
        // Every known variant must survive to_sdk -> from_sdk unchanged, so the
        // CSR-to-Glue mapping in the sink can rely on stable identities.
        for c in [
            Compatibility::None,
            Compatibility::Disabled,
            Compatibility::Backward,
            Compatibility::BackwardAll,
            Compatibility::Forward,
            Compatibility::ForwardAll,
            Compatibility::Full,
            Compatibility::FullAll,
        ] {
            assert_eq!(Compatibility::from_sdk(c.to_sdk()), c);
        }
    }

    #[mz_ore::test]
    fn data_format_sdk_round_trip() {
        for f in [DataFormat::Avro, DataFormat::Json, DataFormat::Protobuf] {
            assert_eq!(DataFormat::from_sdk(f.to_sdk()), f);
        }
    }
}
