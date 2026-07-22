// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_debug_implementations)]

//! An API client for the [AWS Glue Schema Registry][gsr].
//!
//! This crate is the Glue analogue of [`mz-ccsr`], the Confluent Schema
//! Registry client. It is intentionally narrow: only the surface needed by
//! the current Materialize integration is implemented.
//!
//! Read path (source decode, DDL planning, connection validation):
//!
//! * [`Client::get_registry`] — used by `GlueSchemaRegistryConnection::validate`
//!   to verify a registry exists at `CREATE CONNECTION` time.
//! * [`Client::get_schema_version_by_id`] — source decode: fetch a writer
//!   schema by the UUID embedded in each record's Glue wire-format header.
//! * [`Client::get_schema_version_latest_by_name`] — DDL planning: pin a
//!   reader schema to the registry's current "latest" version.
//!
//! Write path (sink encode):
//!
//! * [`Client::get_schema_by_definition`] — reuse an already-registered
//!   definition so a sink restart does not create a duplicate version.
//! * [`Client::register_schema_version`] — add a new version to an existing
//!   schema.
//! * [`Client::create_schema`] — create a schema on first publish, setting its
//!   compatibility policy.
//! * [`Client::get_schema`] — read a schema's compatibility to warn on a
//!   mismatch without overwriting it.
//!
//! Glue has no separate get/update-compatibility API: a schema's compatibility
//! is set once at [`Client::create_schema`] and read back via
//! [`Client::get_schema`]. This crate deliberately exposes no way to change it,
//! matching the sink's set-if-unset policy.
//!
//! ## Example usage
//!
//! ```no_run
//! # async {
//! use mz_aws_glue_schema_registry::{Client, ClientConfig};
//! use aws_types::SdkConfig;
//!
//! let sdk_config: SdkConfig = unimplemented!("from AwsConnection::load_sdk_config");
//! let client = ClientConfig::new(sdk_config).build();
//! let registry = client.get_registry("my-registry").await?;
//! # let _ = registry;
//! # Ok::<_, mz_aws_glue_schema_registry::GetRegistryError>(())
//! # };
//! ```
//!
//! [gsr]: https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html
//! [`mz-ccsr`]: https://docs.rs/mz-ccsr

mod client;
mod config;

pub use client::{
    Client, Compatibility, CreateSchemaError, DataFormat, GetRegistryError,
    GetSchemaByDefinitionError, GetSchemaError, GetSchemaVersionError, RegisterSchemaVersionError,
    RegisteredSchemaVersion, Registry, RegistryLifecycleStatus, Schema, SchemaLifecycleStatus,
    SchemaVersion, SchemaVersionLifecycleStatus,
};
pub use config::ClientConfig;
