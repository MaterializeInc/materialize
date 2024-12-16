// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::{self, Debug};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use mz_repr::CatalogItemId;
use mz_repr::Row;
use serde::{Deserialize, Serialize};

use crate::crd::vpc_endpoint::v1::{VpcEndpointState, VpcEndpointStatus};

/// A prefix for an [external ID] to use for all AWS AssumeRole operations. It
/// should be concatenanted with a non-user-provided suffix identifying the
/// source or sink. The ID used for the suffix should never be reused if the
/// source or sink is deleted.
///
/// **WARNING:** it is critical for security that this ID is **not** provided by
/// end users of Materialize. It must be provided by the operator of the
/// Materialize service.
///
/// This type protects against accidental construction of an
/// `AwsExternalIdPrefix` through the use of an unwieldy and overly descriptive
/// constructor method name.
///
/// [external ID]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsExternalIdPrefix(String);

impl AwsExternalIdPrefix {
    /// Creates a new AWS external ID prefix from a command-line argument or
    /// an environment variable.
    ///
    /// **WARNING:** it is critical for security that this ID is **not**
    /// provided by end users of Materialize. It must be provided by the
    /// operator of the Materialize service.
    ///
    pub fn new_from_cli_argument_or_environment_variable(
        aws_external_id_prefix: &str,
    ) -> Result<AwsExternalIdPrefix, Infallible> {
        Ok(AwsExternalIdPrefix(aws_external_id_prefix.into()))
    }
}

impl fmt::Display for AwsExternalIdPrefix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Configures a VPC endpoint.
pub struct VpcEndpointConfig {
    /// The name of the service to connect to.
    pub aws_service_name: String,
    /// The IDs of the availability zones in which the service is available.
    pub availability_zone_ids: Vec<String>,
}

#[async_trait]
pub trait CloudResourceController: CloudResourceReader {
    /// Creates or updates the specified `VpcEndpoint` Kubernetes object.
    async fn ensure_vpc_endpoint(
        &self,
        id: CatalogItemId,
        vpc_endpoint: VpcEndpointConfig,
    ) -> Result<(), anyhow::Error>;

    /// Deletes the specified `VpcEndpoint` Kubernetes object.
    async fn delete_vpc_endpoint(&self, id: CatalogItemId) -> Result<(), anyhow::Error>;

    /// Lists existing `VpcEndpoint` Kubernetes objects.
    async fn list_vpc_endpoints(
        &self,
    ) -> Result<BTreeMap<CatalogItemId, VpcEndpointStatus>, anyhow::Error>;

    /// Lists existing `VpcEndpoint` Kubernetes objects.
    async fn watch_vpc_endpoints(&self) -> BoxStream<'static, VpcEndpointEvent>;

    /// Returns a reader for the resources managed by this controller.
    fn reader(&self) -> Arc<dyn CloudResourceReader>;
}

#[async_trait]
pub trait CloudResourceReader: Debug + Send + Sync {
    /// Reads the specified `VpcEndpoint` Kubernetes object.
    async fn read(&self, id: CatalogItemId) -> Result<VpcEndpointStatus, anyhow::Error>;
}

/// Returns the name to use for the VPC endpoint with the given ID.
pub fn vpc_endpoint_name(id: CatalogItemId) -> String {
    // This is part of the contract with the VpcEndpointController in the
    // cloud infrastructure layer.
    format!("connection-{id}")
}

/// Attempts to return the ID used to create the given VPC endpoint name.
pub fn id_from_vpc_endpoint_name(vpc_endpoint_name: &str) -> Option<CatalogItemId> {
    vpc_endpoint_name
        .split_once('-')
        .and_then(|(_, id_str)| CatalogItemId::from_str(id_str).ok())
}

/// Returns the host to use for the VPC endpoint with the given ID and
/// optionally in the given availability zone.
pub fn vpc_endpoint_host(id: CatalogItemId, availability_zone: Option<&str>) -> String {
    let name = vpc_endpoint_name(id);
    // This naming scheme is part of the contract with the VpcEndpointController
    // in the cloud infrastructure layer.
    match availability_zone {
        Some(az) => format!("{name}-{az}"),
        None => name,
    }
}

#[derive(Debug)]
pub struct VpcEndpointEvent {
    pub connection_id: CatalogItemId,
    pub status: VpcEndpointState,
    pub time: DateTime<Utc>,
}

impl From<VpcEndpointEvent> for Row {
    fn from(value: VpcEndpointEvent) -> Self {
        use mz_repr::Datum;

        Row::pack_slice(&[
            Datum::TimestampTz(value.time.try_into().expect("must fit")),
            Datum::String(&value.connection_id.to_string()),
            Datum::String(&value.status.to_string()),
        ])
    }
}
