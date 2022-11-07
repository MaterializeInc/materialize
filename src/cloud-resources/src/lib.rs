// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions for management of cloud resources that have no equivalent when running
//! locally, like AWS PrivateLink endpoints.

use std::collections::HashSet;
use std::fmt::{self, Debug};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use mz_repr::GlobalId;

pub mod crd;

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
    ) -> AwsExternalIdPrefix {
        AwsExternalIdPrefix(aws_external_id_prefix.into())
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
pub trait CloudResourceController: Debug + Send + Sync {
    /// Creates or updates the specified `VpcEndpoint` Kubernetes object.
    async fn ensure_vpc_endpoint(
        &self,
        id: GlobalId,
        vpc_endpoint: VpcEndpointConfig,
    ) -> Result<(), anyhow::Error>;

    /// Deletes the specified `VpcEndpoint` Kubernetes object.
    async fn delete_vpc_endpoint(&self, id: GlobalId) -> Result<(), anyhow::Error>;

    /// Lists existing `VpcEndpoint` Kubernetes objects.
    async fn list_vpc_endpoints(&self) -> Result<HashSet<GlobalId>, anyhow::Error>;
}

/// Returns the name to use for the VPC endpoint with the given ID.
pub fn vpc_endpoint_name(id: GlobalId) -> String {
    // This is part of the contract with the VpcEndpointController in the
    // cloud infrastructure layer.
    format!("connection-{id}")
}
