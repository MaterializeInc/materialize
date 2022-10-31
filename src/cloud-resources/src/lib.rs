// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions for management of PrivateLink VpcEndpoint objects

use std::collections::HashSet;
use std::fmt::Debug;

use async_trait::async_trait;

use mz_repr::GlobalId;

pub mod crd;

use crate::crd::vpc_endpoint::v1::VpcEndpointSpec;

#[async_trait]
pub trait CloudResourceController: Debug + Send + Sync {
    /// Creates or updates the specified VpcEndpoint K8s object.
    async fn ensure_vpc_endpoint(
        &self,
        id: GlobalId,
        spec: VpcEndpointSpec,
    ) -> Result<(), anyhow::Error>;

    /// Deletes the specified VpcEndpoint K8S object.
    async fn delete_vpc_endpoint(&self, id: GlobalId) -> Result<(), anyhow::Error>;

    /// Lists existing VpcEndpoint K8S objects.
    async fn list_vpc_endpoints(&self) -> Result<HashSet<GlobalId>, anyhow::Error>;
}
