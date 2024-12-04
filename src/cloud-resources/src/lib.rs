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

pub mod crd;
#[cfg(feature = "vpc-endpoints")]
pub mod vpc_endpoint;
#[cfg(feature = "vpc-endpoints")]
pub use vpc_endpoint::{
    id_from_vpc_endpoint_name, vpc_endpoint_host, vpc_endpoint_name, AwsExternalIdPrefix,
    CloudResourceController, CloudResourceReader, VpcEndpointConfig, VpcEndpointEvent,
};
