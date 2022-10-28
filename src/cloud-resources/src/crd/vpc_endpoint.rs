// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! VpcEndpoint custom resource, to be reconciled into an AWS VPC Endpoint by the
//! environment-controller.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod v1 {
    use super::*;

    #[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    #[kube(
        group = "materialize.cloud",
        version = "v1",
        kind = "VpcEndpoint",
        singular = "vpcendpoint",
        plural = "vpcendpoints",
        shortname = "vpce",
        namespaced,
        status = "VpcEndpointStatus",
        printcolumn = r#"{"name": "AwsServiceName", "type": "string", "description": "Name of the VPC Endpoint Service to connect to.", "jsonPath": ".spec.awsServiceName", "priority": 1}"#,
        printcolumn = r#"{"name": "AvailabilityZoneIDs", "type": "string", "description": "Availability Zone IDs", "jsonPath": ".spec.availabilityZoneIds", "priority": 1}"#
    )]
    // If making changes to this spec,
    // you must also update src/cloud-resources/gen/vpcendpoints.crd.json
    // so that cloudtest can register the CRD.
    pub struct VpcEndpointSpec {
        pub aws_service_name: String,
        pub availability_zone_ids: Vec<String>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct VpcEndpointStatus {}
}

#[cfg(test)]
mod tests {
    use std::fs;

    use kube::core::crd::merge_crds;
    use kube::CustomResourceExt;

    #[test]
    fn test_vpc_endpoint_crd_matches() {
        let crd = merge_crds(vec![super::v1::VpcEndpoint::crd()], "v1").unwrap();
        let crd_json = serde_json::to_string(&serde_json::json!(&crd)).unwrap();
        let exported_crd_json = fs::read_to_string("src/crd/gen/vpcendpoints.json").unwrap();
        let exported_crd_json = exported_crd_json.trim();
        assert_eq!(
            &crd_json, exported_crd_json,
            "VpcEndpoint CRD json does not match exported json.\n\nCRD:\n{}\n\nExported CRD:\n{}",
            &crd_json, exported_crd_json,
        );
    }
}
