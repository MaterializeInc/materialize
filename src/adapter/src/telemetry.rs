// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry utilities.

use serde_json::json;

use mz_sql::catalog::EnvironmentId;

/// Extension trait for [`EnvironmentId`].
pub trait EnvironmentIdExt {
    /// Returns the Segment context fields associated with this environment ID.
    fn as_segment_context(&self) -> serde_json::Value;
}

impl EnvironmentIdExt for EnvironmentId {
    fn as_segment_context(&self) -> serde_json::Value {
        json!({
            "group_id": self.organization_id(),
            "cloud_provider": self.cloud_provider(),
            "cloud_provider_region": self.cloud_provider_region(),
        })
    }
}
