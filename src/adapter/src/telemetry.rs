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
use uuid::Uuid;

use mz_sql::catalog::EnvironmentId;

/// Extension trait for [`mz_segment::Client`].
pub trait SegmentClientExt {
    /// Tracks an event associated with an environment.
    fn environment_track<S>(
        &self,
        environment_id: &EnvironmentId,
        user_id: Uuid,
        event: S,
        properties: serde_json::Value,
    ) where
        S: Into<String>;
}

impl SegmentClientExt for mz_segment::Client {
    /// Tracks an event associated with an environment.
    ///
    /// Various metadata about the environment is automatically attached
    /// using canonical field names.
    ///
    /// # Panics
    ///
    /// Panics if `properties` is not a [`serde_json::Value::Object`].
    fn environment_track<S>(
        &self,
        environment_id: &EnvironmentId,
        user_id: Uuid,
        event: S,
        mut properties: serde_json::Value,
    ) where
        S: Into<String>,
    {
        {
            let properties = match &mut properties {
                serde_json::Value::Object(map) => map,
                _ => {
                    panic!("SegmentClientExt::environment_track called with non-object properties")
                }
            };
            properties.insert("event_source".into(), json!("environment"));
            properties.insert(
                "cloud_provider".into(),
                json!(environment_id.cloud_provider().to_string()),
            );
            properties.insert(
                "cloud_provider_region".into(),
                json!(environment_id.cloud_provider_region()),
            );
        }
        self.track(
            user_id,
            event,
            properties,
            Some(json!({ "group_id": environment_id.organization_id() })),
        );
    }
}
