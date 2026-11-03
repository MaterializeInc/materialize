// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry utilities.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use itertools::Itertools;
use mz_audit_log::ObjectType;
use mz_sql::catalog::EnvironmentId;
use mz_sql::feature_usage::FeatureSet;
use mz_sql_parser::ast::StatementKind;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// Details to attach to a Segment event.
#[derive(Debug, Clone, Default)]
pub struct EventDetails<'a> {
    /// The ID of the user that triggered the event, if any.
    pub user_id: Option<Uuid>,
    /// The value of the `application_name` parameter for the session that
    /// triggered the event, if any.
    pub application_name: Option<&'a str>,
    /// The timestamp at which the event occurred, if not the current time.
    pub timestamp: Option<DateTime<Utc>>,
}

/// Extension trait for [`mz_segment::Client`].
pub trait SegmentClientExt {
    /// Tracks an event associated with an environment.
    fn environment_track<S>(
        &self,
        environment_id: &EnvironmentId,
        event: S,
        properties: serde_json::Value,
        details: EventDetails<'_>,
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
        event: S,
        mut properties: serde_json::Value,
        EventDetails {
            user_id,
            timestamp,
            application_name,
        }: EventDetails<'_>,
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
            if let Some(application_name) = application_name {
                properties.insert("application_name".into(), json!(application_name));
            }
        }

        // "Context" is a defined dictionary of extra information related to a datapoint. Please
        // consult the docs before adding anything here: https://segment.com/docs/connections/spec/common/#context
        let context = json!({
            "group_id": environment_id.organization_id()
        });

        // We use the organization ID as the user ID for events that are not
        // associated with a particular user.
        let user_id = user_id.unwrap_or_else(|| environment_id.organization_id());

        self.track(user_id, event, properties, Some(context), timestamp);
    }
}

/// Describes a way in which DDL statement execution can fail.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StatementFailureType {
    /// The statement failed to parse.
    ParseFailure,
}

impl StatementFailureType {
    /// Renders the name of failure type as a title case string.
    pub fn as_title_case(&self) -> &'static str {
        match self {
            StatementFailureType::ParseFailure => "Parse Failed",
        }
    }
}

serde_plain::derive_display_from_serialize!(StatementFailureType);

/// Describes the action of a DDL statement.
#[derive(Serialize, Deserialize)]
pub enum StatementAction {
    /// The statement alters an object.
    Alter,
    /// The statement creates an object.
    Create,
}

impl StatementAction {
    /// Renders the DDL action as a title case string.
    pub fn as_title_case(&self) -> &'static str {
        match self {
            StatementAction::Alter => "Alter",
            StatementAction::Create => "Create",
        }
    }
}

/// Analyzes the action and object type of DDL statement kinds.
///
/// Returns `None` if the given statement kind is not a tracked DDL `StatementKind`. Tracked
/// statements are those which are written to the audit log.
pub fn analyze_audited_statement(
    statement: StatementKind,
) -> Option<(StatementAction, ObjectType)> {
    match statement {
        StatementKind::AlterConnection => Some((StatementAction::Alter, ObjectType::Connection)),
        StatementKind::AlterIndex => Some((StatementAction::Alter, ObjectType::Index)),
        StatementKind::AlterRole => Some((StatementAction::Alter, ObjectType::Role)),
        StatementKind::AlterSecret => Some((StatementAction::Alter, ObjectType::Secret)),
        StatementKind::AlterSource => Some((StatementAction::Alter, ObjectType::Source)),
        StatementKind::CreateCluster => Some((StatementAction::Create, ObjectType::Cluster)),
        StatementKind::CreateClusterReplica => {
            Some((StatementAction::Create, ObjectType::ClusterReplica))
        }
        StatementKind::CreateConnection => Some((StatementAction::Create, ObjectType::Connection)),
        StatementKind::CreateDatabase => Some((StatementAction::Create, ObjectType::Database)),
        StatementKind::CreateIndex => Some((StatementAction::Create, ObjectType::Index)),
        StatementKind::CreateMaterializedView => {
            Some((StatementAction::Create, ObjectType::MaterializedView))
        }
        StatementKind::CreateRole => Some((StatementAction::Create, ObjectType::Role)),
        StatementKind::CreateSchema => Some((StatementAction::Create, ObjectType::Schema)),
        StatementKind::CreateSecret => Some((StatementAction::Create, ObjectType::Secret)),
        StatementKind::CreateSink => Some((StatementAction::Create, ObjectType::Sink)),
        StatementKind::CreateSource => Some((StatementAction::Create, ObjectType::Source)),
        StatementKind::CreateTable => Some((StatementAction::Create, ObjectType::Table)),
        StatementKind::CreateView => Some((StatementAction::Create, ObjectType::View)),
        _ => None,
    }
}

/// The most distinct feature sets [`FeatureUsage`] tracks between drains.
/// Executions of further sets are only counted in
/// [`FeatureUsageSnapshot::overflow`]. Bounds memory and the number of
/// Segment events per report.
pub const MAX_FEATURE_SETS: usize = 500;

/// Execution counts of distinct SQL feature sets, accumulated until the
/// telemetry loop drains them.
///
/// Counting whole sets, rather than individual features, preserves which
/// features were used together in one statement.
#[derive(Debug, Clone, Default)]
pub struct FeatureUsage {
    inner: Arc<Mutex<FeatureUsageSnapshot>>,
}

/// The feature set counts accumulated since the previous drain.
#[derive(Debug, Default)]
pub struct FeatureUsageSnapshot {
    /// Keyed by application name hint and the rendered, comma-separated
    /// feature set.
    pub counts: BTreeMap<(&'static str, String), u64>,
    /// Executions whose feature set was not tracked because
    /// [`MAX_FEATURE_SETS`] distinct sets had already been seen.
    pub overflow: u64,
}

impl FeatureUsage {
    pub fn record(&self, application_name: &'static str, features: &FeatureSet) {
        let key = (application_name, features.iter().join(","));
        let mut inner = self.inner.lock().expect("lock poisoned");
        let at_capacity = inner.counts.len() >= MAX_FEATURE_SETS;
        match inner.counts.get_mut(&key) {
            Some(count) => *count += 1,
            None if at_capacity => inner.overflow += 1,
            None => {
                inner.counts.insert(key, 1);
            }
        }
    }

    /// Returns the counts accumulated since the previous call and resets them.
    pub fn drain(&self) -> FeatureUsageSnapshot {
        std::mem::take(&mut *self.inner.lock().expect("lock poisoned"))
    }
}

#[cfg(test)]
mod tests {
    use mz_sql::feature_usage::{Feature, FeatureCategory};

    use super::*;

    fn feature_set(name: &str) -> FeatureSet {
        [Feature {
            category: FeatureCategory::Function,
            name: name.to_string().into(),
        }]
        .into()
    }

    #[mz_ore::test]
    fn feature_usage_counts_sets_and_overflows() {
        let usage = FeatureUsage::default();
        usage.record("psql", &feature_set("a"));
        usage.record("psql", &feature_set("a"));
        usage.record("dbt", &feature_set("a"));
        let snapshot = usage.drain();
        assert_eq!(snapshot.counts[&("psql", "fn:a".to_string())], 2);
        assert_eq!(snapshot.counts[&("dbt", "fn:a".to_string())], 1);
        assert!(usage.drain().counts.is_empty(), "drain resets the counts");

        for i in 0..MAX_FEATURE_SETS {
            usage.record("psql", &feature_set(&i.to_string()));
        }
        usage.record("psql", &feature_set("0"));
        usage.record("psql", &feature_set("new"));
        let snapshot = usage.drain();
        assert_eq!(snapshot.counts.len(), MAX_FEATURE_SETS);
        assert_eq!(snapshot.counts[&("psql", "fn:0".to_string())], 2);
        assert_eq!(snapshot.overflow, 1, "only unseen sets overflow");
    }
}
