// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry utilities.

pub use mz_audit_log::ObjectType;
use mz_sql::catalog::EnvironmentId;
use mz_sql_parser::{ast::StatementKind, parser::ParserError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// Extension trait for [`mz_segment::Client`].
pub trait SegmentClientExt {
    /// Tracks an event associated with an environment.
    fn environment_track<S>(
        &self,
        environment_id: &EnvironmentId,
        app_name: &str,
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
        app_name: &str,
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
            properties.insert("application_name".into(), json!(app_name));
        }

        // "Context" is a defined dictionary of extra information related to a datapoint. Please
        // consult the docs before adding anything here: https://segment.com/docs/connections/spec/common/#context
        let context = json!({
            "group_id": environment_id.organization_id()
        });

        self.track(user_id, event, properties, Some(context));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StatementFailureType {
    ParseFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatementFailureEvent {
    pub event_type: StatementFailureType,
    pub object_type: ObjectType,
    pub statement_kind: StatementKind,
    pub error: ParserError,
}

impl StatementFailureEvent {
    pub fn new(
        event_type: StatementFailureType,
        object_type: ObjectType,
        parser_error: ParserError,
        statement_kind: StatementKind,
    ) -> StatementFailureEvent {
        StatementFailureEvent {
            event_type,
            object_type,
            error: parser_error,
            statement_kind,
        }
    }
}

impl StatementFailureType {
    pub fn as_title_case(&self) -> &'static str {
        match self {
            StatementFailureType::ParseFailure => "Parse Failed",
        }
    }
}

serde_plain::derive_display_from_serialize!(StatementFailureType);

#[derive(Serialize, Deserialize)]
pub enum StatementAction {
    Alter,
    Create,
}

impl StatementAction {
    pub fn as_title_case(&self) -> &'static str {
        match self {
            StatementAction::Alter => "Alter",
            StatementAction::Create => "Create",
        }
    }
}

pub fn statement_object_type(
    statement: Option<StatementKind>,
) -> Option<(StatementAction, ObjectType)> {
    statement
        .map(|s| match s {
            StatementKind::AlterConnection => {
                Some((StatementAction::Alter, ObjectType::Connection))
            }
            StatementKind::AlterIndex => Some((StatementAction::Alter, ObjectType::Index)),
            StatementKind::AlterRole => Some((StatementAction::Alter, ObjectType::Role)),
            StatementKind::AlterSecret => Some((StatementAction::Alter, ObjectType::Secret)),
            StatementKind::AlterSink => Some((StatementAction::Alter, ObjectType::Sink)),
            StatementKind::AlterSource => Some((StatementAction::Alter, ObjectType::Source)),
            StatementKind::CreateCluster => Some((StatementAction::Create, ObjectType::Cluster)),
            StatementKind::CreateClusterReplica => {
                Some((StatementAction::Create, ObjectType::ClusterReplica))
            }
            StatementKind::CreateConnection => {
                Some((StatementAction::Create, ObjectType::Connection))
            }
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
        })
        .flatten()
}
