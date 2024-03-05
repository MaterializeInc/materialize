// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use mz_controller::clusters::ClusterStatus;
use mz_orchestrator::{NotReadyReason, ServiceStatus};
use mz_ore::str::{separated, StrExt};
use mz_pgwire_common::{ErrorResponse, Severity};
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::strconv;
use mz_sql::ast::NoticeSeverity;
use mz_sql::catalog::ErrorMessageObjectDescription;
use mz_sql::plan::PlanNotice;
use mz_sql::session::vars::IsolationLevel;
use tokio_postgres::error::SqlState;

use crate::TimestampExplanation;

/// Notices that can occur in the adapter layer.
///
/// These are diagnostic warnings or informational messages that are not
/// severe enough to warrant failing a query entirely.
#[derive(Clone, Debug)]
pub enum AdapterNotice {
    DatabaseAlreadyExists {
        name: String,
    },
    SchemaAlreadyExists {
        name: String,
    },
    TableAlreadyExists {
        name: String,
    },
    ObjectAlreadyExists {
        name: String,
        ty: &'static str,
    },
    DatabaseDoesNotExist {
        name: String,
    },
    ClusterDoesNotExist {
        name: String,
    },
    DefaultClusterDoesNotExist {
        name: String,
        kind: Option<&'static str>,
        suggested_action: String,
    },
    NoResolvableSearchPathSchema {
        search_path: Vec<String>,
    },
    ExistingTransactionInProgress,
    ExplicitTransactionControlInImplicitTransaction,
    UserRequested {
        severity: NoticeSeverity,
    },
    ClusterReplicaStatusChanged {
        cluster: String,
        replica: String,
        status: ClusterStatus,
        time: DateTime<Utc>,
    },
    CascadeDroppedObject {
        objects: Vec<String>,
    },
    DroppedActiveDatabase {
        name: String,
    },
    DroppedActiveCluster {
        name: String,
    },
    QueryTimestamp {
        explanation: TimestampExplanation<mz_repr::Timestamp>,
    },
    EqualSubscribeBounds {
        bound: mz_repr::Timestamp,
    },
    QueryTrace {
        trace_id: opentelemetry::trace::TraceId,
    },
    UnimplementedIsolationLevel {
        isolation_level: String,
    },
    StrongSessionSerializable,
    BadStartupSetting {
        name: String,
        reason: String,
    },
    RbacUserDisabled,
    RoleMembershipAlreadyExists {
        role_name: String,
        member_name: String,
    },
    RoleMembershipDoesNotExists {
        role_name: String,
        member_name: String,
    },
    AutoRunOnIntrospectionCluster,
    AlterIndexOwner {
        name: String,
    },
    CannotRevoke {
        object_description: ErrorMessageObjectDescription,
    },
    NonApplicablePrivilegeTypes {
        non_applicable_privileges: AclMode,
        object_description: ErrorMessageObjectDescription,
    },
    PlanNotice(PlanNotice),
    UnknownSessionDatabase(String),
    OptimizerNotice {
        notice: String,
        hint: String,
    },
    WebhookSourceCreated {
        url: url::Url,
    },
    DroppedInUseIndex(DroppedInUseIndex),
    PerReplicaLogRead {
        log_names: Vec<String>,
    },
    VarDefaultUpdated {
        role: Option<String>,
        var_name: Option<String>,
    },
    Welcome(String),
}

impl AdapterNotice {
    pub fn into_response(self) -> ErrorResponse {
        ErrorResponse {
            severity: self.severity(),
            code: self.code(),
            message: self.to_string(),
            detail: self.detail(),
            hint: self.hint(),
            position: None,
        }
    }

    /// Returns the severity for a notice.
    pub fn severity(&self) -> Severity {
        match self {
            AdapterNotice::DatabaseAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::SchemaAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::TableAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::ObjectAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::DatabaseDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::ClusterDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::DefaultClusterDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::NoResolvableSearchPathSchema { .. } => Severity::Notice,
            AdapterNotice::ExistingTransactionInProgress => Severity::Warning,
            AdapterNotice::ExplicitTransactionControlInImplicitTransaction => Severity::Warning,
            AdapterNotice::UserRequested { severity } => match severity {
                NoticeSeverity::Debug => Severity::Debug,
                NoticeSeverity::Info => Severity::Info,
                NoticeSeverity::Log => Severity::Log,
                NoticeSeverity::Notice => Severity::Notice,
                NoticeSeverity::Warning => Severity::Warning,
            },
            AdapterNotice::ClusterReplicaStatusChanged { .. } => Severity::Notice,
            AdapterNotice::CascadeDroppedObject { .. } => Severity::Notice,
            AdapterNotice::DroppedActiveDatabase { .. } => Severity::Notice,
            AdapterNotice::DroppedActiveCluster { .. } => Severity::Notice,
            AdapterNotice::QueryTimestamp { .. } => Severity::Notice,
            AdapterNotice::EqualSubscribeBounds { .. } => Severity::Notice,
            AdapterNotice::QueryTrace { .. } => Severity::Notice,
            AdapterNotice::UnimplementedIsolationLevel { .. } => Severity::Notice,
            AdapterNotice::StrongSessionSerializable => Severity::Notice,
            AdapterNotice::BadStartupSetting { .. } => Severity::Notice,
            AdapterNotice::RbacUserDisabled => Severity::Notice,
            AdapterNotice::RoleMembershipAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::RoleMembershipDoesNotExists { .. } => Severity::Warning,
            AdapterNotice::AutoRunOnIntrospectionCluster => Severity::Debug,
            AdapterNotice::AlterIndexOwner { .. } => Severity::Warning,
            AdapterNotice::CannotRevoke { .. } => Severity::Warning,
            AdapterNotice::NonApplicablePrivilegeTypes { .. } => Severity::Notice,
            AdapterNotice::PlanNotice(notice) => match notice {
                PlanNotice::ObjectDoesNotExist { .. } => Severity::Notice,
                PlanNotice::UpsertSinkKeyNotEnforced { .. } => Severity::Warning,
            },
            AdapterNotice::UnknownSessionDatabase(_) => Severity::Notice,
            AdapterNotice::OptimizerNotice { .. } => Severity::Notice,
            AdapterNotice::WebhookSourceCreated { .. } => Severity::Notice,
            AdapterNotice::DroppedInUseIndex { .. } => Severity::Notice,
            AdapterNotice::PerReplicaLogRead { .. } => Severity::Notice,
            AdapterNotice::VarDefaultUpdated { .. } => Severity::Notice,
            AdapterNotice::Welcome(_) => Severity::Notice,
        }
    }

    /// Reports additional details about the notice, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            AdapterNotice::PlanNotice(notice) => notice.detail(),
            AdapterNotice::QueryTimestamp { explanation } => Some(format!("\n{explanation}")),
            AdapterNotice::CascadeDroppedObject { objects } => Some(
                objects
                    .iter()
                    .map(|obj_info| format!("drop cascades to {}", obj_info))
                    .join("\n"),
            ),
            _ => None,
        }
    }

    /// Reports a hint for the user about how the notice could be addressed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AdapterNotice::DatabaseDoesNotExist { name: _ } => Some("Create the database with CREATE DATABASE or pick an extant database with SET DATABASE = name. List available databases with SHOW DATABASES.".into()),
            AdapterNotice::ClusterDoesNotExist { name: _ } => Some("Create the cluster with CREATE CLUSTER or pick an extant cluster with SET CLUSTER = name. List available clusters with SHOW CLUSTERS.".into()),
            AdapterNotice::DefaultClusterDoesNotExist { name: _, kind: _, suggested_action } => Some(suggested_action.clone()),
            AdapterNotice::NoResolvableSearchPathSchema { search_path: _ } => Some("Create a schema with CREATE SCHEMA or pick an extant schema with SET SCHEMA = name. List available schemas with SHOW SCHEMAS.".into()),
            AdapterNotice::DroppedActiveDatabase { name: _ } => Some("Choose a new active database by executing SET DATABASE = <name>.".into()),
            AdapterNotice::DroppedActiveCluster { name: _ } => Some("Choose a new active cluster by executing SET CLUSTER = <name>.".into()),
            AdapterNotice::ClusterReplicaStatusChanged { status, .. } => {
                match status {
                    ServiceStatus::NotReady(None) => Some("The cluster replica may be restarting or going offline.".into()),
                    ServiceStatus::NotReady(Some(NotReadyReason::OomKilled)) => Some("The cluster replica may have run out of memory and been killed.".into()),
                    ServiceStatus::Ready => None,
                }
            },
            AdapterNotice::RbacUserDisabled => Some("To enable RBAC globally run `ALTER SYSTEM SET enable_rbac_checks TO TRUE` as a superuser. TO enable RBAC for just this session run `SET enable_session_rbac_checks TO TRUE`.".into()),
            AdapterNotice::AlterIndexOwner {name: _} => Some("Change the ownership of the index's relation, instead.".into()),
            AdapterNotice::UnknownSessionDatabase(_) => Some(
                "Create the database with CREATE DATABASE \
                 or pick an extant database with SET DATABASE = name. \
                 List available databases with SHOW DATABASES."
                    .into(),
            ),
            AdapterNotice::OptimizerNotice { notice: _, hint } => Some(hint.clone()),
            AdapterNotice::DroppedInUseIndex(..) => Some("To free up the resources used by the index, recreate all the above-mentioned objects.".into()),
            _ => None
        }
    }

    /// Reports the error code.
    pub fn code(&self) -> SqlState {
        match self {
            AdapterNotice::DatabaseAlreadyExists { .. } => SqlState::DUPLICATE_DATABASE,
            AdapterNotice::SchemaAlreadyExists { .. } => SqlState::DUPLICATE_SCHEMA,
            AdapterNotice::TableAlreadyExists { .. } => SqlState::DUPLICATE_TABLE,
            AdapterNotice::ObjectAlreadyExists { .. } => SqlState::DUPLICATE_OBJECT,
            AdapterNotice::DatabaseDoesNotExist { .. } => SqlState::WARNING,
            AdapterNotice::ClusterDoesNotExist { .. } => SqlState::WARNING,
            AdapterNotice::DefaultClusterDoesNotExist { .. } => SqlState::WARNING,
            AdapterNotice::NoResolvableSearchPathSchema { .. } => SqlState::WARNING,
            AdapterNotice::ExistingTransactionInProgress => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterNotice::ExplicitTransactionControlInImplicitTransaction => {
                SqlState::NO_ACTIVE_SQL_TRANSACTION
            }
            AdapterNotice::UserRequested { .. } => SqlState::WARNING,
            AdapterNotice::ClusterReplicaStatusChanged { .. } => SqlState::WARNING,
            AdapterNotice::CascadeDroppedObject { .. } => SqlState::SUCCESSFUL_COMPLETION,
            AdapterNotice::DroppedActiveDatabase { .. } => SqlState::WARNING,
            AdapterNotice::DroppedActiveCluster { .. } => SqlState::WARNING,
            AdapterNotice::QueryTimestamp { .. } => SqlState::WARNING,
            AdapterNotice::EqualSubscribeBounds { .. } => SqlState::WARNING,
            AdapterNotice::QueryTrace { .. } => SqlState::WARNING,
            AdapterNotice::UnimplementedIsolationLevel { .. } => SqlState::WARNING,
            AdapterNotice::StrongSessionSerializable => SqlState::WARNING,
            AdapterNotice::BadStartupSetting { .. } => SqlState::WARNING,
            AdapterNotice::RbacUserDisabled => SqlState::WARNING,
            AdapterNotice::RoleMembershipAlreadyExists { .. } => SqlState::WARNING,
            AdapterNotice::RoleMembershipDoesNotExists { .. } => SqlState::WARNING,
            AdapterNotice::AutoRunOnIntrospectionCluster => SqlState::WARNING,
            AdapterNotice::AlterIndexOwner { .. } => SqlState::WARNING,
            AdapterNotice::CannotRevoke { .. } => SqlState::WARNING,
            AdapterNotice::NonApplicablePrivilegeTypes { .. } => SqlState::WARNING,
            AdapterNotice::PlanNotice(plan) => match plan {
                PlanNotice::ObjectDoesNotExist { .. } => SqlState::UNDEFINED_OBJECT,
                PlanNotice::UpsertSinkKeyNotEnforced { .. } => SqlState::WARNING,
            },
            AdapterNotice::UnknownSessionDatabase(_) => SqlState::SUCCESSFUL_COMPLETION,
            AdapterNotice::OptimizerNotice { .. } => SqlState::SUCCESSFUL_COMPLETION,
            AdapterNotice::DroppedInUseIndex { .. } => SqlState::WARNING,
            AdapterNotice::WebhookSourceCreated { .. } => SqlState::WARNING,
            AdapterNotice::PerReplicaLogRead { .. } => SqlState::WARNING,
            AdapterNotice::VarDefaultUpdated { .. } => SqlState::SUCCESSFUL_COMPLETION,
            AdapterNotice::Welcome(_) => SqlState::SUCCESSFUL_COMPLETION,
        }
    }
}

impl fmt::Display for AdapterNotice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AdapterNotice::DatabaseAlreadyExists { name } => {
                write!(f, "database {} already exists, skipping", name.quoted())
            }
            AdapterNotice::SchemaAlreadyExists { name } => {
                write!(f, "schema {} already exists, skipping", name.quoted())
            }
            AdapterNotice::TableAlreadyExists { name } => {
                write!(f, "table {} already exists, skipping", name.quoted())
            }
            AdapterNotice::ObjectAlreadyExists { name, ty } => {
                write!(f, "{} {} already exists, skipping", ty, name.quoted())
            }
            AdapterNotice::DatabaseDoesNotExist { name } => {
                write!(f, "database {} does not exist", name.quoted())
            }
            AdapterNotice::CascadeDroppedObject { objects } => {
                write!(f, "drop cascades to {} other objects", objects.len())
            }
            AdapterNotice::ClusterDoesNotExist { name } => {
                write!(f, "cluster {} does not exist", name.quoted())
            }
            AdapterNotice::DefaultClusterDoesNotExist { kind, name, .. } => {
                let kind = kind.map(|k| format!("{k} ")).unwrap_or(String::new());
                write!(f, "{kind}default cluster {} does not exist", name.quoted())
            }
            AdapterNotice::NoResolvableSearchPathSchema { search_path } => {
                write!(
                    f,
                    "no schema on the search path exists: {}",
                    search_path.join(", ")
                )
            }
            AdapterNotice::ExistingTransactionInProgress => {
                write!(f, "there is already a transaction in progress")
            }
            AdapterNotice::ExplicitTransactionControlInImplicitTransaction => {
                write!(f, "there is no transaction in progress")
            }
            AdapterNotice::UserRequested { severity } => {
                write!(f, "raised a test {}", severity.to_string().to_lowercase())
            }
            AdapterNotice::ClusterReplicaStatusChanged {
                cluster,
                replica,
                status,
                time,
            } => {
                let mut time_buf = String::new();
                strconv::format_timestamptz(&mut time_buf, time);
                write!(
                    f,
                    "cluster replica {}.{} changed status to {} at {}",
                    cluster,
                    replica,
                    status.as_kebab_case_str().quoted(),
                    time_buf,
                )?;
                Ok(())
            }
            AdapterNotice::DroppedActiveDatabase { name } => {
                write!(f, "active database {} has been dropped", name.quoted())
            }
            AdapterNotice::DroppedActiveCluster { name } => {
                write!(f, "active cluster {} has been dropped", name.quoted())
            }
            AdapterNotice::QueryTimestamp { .. } => write!(f, "EXPLAIN TIMESTAMP for query"),
            AdapterNotice::EqualSubscribeBounds { bound } => {
                write!(f, "subscribe as of {bound} (inclusive) up to the same bound {bound} (exclusive) is guaranteed to be empty")
            }
            AdapterNotice::QueryTrace { trace_id } => {
                write!(f, "trace id: {}", trace_id)
            }
            AdapterNotice::UnimplementedIsolationLevel { isolation_level } => {
                write!(
                    f,
                    "transaction isolation level {isolation_level} is unimplemented, the session will be upgraded to {}",
                    IsolationLevel::Serializable.as_str()
                )
            }
            AdapterNotice::StrongSessionSerializable => {
                write!(
                    f,
                    "The Strong Session Serializable isolation level may exhibit consistency violations when reading from catalog objects",
                )
            }
            AdapterNotice::BadStartupSetting { name, reason } => {
                write!(f, "startup setting {name} not set: {reason}")
            }
            AdapterNotice::RbacUserDisabled => {
                write!(
                    f,
                    "RBAC is disabled so no role attributes or object ownership will be considered \
                    when executing statements"
                )
            }
            AdapterNotice::RoleMembershipAlreadyExists {
                role_name,
                member_name,
            } => write!(
                f,
                "role \"{member_name}\" is already a member of role \"{role_name}\""
            ),
            AdapterNotice::RoleMembershipDoesNotExists {
                role_name,
                member_name,
            } => write!(
                f,
                "role \"{member_name}\" is not a member of role \"{role_name}\""
            ),
            AdapterNotice::AutoRunOnIntrospectionCluster => write!(
                f,
                "query was automatically run on the \"mz_introspection\" cluster"
            ),
            AdapterNotice::AlterIndexOwner { name } => {
                write!(f, "cannot change owner of {}", name.quoted())
            }
            AdapterNotice::CannotRevoke { object_description } => {
                write!(f, "no privileges could be revoked for {object_description}")
            }
            AdapterNotice::NonApplicablePrivilegeTypes {
                non_applicable_privileges,
                object_description,
            } => {
                write!(
                    f,
                    "non-applicable privilege types {} for {}",
                    non_applicable_privileges.to_error_string(),
                    object_description,
                )
            }
            AdapterNotice::PlanNotice(plan) => plan.fmt(f),
            AdapterNotice::UnknownSessionDatabase(name) => {
                write!(f, "session database {} does not exist", name.quoted())
            }
            AdapterNotice::OptimizerNotice { notice, hint: _ } => notice.fmt(f),
            AdapterNotice::WebhookSourceCreated { url } => {
                write!(f, "URL to POST data is '{url}'")
            }
            AdapterNotice::DroppedInUseIndex(DroppedInUseIndex {
                index_name,
                dependant_objects,
            }) => {
                write!(f, "The dropped index {index_name} is being used by the following objects: {}. The index is now dropped from the catalog, but it will continue to be maintained and take up resources until all dependent objects are dropped, altered, or Materialize is restarted!", separated(", ", dependant_objects))
            }
            AdapterNotice::PerReplicaLogRead { log_names } => {
                write!(f, "Queried introspection relations: {}. Unlike other objects in Materialize, results from querying these objects depend on the current values of the `cluster` and `cluster_replica` session variables.", log_names.join(", "))
            }
            AdapterNotice::VarDefaultUpdated { role, var_name } => {
                let vars = match var_name {
                    Some(name) => format!("variable {} was", name.quoted()),
                    None => "variables were".to_string(),
                };
                let target = match role {
                    Some(role_name) => role_name.quoted().to_string(),
                    None => "the system".to_string(),
                };
                write!(
                    f,
                    "{vars} updated for {target}, this will have no effect on the current session"
                )
            }
            AdapterNotice::Welcome(message) => message.fmt(f),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DroppedInUseIndex {
    pub index_name: String,
    pub dependant_objects: Vec<String>,
}

impl From<PlanNotice> for AdapterNotice {
    fn from(notice: PlanNotice) -> AdapterNotice {
        AdapterNotice::PlanNotice(notice)
    }
}
