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

use mz_controller::clusters::ClusterStatus;
use mz_orchestrator::{NotReadyReason, ServiceStatus};
use mz_ore::str::StrExt;
use mz_repr::strconv;
use mz_sql::ast::NoticeSeverity;
use mz_sql::session::vars::IsolationLevel;

/// Notices that can occur in the adapter layer.
///
/// These are diagnostic warnings or informational messages that are not
/// severe enough to warrant failing a query entirely.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    DroppedActiveDatabase {
        name: String,
    },
    DroppedActiveCluster {
        name: String,
    },
    QueryTimestamp {
        timestamp: mz_repr::Timestamp,
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
    DroppedSubscribe {
        dropped_name: String,
    },
    BadStartupSetting {
        name: String,
        reason: String,
    },
    RbacSystemDisabled,
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
}

impl AdapterNotice {
    /// Reports additional details about the notice, if any are available.
    pub fn detail(&self) -> Option<String> {
        None
    }

    /// Reports a hint for the user about how the notice could be addressed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AdapterNotice::DatabaseDoesNotExist { name: _ } => Some("Create the database with CREATE DATABASE or pick an extant database with SET DATABASE = name. List available databases with SHOW DATABASES.".into()),
            AdapterNotice::ClusterDoesNotExist { name: _ } => Some("Create the cluster with CREATE CLUSTER or pick an extant cluster with SET CLUSTER = name. List available clusters with SHOW CLUSTERS.".into()),
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
            AdapterNotice::RbacSystemDisabled => Some("To enable RBAC please reach out to support with a request to turn RBAC on.".into()),
            AdapterNotice::RbacUserDisabled => Some("To enable RBAC globally run `ALTER SYSTEM SET enable_rbac_checks TO TRUE` as a superuser. TO enable RBAC for just this session run `SET enable_session_rbac_checks TO TRUE`.".into()),
            AdapterNotice::AlterIndexOwner {name: _} => Some("Change the ownership of the index's relation, instead.".into()),
            _ => None
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
            AdapterNotice::ClusterDoesNotExist { name } => {
                write!(f, "cluster {} does not exist", name.quoted())
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
            AdapterNotice::QueryTimestamp { timestamp } => {
                write!(f, "query timestamp: {}", timestamp)
            }
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
            AdapterNotice::DroppedSubscribe { dropped_name } => {
                write!(
                    f,
                "subscribe has been terminated because underlying relation {dropped_name} was dropped"
                )
            }
            AdapterNotice::BadStartupSetting { name, reason } => {
                write!(f, "startup setting {name} not set: {reason}")
            }
            AdapterNotice::RbacSystemDisabled => {
                write!(
                    f,
                    "RBAC is disabled so no role attributes or object ownership will be considered \
                    when executing statements"
                )
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
        }
    }
}
