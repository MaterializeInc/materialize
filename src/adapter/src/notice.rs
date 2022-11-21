// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_compute_client::controller::ComputeInstanceStatus;
use mz_ore::str::StrExt;
use mz_sql::ast::NoticeSeverity;

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
    ExistingTransactionInProgress,
    ExplicitTransactionControlInImplicitTransaction,
    UserRequested {
        severity: NoticeSeverity,
    },
    ClusterReplicaStatusChanged {
        cluster: String,
        replica: String,
        status: ComputeInstanceStatus,
    },
    DroppedActiveDatabase {
        name: String,
    },
    DroppedActiveCluster {
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
            AdapterNotice::DroppedActiveDatabase { name: _ } => Some("Choose a new active database by executing SET DATABASE = <name>.".into()),
            AdapterNotice::DroppedActiveCluster { name: _ } => Some("Choose a new active cluster by executing SET CLUSTER = <name>.".into()),
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
            } => {
                write!(
                    f,
                    "cluster replica {}.{} changed status to: {:?}",
                    cluster, replica, status,
                )
            }
            AdapterNotice::DroppedActiveDatabase { name } => {
                write!(f, "active database {} has been dropped", name.quoted())
            }
            AdapterNotice::DroppedActiveCluster { name } => {
                write!(f, "active cluster {} has been dropped", name.quoted())
            }
        }
    }
}
