// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared object metadata for catalog reconciliation.

use mz_sql_parser::ast::{
    GrantPrivilegesStatement, GrantTargetAllSpecification, GrantTargetSpecification,
    GrantTargetSpecificationInner, Ident, ObjectType, Privilege, Raw, UnresolvedDatabaseName,
    UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName,
};

use crate::cli::CliError;
use crate::cli::commands::grants as grant_reconciliation;
use crate::cli::executor::DeploymentExecutor;
use crate::client::Client;
use crate::project::ir::object_id::ObjectId;

/// A catalog object kind managed by mz-deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Table,
    Source,
    Secret,
    Connection,
    Cluster,
    NetworkPolicy,
    Database,
    Schema,
}

impl ObjectKind {
    /// The system catalog relation that stores objects of this kind.
    pub fn catalog_table(self) -> &'static str {
        match self {
            Self::Table => "mz_tables",
            Self::Source => "mz_sources",
            Self::Secret => "mz_secrets",
            Self::Connection => "mz_connections",
            Self::Cluster => "mz_clusters",
            Self::NetworkPolicy => "mz_network_policies",
            Self::Database => "mz_databases",
            Self::Schema => "mz_schemas",
        }
    }

    /// The object type stored in `mz_default_privileges`.
    pub fn default_privilege_type(self) -> Option<&'static str> {
        match self {
            Self::Table | Self::Source => Some("table"),
            Self::Secret => Some("secret"),
            Self::Connection => Some("connection"),
            Self::Cluster => Some("cluster"),
            Self::NetworkPolicy => Some("network policy"),
            Self::Database => Some("database"),
            Self::Schema => Some("schema"),
        }
    }

    /// The privileges represented by `ALL` for this kind.
    pub fn all_privileges(self) -> &'static [Privilege] {
        match self {
            Self::Table => &[
                Privilege::SELECT,
                Privilege::INSERT,
                Privilege::UPDATE,
                Privilege::DELETE,
            ],
            Self::Source => &[Privilege::SELECT],
            Self::Secret | Self::Connection | Self::NetworkPolicy => &[Privilege::USAGE],
            Self::Cluster | Self::Database | Self::Schema => &[Privilege::USAGE, Privilege::CREATE],
        }
    }

    /// The object type used by `GRANT` and `REVOKE`.
    fn grant_type(self) -> ObjectType {
        match self {
            Self::Table | Self::Source => ObjectType::Table,
            Self::Secret => ObjectType::Secret,
            Self::Connection => ObjectType::Connection,
            Self::Cluster => ObjectType::Cluster,
            Self::NetworkPolicy => ObjectType::NetworkPolicy,
            Self::Database => ObjectType::Database,
            Self::Schema => ObjectType::Schema,
        }
    }

    /// A short object type for user-facing status messages.
    pub fn label(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Source => "source",
            Self::Secret => "secret",
            Self::Connection => "connection",
            Self::Cluster => "cluster",
            Self::NetworkPolicy => "network policy",
            Self::Database => "database",
            Self::Schema => "schema",
        }
    }
}

/// A concrete catalog object being reconciled.
pub enum ReconcileTarget<'a> {
    /// A schema-qualified database object.
    Item { kind: ObjectKind, id: &'a ObjectId },
    /// An object identified by one global name.
    Named { kind: ObjectKind, name: &'a str },
    /// A schema, which needs both its database and schema names.
    Schema { database: &'a str, schema: &'a str },
}

impl<'a> ReconcileTarget<'a> {
    /// Construct a schema-qualified target.
    pub fn item(kind: ObjectKind, id: &'a ObjectId) -> Self {
        debug_assert!(matches!(
            kind,
            ObjectKind::Table | ObjectKind::Source | ObjectKind::Secret | ObjectKind::Connection
        ));
        Self::Item { kind, id }
    }

    /// Construct a globally named target.
    pub fn named(kind: ObjectKind, name: &'a str) -> Self {
        debug_assert!(matches!(
            kind,
            ObjectKind::Cluster | ObjectKind::NetworkPolicy | ObjectKind::Database
        ));
        Self::Named { kind, name }
    }

    /// Construct a schema target.
    pub fn schema(database: &'a str, schema: &'a str) -> Self {
        Self::Schema { database, schema }
    }

    /// The target's object kind.
    pub fn kind(&self) -> ObjectKind {
        match self {
            Self::Item { kind, .. } | Self::Named { kind, .. } => *kind,
            Self::Schema { .. } => ObjectKind::Schema,
        }
    }

    /// The target name for user-facing status messages.
    pub fn display_name(&self) -> String {
        match self {
            Self::Item { id, .. } => id.to_string(),
            Self::Named { name, .. } => name.to_string(),
            Self::Schema { database, schema } => format!("{}.{}", database, schema),
        }
    }

    /// The SQL target for a `GRANT` or `REVOKE` statement.
    pub fn grant_target(&self) -> Option<GrantTargetSpecification<Raw>> {
        let name = match self {
            Self::Item { id, .. } => {
                let item_name = UnresolvedItemName::qualified(&[
                    Ident::new_unchecked(id.expect_database()),
                    Ident::new_unchecked(id.schema()),
                    Ident::new_unchecked(id.object()),
                ]);
                UnresolvedObjectName::Item(item_name)
            }
            Self::Named {
                kind: ObjectKind::Cluster,
                name,
            } => UnresolvedObjectName::Cluster(Ident::new_unchecked(*name)),
            Self::Named {
                kind: ObjectKind::NetworkPolicy,
                name,
            } => UnresolvedObjectName::NetworkPolicy(Ident::new_unchecked(*name)),
            Self::Named {
                kind: ObjectKind::Database,
                name,
            } => {
                UnresolvedObjectName::Database(UnresolvedDatabaseName(Ident::new_unchecked(*name)))
            }
            Self::Schema { database, schema } => {
                UnresolvedObjectName::Schema(UnresolvedSchemaName(vec![
                    Ident::new_unchecked(*database),
                    Ident::new_unchecked(*schema),
                ]))
            }
            Self::Named { kind, .. } => unreachable!("invalid named {} target", kind.label()),
        };
        Some(GrantTargetSpecification::Object {
            object_type: self.kind().grant_type(),
            object_spec_inner: GrantTargetSpecificationInner::Objects { names: vec![name] },
        })
    }

    /// The scope clause for synthesized default-privilege rules.
    pub fn default_privilege_scope(&self) -> Option<GrantTargetAllSpecification<Raw>> {
        match self {
            Self::Named {
                kind: ObjectKind::Database,
                name,
            } => Some(GrantTargetAllSpecification::AllDatabases {
                databases: vec![UnresolvedDatabaseName(Ident::new_unchecked(*name))],
            }),
            Self::Schema { database, schema } => Some(GrantTargetAllSpecification::AllSchemas {
                schemas: vec![UnresolvedSchemaName(vec![
                    Ident::new_unchecked(*database),
                    Ident::new_unchecked(*schema),
                ])],
            }),
            _ => None,
        }
    }
}

/// Read and reconcile grants for one object.
pub async fn grants(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    target: &ReconcileTarget<'_>,
    desired: &[GrantPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    let introspection = client.introspection();
    let kind = target.kind();
    let (current, default_privileges) = match target {
        ReconcileTarget::Item { id, .. } => (
            introspection
                .get_database_object_grants(
                    kind.catalog_table(),
                    id.expect_database(),
                    id.schema(),
                    id.object(),
                )
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_database_object(
                    kind.catalog_table(),
                    id.expect_database(),
                    id.schema(),
                    id.object(),
                    kind.default_privilege_type()
                        .expect("database objects support default privileges"),
                )
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named {
            kind: ObjectKind::Cluster,
            name,
        } => (
            introspection
                .get_cluster_grants(name)
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_cluster(name)
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named {
            kind: ObjectKind::NetworkPolicy,
            name,
        } => (
            introspection
                .get_network_policy_grants(name)
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_network_policy(name)
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named { kind, .. } => {
            unreachable!("{} scope grants are reconciled separately", kind.label())
        }
        ReconcileTarget::Schema { .. } => {
            unreachable!("schema grants are reconciled separately")
        }
    };
    grant_reconciliation::reconcile(executor, target, desired, &current, &default_privileges).await
}
