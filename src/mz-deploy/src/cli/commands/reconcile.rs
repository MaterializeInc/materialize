// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared object metadata and orchestration for catalog reconciliation.

use mz_sql_parser::ast::{
    AlterDefaultPrivilegesStatement, ColumnName, CommentObjectType, CommentStatement,
    GrantPrivilegesStatement, GrantTargetAllSpecification, GrantTargetSpecification,
    GrantTargetSpecificationInner, Ident, ObjectType, Privilege, Raw, RawClusterName, RawItemName,
    RawNetworkPolicyName, UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName,
    UnresolvedSchemaName,
};
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};

use crate::cli::CliError;
use crate::cli::commands::comments::CommentTarget;
use crate::cli::commands::{comments, default_privileges, grants};
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, CurrentObjectState, ObjectComment, ObjectGrant};
use crate::project::ir::compiled;
use crate::project::ir::object_id::ObjectId;

/// A catalog object kind managed by mz-deploy.
///
/// This is the single source of truth for the SQL and catalog metadata that
/// grant, comment, and default-privilege reconciliation need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Table,
    Source,
    Secret,
    Connection,
    Cluster,
    Role,
    NetworkPolicy,
    Database,
    Schema,
}

impl ObjectKind {
    /// The object type stored in `mz_objects`.
    pub fn catalog_object_type(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Source => "source",
            Self::Secret => "secret",
            Self::Connection => "connection",
            _ => unreachable!("{} is not stored in mz_objects", self.label()),
        }
    }

    /// The system catalog relation that stores objects of this kind.
    pub fn catalog_table(self) -> &'static str {
        match self {
            Self::Table => "mz_tables",
            Self::Source => "mz_sources",
            Self::Secret => "mz_secrets",
            Self::Connection => "mz_connections",
            Self::Cluster => "mz_clusters",
            Self::Role => "mz_roles",
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
            Self::Role => None,
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
            Self::Role => &[],
        }
    }

    /// The object type used by `GRANT` and `REVOKE`.
    fn grant_type(self) -> Option<ObjectType> {
        match self {
            Self::Table | Self::Source => Some(ObjectType::Table),
            Self::Secret => Some(ObjectType::Secret),
            Self::Connection => Some(ObjectType::Connection),
            Self::Cluster => Some(ObjectType::Cluster),
            Self::NetworkPolicy => Some(ObjectType::NetworkPolicy),
            Self::Database => Some(ObjectType::Database),
            Self::Schema => Some(ObjectType::Schema),
            Self::Role => None,
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
            Self::Role => "role",
            Self::NetworkPolicy => "network policy",
            Self::Database => "database",
            Self::Schema => "schema",
        }
    }
}

/// A concrete catalog object being reconciled.
pub enum ReconcileTarget<'a> {
    /// A schema-qualified object in `mz_catalog.mz_objects`.
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
            ObjectKind::Cluster
                | ObjectKind::Role
                | ObjectKind::NetworkPolicy
                | ObjectKind::Database
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
        let object_type = self.kind().grant_type()?;
        let name = match self {
            Self::Item { id, .. } => UnresolvedObjectName::Item(unresolved_item_name(id)),
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
            Self::Named {
                kind: ObjectKind::Role,
                ..
            } => return None,
            Self::Named { kind, .. } => unreachable!("invalid named {} target", kind.label()),
        };
        Some(GrantTargetSpecification::Object {
            object_type,
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

    /// Build a statement that sets or clears one comment target.
    pub fn comment_statement(
        &self,
        target: &CommentTarget,
        comment: Option<String>,
    ) -> CommentStatement<Raw> {
        let object = match target {
            CommentTarget::Column(column) => {
                let Self::Item { id, .. } = self else {
                    unreachable!("a {} cannot carry a column comment", self.kind().label());
                };
                CommentObjectType::Column {
                    name: ColumnName {
                        relation: item_name(id),
                        column: Ident::new_unchecked(column),
                    },
                }
            }
            CommentTarget::Object => match self {
                Self::Item {
                    kind: ObjectKind::Table,
                    id,
                } => CommentObjectType::Table {
                    name: item_name(id),
                },
                Self::Item {
                    kind: ObjectKind::Source,
                    id,
                } => CommentObjectType::Source {
                    name: item_name(id),
                },
                Self::Item {
                    kind: ObjectKind::Secret,
                    id,
                } => CommentObjectType::Secret {
                    name: item_name(id),
                },
                Self::Item {
                    kind: ObjectKind::Connection,
                    id,
                } => CommentObjectType::Connection {
                    name: item_name(id),
                },
                Self::Named {
                    kind: ObjectKind::Cluster,
                    name,
                } => CommentObjectType::Cluster {
                    name: RawClusterName::Unresolved(Ident::new_unchecked(*name)),
                },
                Self::Named {
                    kind: ObjectKind::Role,
                    name,
                } => CommentObjectType::Role {
                    name: Ident::new_unchecked(*name),
                },
                Self::Named {
                    kind: ObjectKind::NetworkPolicy,
                    name,
                } => CommentObjectType::NetworkPolicy {
                    name: RawNetworkPolicyName::Unresolved(Ident::new_unchecked(*name)),
                },
                Self::Named {
                    kind: ObjectKind::Database,
                    name,
                } => CommentObjectType::Database {
                    name: UnresolvedDatabaseName(Ident::new_unchecked(*name)),
                },
                Self::Schema { database, schema } => CommentObjectType::Schema {
                    name: UnresolvedSchemaName(vec![
                        Ident::new_unchecked(*database),
                        Ident::new_unchecked(*schema),
                    ]),
                },
                Self::Item { kind, .. } | Self::Named { kind, .. } => {
                    unreachable!("invalid {} target identity", kind.label())
                }
            },
        };
        CommentStatement { object, comment }
    }
}

/// Catalog state read in bulk for every object in one apply phase.
pub struct ReconcileState<K: Ord> {
    objects: BTreeMap<K, CurrentObjectState>,
}

impl<K: Ord> ReconcileState<K> {
    fn from_parts(
        grants: BTreeMap<K, Vec<ObjectGrant>>,
        default_privileges: BTreeMap<K, Vec<ObjectGrant>>,
        comments: BTreeMap<K, Vec<ObjectComment>>,
    ) -> Self {
        let mut objects: BTreeMap<K, CurrentObjectState> = BTreeMap::new();
        for (key, grants) in grants {
            objects.entry(key).or_default().grants = grants;
        }
        for (key, default_privileges) in default_privileges {
            objects.entry(key).or_default().default_privileges = default_privileges;
        }
        for (key, comments) in comments {
            objects.entry(key).or_default().comments = comments;
        }
        Self { objects }
    }

    /// The catalog state for `key`, if any was recorded.
    pub fn get<Q>(&self, key: &Q) -> Option<&CurrentObjectState>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.objects.get(key)
    }
}

impl ReconcileState<ObjectId> {
    /// Read grants and comments for schema-qualified objects of one kind.
    pub async fn for_database_objects(
        client: &Client,
        kind: ObjectKind,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Self, CliError> {
        let introspection = client.introspection();
        let catalog_object_type = kind.catalog_object_type();
        let default_privilege_type = kind
            .default_privilege_type()
            .expect("database objects support default privileges");
        let (grants, default_privileges, comments) = futures::try_join!(
            introspection.get_database_object_grants(objects, catalog_object_type),
            introspection.get_default_privilege_grants_for_database_objects(
                objects,
                catalog_object_type,
                default_privilege_type,
            ),
            introspection.get_database_object_comments(objects, catalog_object_type),
        )?;
        Ok(Self::from_parts(grants, default_privileges, comments))
    }
}

impl ReconcileState<String> {
    /// Read grants and comments for globally named objects of one kind.
    pub async fn for_named_objects(
        client: &Client,
        kind: ObjectKind,
        names: &[&str],
    ) -> Result<Self, CliError> {
        let introspection = client.introspection();
        let grants = async {
            match kind {
                ObjectKind::Cluster => introspection.get_cluster_grants(names).await,
                ObjectKind::NetworkPolicy => introspection.get_network_policy_grants(names).await,
                _ => unreachable!("{} is not a grant-bearing named object", kind.label()),
            }
        };
        let object_type = kind
            .default_privilege_type()
            .expect("grant-bearing named objects support default privileges");
        let (grants, default_privileges, comments) = futures::try_join!(
            grants,
            introspection.get_default_privilege_grants_for_named_objects(
                kind.catalog_table(),
                names,
                object_type,
            ),
            introspection.get_named_object_comments(kind.catalog_table(), names),
        )?;
        Ok(Self::from_parts(grants, default_privileges, comments))
    }
}

/// Reconcile grants and comments for one target.
pub async fn grants_and_comments(
    executor: &DeploymentExecutor<'_>,
    target: &ReconcileTarget<'_>,
    desired_grants: &[GrantPrivilegesStatement<Raw>],
    desired_comments: &[CommentStatement<Raw>],
    current: &CurrentObjectState,
) -> Result<(), CliError> {
    if target.grant_target().is_some() {
        grants::reconcile(
            executor,
            target,
            desired_grants,
            &current.grants,
            &current.default_privileges,
        )
        .await?;
    }
    comments::reconcile(executor, target, desired_comments, &current.comments).await
}

/// Reconcile one compiled database object from phase-level catalog state.
pub async fn database_object(
    executor: &DeploymentExecutor<'_>,
    id: &ObjectId,
    object: &compiled::DatabaseObject,
    kind: ObjectKind,
    state: &ReconcileState<ObjectId>,
) -> Result<(), CliError> {
    let empty = CurrentObjectState::default();
    grants_and_comments(
        executor,
        &ReconcileTarget::item(kind, id),
        &object.grants,
        &object.comments,
        state.get(id).unwrap_or(&empty),
    )
    .await
}

/// Reconcile one named object from phase-level catalog state.
pub async fn named_object(
    executor: &DeploymentExecutor<'_>,
    name: &str,
    kind: ObjectKind,
    desired_grants: &[GrantPrivilegesStatement<Raw>],
    desired_comments: &[CommentStatement<Raw>],
    state: &ReconcileState<String>,
) -> Result<(), CliError> {
    let empty = CurrentObjectState::default();
    grants_and_comments(
        executor,
        &ReconcileTarget::named(kind, name),
        desired_grants,
        desired_comments,
        state.get(name).unwrap_or(&empty),
    )
    .await
}

/// Read the catalog state needed to reconcile a database or schema scope.
async fn current_scope_state(
    client: &Client,
    target: &ReconcileTarget<'_>,
) -> Result<CurrentObjectState, CliError> {
    let introspection = client.introspection();
    let (grants, default_privileges, comments) = match target {
        ReconcileTarget::Named {
            kind: ObjectKind::Database,
            name,
        } => futures::try_join!(
            introspection.get_database_grants(name),
            introspection.get_default_privilege_grants_for_database(name),
            introspection.get_one_named_object_comments("mz_databases", name),
        )?,
        ReconcileTarget::Schema { database, schema } => futures::try_join!(
            introspection.get_schema_grants(database, schema),
            introspection.get_default_privilege_grants_for_schema(database, schema),
            introspection.get_schema_comments(database, schema),
        )?,
        _ => unreachable!("scope reconciliation requires a database or schema"),
    };
    Ok(CurrentObjectState {
        grants,
        default_privileges,
        comments,
    })
}

/// Reconcile all state declared by one database or schema modifier file.
pub async fn scope(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    target: &ReconcileTarget<'_>,
    desired_grants: &[GrantPrivilegesStatement<Raw>],
    desired_comments: &[CommentStatement<Raw>],
    desired_default_privileges: &[AlterDefaultPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    let introspection = client.introspection();
    let current_default_privileges = async {
        match target {
            ReconcileTarget::Named {
                kind: ObjectKind::Database,
                name,
            } => introspection
                .get_database_default_privileges(name)
                .await
                .map_err(CliError::Connection),
            ReconcileTarget::Schema { database, schema } => introspection
                .get_schema_default_privileges(database, schema)
                .await
                .map_err(CliError::Connection),
            _ => unreachable!("scope reconciliation requires a database or schema"),
        }
    };
    let (current, current_default_privileges) = futures::try_join!(
        current_scope_state(client, target),
        current_default_privileges
    )?;
    grants_and_comments(executor, target, desired_grants, desired_comments, &current).await?;
    default_privileges::reconcile(
        executor,
        target,
        desired_default_privileges,
        &current_default_privileges,
    )
    .await
}

/// The fully-qualified name of a schema-qualified object.
fn item_name(id: &ObjectId) -> RawItemName {
    RawItemName::Name(unresolved_item_name(id))
}

fn unresolved_item_name(id: &ObjectId) -> UnresolvedItemName {
    UnresolvedItemName::qualified(&[
        Ident::new_unchecked(id.expect_database()),
        Ident::new_unchecked(id.schema()),
        Ident::new_unchecked(id.object()),
    ])
}
