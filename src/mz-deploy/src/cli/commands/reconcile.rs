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
    ColumnName, CommentObjectType, CommentStatement, GrantTargetSpecification,
    GrantTargetSpecificationInner, Ident, ObjectType, Raw, RawClusterName, RawItemName,
    RawNetworkPolicyName, UnresolvedItemName, UnresolvedObjectName,
};

use crate::cli::commands::comments::CommentTarget;

use crate::project::ir::object_id::ObjectId;

/// A catalog object kind managed by mz-deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Table,
    Source,
    Secret,
    Connection,
    Cluster,
    Role,
    NetworkPolicy,
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
            Self::Role => "mz_roles",
            Self::NetworkPolicy => "mz_network_policies",
        }
    }

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

    /// The object type stored in `mz_default_privileges`.
    pub fn default_privilege_type(self) -> &'static str {
        match self {
            Self::Table | Self::Source => "table",
            Self::Secret => "secret",
            Self::Connection => "connection",
            Self::Cluster => "cluster",
            Self::NetworkPolicy => "network policy",
            Self::Role => unreachable!("a role has no default privileges"),
        }
    }

    /// The privileges represented by `ALL` for this kind.
    pub fn all_privileges(self) -> &'static [&'static str] {
        match self {
            Self::Table => &["SELECT", "INSERT", "UPDATE", "DELETE"],
            Self::Source => &["SELECT"],
            Self::Secret | Self::Connection | Self::NetworkPolicy => &["USAGE"],
            Self::Cluster => &["USAGE", "CREATE"],
            Self::Role => &[],
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
            Self::Role => unreachable!("a role has no grant target"),
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
        }
    }
}

/// A concrete catalog object being reconciled.
pub enum ReconcileTarget<'a> {
    /// A schema-qualified database object.
    Item { kind: ObjectKind, id: &'a ObjectId },
    /// An object identified by one global name.
    Named { kind: ObjectKind, name: &'a str },
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
            ObjectKind::Cluster | ObjectKind::Role | ObjectKind::NetworkPolicy
        ));
        Self::Named { kind, name }
    }

    /// The target's object kind.
    pub fn kind(&self) -> ObjectKind {
        match self {
            Self::Item { kind, .. } | Self::Named { kind, .. } => *kind,
        }
    }

    /// The target name for user-facing status messages.
    pub fn display_name(&self) -> String {
        match self {
            Self::Item { id, .. } => id.to_string(),
            Self::Named { name, .. } => name.to_string(),
        }
    }

    /// The SQL target for a `GRANT` or `REVOKE` statement.
    pub fn grant_target(&self) -> GrantTargetSpecification<Raw> {
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
            Self::Named { kind, .. } => unreachable!("invalid named {} target", kind.label()),
        };
        GrantTargetSpecification::Object {
            object_type: self.kind().grant_type(),
            object_spec_inner: GrantTargetSpecificationInner::Objects { names: vec![name] },
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
                Self::Item { kind, .. } | Self::Named { kind, .. } => {
                    unreachable!("invalid {} target identity", kind.label())
                }
            },
        };
        CommentStatement { object, comment }
    }
}

/// The fully-qualified name of a schema-qualified object.
fn item_name(id: &ObjectId) -> RawItemName {
    RawItemName::Name(UnresolvedItemName::qualified(&[
        Ident::new_unchecked(id.expect_database()),
        Ident::new_unchecked(id.schema()),
        Ident::new_unchecked(id.object()),
    ]))
}
