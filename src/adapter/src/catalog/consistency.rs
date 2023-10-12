// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal consistency checks that validate invariants of [`CatalogState`].
//!
//! Note: the implementation of consistency checks should favor simplicity over performance, to
//! make it as easy as possible to understand what a given check is doing.

use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::DefaultPrivilegeObject;
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use serde::Serialize;

use super::CatalogState;

#[derive(Debug, Default, Clone, Serialize, PartialEq)]
pub struct CatalogInconsistencies {
    /// Inconsistencies found with internal fields, if any.
    internal_fields: Vec<InternalFieldsInconsistency>,
    /// Inconsistencies found with roles, if any.
    roles: Vec<RoleInconsistency>,
    /// Inconsistencies found with comments, if any.
    comments: Vec<CommentInconsistency>,
    /// Inconsistencies found with object dependencies, if any.
    object_dependencies: Vec<ObjectDependencyInconsistency>,
}

impl CatalogInconsistencies {
    pub fn is_empty(&self) -> bool {
        self.internal_fields.is_empty() && self.roles.is_empty() && self.comments.is_empty()
    }
}

impl CatalogState {
    /// Checks the [`CatalogState`] to make sure we're internally consistent.
    pub fn check_consistency(&self) -> Result<(), CatalogInconsistencies> {
        let mut inconsistencies = CatalogInconsistencies::default();

        if let Err(internal_fields) = self.check_internal_fields() {
            inconsistencies.internal_fields = internal_fields;
        }
        if let Err(roles) = self.check_roles() {
            inconsistencies.roles = roles;
        }
        if let Err(comments) = self.check_comments() {
            inconsistencies.comments = comments;
        }
        if let Err(dependencies) = self.check_object_dependencies() {
            inconsistencies.object_dependencies = dependencies;
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * Any fields within [`CatalogState`] that reference another field need to be kept in sync.
    ///
    /// TODO(parkmycar): Check the reverse direction for these collections, e.g. all of the
    /// `DatabaseId`s in `database_by_id` also exist in `database_by_name`.
    fn check_internal_fields(&self) -> Result<(), Vec<InternalFieldsInconsistency>> {
        let mut inconsistencies = Vec::new();
        for (name, id) in &self.database_by_name {
            if self.database_by_id.get(id).is_none() {
                inconsistencies.push(InternalFieldsInconsistency::Database(name.clone(), *id));
            }
        }
        for (name, id) in &self.ambient_schemas_by_name {
            if self.ambient_schemas_by_id.get(id).is_none() {
                inconsistencies.push(InternalFieldsInconsistency::AmbientSchema(
                    name.clone(),
                    *id,
                ));
            }
        }
        for (name, id) in &self.clusters_by_name {
            if self.clusters_by_id.get(id).is_none() {
                inconsistencies.push(InternalFieldsInconsistency::Cluster(name.clone(), *id));
            }
        }
        for (global_id, cluster_id) in &self.clusters_by_linked_object_id {
            if self.clusters_by_id.get(cluster_id).is_none() {
                inconsistencies.push(InternalFieldsInconsistency::ClusterLinkedObjects(
                    *global_id,
                    *cluster_id,
                ));
            }
        }
        for (name, role_id) in &self.roles_by_name {
            if self.roles_by_id.get(role_id).is_none() {
                inconsistencies.push(InternalFieldsInconsistency::Role(name.clone(), *role_id))
            }
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * All RoleIds referenced from other objects must exist.
    ///
    fn check_roles(&self) -> Result<(), Vec<RoleInconsistency>> {
        let mut inconsistencies = Vec::new();
        for (database_id, database) in &self.database_by_id {
            if self.roles_by_id.get(&database.owner_id).is_none() {
                inconsistencies.push(RoleInconsistency::Database(*database_id, database.owner_id));
            }
            for (schema_id, schema) in &database.schemas_by_id {
                if self.roles_by_id.get(&schema.owner_id).is_none() {
                    inconsistencies.push(RoleInconsistency::Schema(*schema_id, schema.owner_id));
                }
            }
        }
        for (global_id, entry) in &self.entry_by_id {
            if self.roles_by_id.get(entry.owner_id()).is_none() {
                inconsistencies.push(RoleInconsistency::Entry(
                    *global_id,
                    entry.owner_id().clone(),
                ));
            }
        }
        for (cluster_id, cluster) in &self.clusters_by_id {
            if self.roles_by_id.get(&cluster.owner_id).is_none() {
                inconsistencies.push(RoleInconsistency::Cluster(*cluster_id, cluster.owner_id));
            }
            for replica in cluster.replicas() {
                if self.roles_by_id.get(&replica.owner_id).is_none() {
                    inconsistencies.push(RoleInconsistency::ClusterReplica(
                        *cluster_id,
                        replica.replica_id,
                        cluster.owner_id,
                    ));
                }
            }
        }
        for (default_priv, privileges) in self.default_privileges.iter() {
            if self.roles_by_id.get(&default_priv.role_id).is_none() {
                inconsistencies.push(RoleInconsistency::DefaultPrivilege(default_priv.clone()));
            }
            for acl_item in privileges {
                if self.roles_by_id.get(&acl_item.grantee).is_none() {
                    inconsistencies.push(RoleInconsistency::DefaultPrivilegeItem {
                        grantor: default_priv.role_id,
                        grantee: acl_item.grantee,
                    });
                }
            }
        }
        for acl in self.system_privileges.all_values() {
            let grantor = self.roles_by_id.get(&acl.grantor);
            let grantee = self.roles_by_id.get(&acl.grantee);

            let inconsistency = match (grantor, grantee) {
                (None, None) => RoleInconsistency::SystemPrivilege {
                    grantor: Some(acl.grantor),
                    grantee: Some(acl.grantee),
                },
                (Some(_), None) => RoleInconsistency::SystemPrivilege {
                    grantor: None,
                    grantee: Some(acl.grantee),
                },
                (None, Some(_)) => RoleInconsistency::SystemPrivilege {
                    grantor: Some(acl.grantor),
                    grantee: None,
                },
                (Some(_), Some(_)) => continue,
            };
            inconsistencies.push(inconsistency);
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * Comments should only reference existing objects.
    /// * A comment should only have a column position if it references a relation.
    ///
    fn check_comments(&self) -> Result<(), Vec<CommentInconsistency>> {
        let mut comment_inconsistencies = Vec::new();
        for (comment_object_id, col_pos, _comment) in self.comments.iter() {
            match comment_object_id {
                CommentObjectId::Table(global_id)
                | CommentObjectId::View(global_id)
                | CommentObjectId::MaterializedView(global_id)
                | CommentObjectId::Source(global_id)
                | CommentObjectId::Sink(global_id)
                | CommentObjectId::Index(global_id)
                | CommentObjectId::Func(global_id)
                | CommentObjectId::Connection(global_id)
                | CommentObjectId::Type(global_id)
                | CommentObjectId::Secret(global_id) => {
                    let entry = self.entry_by_id.get(&global_id);
                    match entry {
                        None => comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id)),
                        Some(entry) => {
                            // TODO: Refactor this to use if-let chains, once they're stable.
                            #[allow(clippy::unnecessary_unwrap)]
                            if !entry.is_relation() && col_pos.is_some() {
                                let col_pos = col_pos.expect("checked above");
                                comment_inconsistencies.push(CommentInconsistency::NonRelation(
                                    comment_object_id,
                                    col_pos,
                                ));
                            }
                        }
                    }
                }
                CommentObjectId::Role(role_id) => {
                    if self.roles_by_id.get(&role_id).is_none() {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }
                CommentObjectId::Database(database_id) => {
                    if self.database_by_id.get(&database_id).is_none() {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }
                CommentObjectId::Schema((database, schema)) => {
                    match (database, schema) {
                        (
                            ResolvedDatabaseSpecifier::Id(database_id),
                            SchemaSpecifier::Id(schema_id),
                        ) => {
                            let schema = self
                                .database_by_id
                                .get(&database_id)
                                .and_then(|database| database.schemas_by_id.get(&schema_id));
                            if schema.is_none() {
                                comment_inconsistencies
                                    .push(CommentInconsistency::Dangling(comment_object_id));
                            }
                        }
                        (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(schema_id)) => {
                            if self.ambient_schemas_by_id.get(&schema_id).is_none() {
                                comment_inconsistencies
                                    .push(CommentInconsistency::Dangling(comment_object_id));
                            }
                        }
                        // Temporary schemas are in the ambient database.
                        (ResolvedDatabaseSpecifier::Id(_id), SchemaSpecifier::Temporary) => (),
                        // TODO: figure out how to check for consistency in this case.
                        (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => (),
                    }
                }
                CommentObjectId::Cluster(cluster_id) => {
                    if self.clusters_by_id.get(&cluster_id).is_none() {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }
                CommentObjectId::ClusterReplica((cluster_id, replica_id)) => {
                    let replica = self
                        .clusters_by_id
                        .get(&cluster_id)
                        .and_then(|cluster| cluster.replica(replica_id));
                    if replica.is_none() {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }
            }
        }

        if comment_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(comment_inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * All of the objects in the "uses" collection of a CatalogEntry, should contain said
    ///   CatalogEntry in their own "used_by" collection.
    /// * All of the objects in the "used_by" collection of a CatalogEntry, should contain said
    ///   CatalogEntry in their own "uses" collection.
    ///
    fn check_object_dependencies(&self) -> Result<(), Vec<ObjectDependencyInconsistency>> {
        let mut dependency_inconsistencies = vec![];

        for (id, entry) in &self.entry_by_id {
            for used_id in &entry.uses().0 {
                let Some(used_entry) = self.entry_by_id.get(used_id) else {
                    dependency_inconsistencies.push(ObjectDependencyInconsistency::MissingUses {
                        object_a: *id,
                        object_b: *used_id,
                    });
                    continue;
                };
                if !used_entry.used_by().contains(id) {
                    dependency_inconsistencies.push(
                        ObjectDependencyInconsistency::InconsistentUsedBy {
                            object_a: *id,
                            object_b: *used_id,
                        },
                    );
                }
            }

            for used_by in entry.used_by() {
                let Some(used_by_entry) = self.entry_by_id.get(used_by) else {
                    dependency_inconsistencies.push(ObjectDependencyInconsistency::MissingUsedBy {
                        object_a: *id,
                        object_b: *used_by,
                    });
                    continue;
                };
                if !used_by_entry.uses().0.contains(id) {
                    dependency_inconsistencies.push(
                        ObjectDependencyInconsistency::InconsistentUses {
                            object_a: *id,
                            object_b: *used_by,
                        },
                    );
                }
            }
        }

        if dependency_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(dependency_inconsistencies)
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum InternalFieldsInconsistency {
    Database(String, DatabaseId),
    AmbientSchema(String, SchemaId),
    Cluster(String, ClusterId),
    ClusterLinkedObjects(GlobalId, ClusterId),
    Role(String, RoleId),
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum RoleInconsistency {
    Database(DatabaseId, RoleId),
    Schema(SchemaId, RoleId),
    Entry(GlobalId, RoleId),
    Cluster(ClusterId, RoleId),
    ClusterReplica(ClusterId, ReplicaId, RoleId),
    DefaultPrivilege(DefaultPrivilegeObject),
    DefaultPrivilegeItem {
        grantor: RoleId,
        grantee: RoleId,
    },
    SystemPrivilege {
        grantor: Option<RoleId>,
        grantee: Option<RoleId>,
    },
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum CommentInconsistency {
    /// A comment was found for an object that no longer exists.
    Dangling(CommentObjectId),
    /// A comment with a column position was found on a non-relation.
    NonRelation(CommentObjectId, usize),
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum ObjectDependencyInconsistency {
    /// Object A uses Object B, but Object B does not exist.
    MissingUses {
        object_a: GlobalId,
        object_b: GlobalId,
    },
    /// Object A is used by Object B, but Object B does not exist.
    MissingUsedBy {
        object_a: GlobalId,
        object_b: GlobalId,
    },
    /// Object A uses Object B, but Object B does not record that it is used by Object A.
    InconsistentUsedBy {
        object_a: GlobalId,
        object_b: GlobalId,
    },
    /// Object B is used by Object A, but Object B does not record that is uses Object A.
    InconsistentUses {
        object_a: GlobalId,
        object_b: GlobalId,
    },
}
