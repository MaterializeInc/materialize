// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal consistency checks that validate invariants of [`CatalogState`].

use mz_controller::clusters::{ClusterId, ReplicaId};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::DefaultPrivilegeObject;
use mz_sql::names::{CommentObjectId, DatabaseId, SchemaId};
use serde::Serialize;

use super::CatalogState;

#[derive(Debug, Default, Serialize)]
pub struct CatalogInconsistencies {
    /// Inconsistencies found with internal fields, if any.
    internal_fields: Vec<InternalFieldsInconsistency>,
    /// Inconsistencies found with roles, if any.
    roles: Vec<RoleInconsistency>,
    /// Inconsistencies found with comments, if any.
    comments: Vec<CommentInconsistency>,
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
            if self.roles_by_id.get(&entry.owner_id).is_none() {
                inconsistencies.push(RoleInconsistency::Entry(*global_id, entry.owner_id));
            }
        }
        for (cluster_id, cluster) in &self.clusters_by_id {
            if self.roles_by_id.get(&cluster.owner_id).is_none() {
                inconsistencies.push(RoleInconsistency::Cluster(*cluster_id, cluster.owner_id));
            }
            for (replica_id, replica) in &cluster.replicas_by_id {
                if self.roles_by_id.get(&replica.owner_id).is_none() {
                    inconsistencies.push(RoleInconsistency::ClusterReplica(
                        *cluster_id,
                        *replica_id,
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
                CommentObjectId::Table(global_id) | CommentObjectId::View(global_id) => {
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
            }
        }

        if comment_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(comment_inconsistencies)
        }
    }
}

#[derive(Debug, Serialize)]
enum InternalFieldsInconsistency {
    Database(String, DatabaseId),
    AmbientSchema(String, SchemaId),
    Cluster(String, ClusterId),
    ClusterLinkedObjects(GlobalId, ClusterId),
    Role(String, RoleId),
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
enum CommentInconsistency {
    /// A comment was found for an object that no longer exists.
    Dangling(CommentObjectId),
    /// A comment with a column position was found on a non-relation.
    NonRelation(CommentObjectId, usize),
}
