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
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::catalog::{CatalogItem, DefaultPrivilegeObject};
use mz_sql::names::{
    CommentObjectId, DatabaseId, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql_parser::ast::{self, Statement};
use mz_sql_parser::parser::ParserStatementError;
use serde::Serialize;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
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
    /// Inconsistencies found with items in the catalog, if any.
    items: Vec<ItemInconsistency>,
}

impl CatalogInconsistencies {
    pub fn is_empty(&self) -> bool {
        let CatalogInconsistencies {
            internal_fields,
            roles,
            comments,
            object_dependencies,
            items,
        } = self;
        internal_fields.is_empty()
            && roles.is_empty()
            && comments.is_empty()
            && object_dependencies.is_empty()
            && items.is_empty()
    }
}

impl CatalogState {
    /// Checks the [`CatalogState`] to make sure we're internally consistent.
    pub fn check_consistency(&self) -> Result<(), Box<CatalogInconsistencies>> {
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
        if let Err(items) = self.check_items() {
            inconsistencies.items = items;
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(Box::new(inconsistencies))
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
            if !self.database_by_id.contains_key(id) {
                inconsistencies.push(InternalFieldsInconsistency::Database(name.clone(), *id));
            }
        }
        for (name, id) in &self.ambient_schemas_by_name {
            if !self.ambient_schemas_by_id.contains_key(id) {
                inconsistencies.push(InternalFieldsInconsistency::AmbientSchema(
                    name.clone(),
                    *id,
                ));
            }
        }
        for (name, id) in &self.clusters_by_name {
            if !self.clusters_by_id.contains_key(id) {
                inconsistencies.push(InternalFieldsInconsistency::Cluster(name.clone(), *id));
            }
        }
        for (name, role_id) in &self.roles_by_name {
            if !self.roles_by_id.contains_key(role_id) {
                inconsistencies.push(InternalFieldsInconsistency::Role(name.clone(), *role_id))
            }
        }

        for (source_id, _references) in &self.source_references {
            if !self.entry_by_id.contains_key(source_id) {
                inconsistencies.push(InternalFieldsInconsistency::SourceReferences(*source_id));
            }
        }

        for (item_id, entry) in &self.entry_by_id {
            let missing_gids: Vec<_> = entry
                .global_ids()
                .filter(|gid| !self.entry_by_global_id.contains_key(gid))
                .collect();
            if !missing_gids.is_empty() {
                inconsistencies.push(InternalFieldsInconsistency::EntryMissingGlobalIds(
                    *item_id,
                    missing_gids,
                ));
            }
        }
        for (gid, item_id) in &self.entry_by_global_id {
            if !self.entry_by_id.contains_key(item_id) {
                inconsistencies.push(InternalFieldsInconsistency::GlobalIdsMissingEntry(
                    *gid, *item_id,
                ));
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
            if !self.roles_by_id.contains_key(&database.owner_id) {
                inconsistencies.push(RoleInconsistency::Database(*database_id, database.owner_id));
            }
            for (schema_id, schema) in &database.schemas_by_id {
                if !self.roles_by_id.contains_key(&schema.owner_id) {
                    inconsistencies.push(RoleInconsistency::Schema(*schema_id, schema.owner_id));
                }
            }
        }
        for (item_id, entry) in &self.entry_by_id {
            if !self.roles_by_id.contains_key(entry.owner_id()) {
                inconsistencies.push(RoleInconsistency::Entry(*item_id, entry.owner_id().clone()));
            }
        }
        for (cluster_id, cluster) in &self.clusters_by_id {
            if !self.roles_by_id.contains_key(&cluster.owner_id) {
                inconsistencies.push(RoleInconsistency::Cluster(*cluster_id, cluster.owner_id));
            }
            for replica in cluster.replicas() {
                if !self.roles_by_id.contains_key(&replica.owner_id) {
                    inconsistencies.push(RoleInconsistency::ClusterReplica(
                        *cluster_id,
                        replica.replica_id,
                        cluster.owner_id,
                    ));
                }
            }
        }
        for (default_priv, privileges) in self.default_privileges.iter() {
            if !self.roles_by_id.contains_key(&default_priv.role_id) {
                inconsistencies.push(RoleInconsistency::DefaultPrivilege(default_priv.clone()));
            }
            for acl_item in privileges {
                if !self.roles_by_id.contains_key(&acl_item.grantee) {
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
        for role in self.roles_by_id.values() {
            for (parent_id, grantor_id) in &role.membership.map {
                let parent = self.roles_by_id.get(parent_id);
                let grantor = self.roles_by_id.get(grantor_id);
                let inconsistency = match (parent, grantor) {
                    (None, None) => RoleInconsistency::Membership {
                        parent: Some(*parent_id),
                        grantor: Some(*grantor_id),
                    },
                    (Some(_), None) => RoleInconsistency::Membership {
                        parent: None,
                        grantor: Some(*grantor_id),
                    },
                    (None, Some(_)) => RoleInconsistency::Membership {
                        parent: Some(*parent_id),
                        grantor: None,
                    },
                    (Some(_), Some(_)) => continue,
                };
                inconsistencies.push(inconsistency);
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
    /// * Comments should only reference existing objects.
    /// * A comment should only have a column position if it references a relation.
    ///
    fn check_comments(&self) -> Result<(), Vec<CommentInconsistency>> {
        let mut comment_inconsistencies = Vec::new();
        for (comment_object_id, col_pos, _comment) in self.comments.iter() {
            match comment_object_id {
                CommentObjectId::Table(item_id)
                | CommentObjectId::View(item_id)
                | CommentObjectId::MaterializedView(item_id)
                | CommentObjectId::Source(item_id)
                | CommentObjectId::Sink(item_id)
                | CommentObjectId::Index(item_id)
                | CommentObjectId::Func(item_id)
                | CommentObjectId::Connection(item_id)
                | CommentObjectId::Type(item_id)
                | CommentObjectId::Secret(item_id)
                | CommentObjectId::ContinualTask(item_id) => {
                    let entry = self.entry_by_id.get(&item_id);
                    match entry {
                        None => comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id)),
                        Some(entry) => {
                            // TODO: Refactor this to use if-let chains, once they're stable.
                            #[allow(clippy::unnecessary_unwrap)]
                            if !entry.has_columns() && col_pos.is_some() {
                                let col_pos = col_pos.expect("checked above");
                                comment_inconsistencies.push(CommentInconsistency::NonRelation(
                                    comment_object_id,
                                    col_pos,
                                ));
                            }
                        }
                    }
                }
                CommentObjectId::NetworkPolicy(network_policy_id) => {
                    if !self.network_policies_by_id.contains_key(&network_policy_id) {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }

                CommentObjectId::Role(role_id) => {
                    if !self.roles_by_id.contains_key(&role_id) {
                        comment_inconsistencies
                            .push(CommentInconsistency::Dangling(comment_object_id));
                    }
                }
                CommentObjectId::Database(database_id) => {
                    if !self.database_by_id.contains_key(&database_id) {
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
                            if !self.ambient_schemas_by_id.contains_key(&schema_id) {
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
                    if !self.clusters_by_id.contains_key(&cluster_id) {
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
            for referenced_id in entry.references().items() {
                let Some(referenced_entry) = self.entry_by_id.get(referenced_id) else {
                    dependency_inconsistencies.push(ObjectDependencyInconsistency::MissingUses {
                        object_a: *id,
                        object_b: *referenced_id,
                    });
                    continue;
                };
                if !referenced_entry.referenced_by().contains(id)
                    // Continual Tasks are self referential.
                    && (referenced_entry.id() != *id && !referenced_entry.is_continual_task())
                {
                    dependency_inconsistencies.push(
                        ObjectDependencyInconsistency::InconsistentUsedBy {
                            object_a: *id,
                            object_b: *referenced_id,
                        },
                    );
                }
            }
            for used_id in entry.uses() {
                let Some(used_entry) = self.entry_by_id.get(&used_id) else {
                    dependency_inconsistencies.push(ObjectDependencyInconsistency::MissingUses {
                        object_a: *id,
                        object_b: used_id,
                    });
                    continue;
                };
                if !used_entry.used_by().contains(id)
                    // Continual Tasks are self referential.
                    && (used_entry.id() != *id && !used_entry.is_continual_task())
                {
                    dependency_inconsistencies.push(
                        ObjectDependencyInconsistency::InconsistentUsedBy {
                            object_a: *id,
                            object_b: used_id,
                        },
                    );
                }
            }

            for referenced_by in entry.referenced_by() {
                let Some(referenced_by_entry) = self.entry_by_id.get(referenced_by) else {
                    dependency_inconsistencies.push(ObjectDependencyInconsistency::MissingUsedBy {
                        object_a: *id,
                        object_b: *referenced_by,
                    });
                    continue;
                };
                if !referenced_by_entry.references().contains_item(id) {
                    dependency_inconsistencies.push(
                        ObjectDependencyInconsistency::InconsistentUses {
                            object_a: *id,
                            object_b: *referenced_by,
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
                if !used_by_entry.uses().contains(id) {
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

    /// # Invariants
    ///
    /// * Every schema that exists in the `schemas_by_name` map, also exists in `schemas_by_id`.
    /// * The name present in the `schemas_by_name` map matches the name in the associated `Schema`
    ///   struct.
    /// * All items that exist in a `Schema` struct, also exist in the `entries_by_id` map.
    /// * Parsing the `create_sql` string from an `Entry` succeeds.
    /// * The result of parsing the `create_sql` must return a single `Statement`.
    /// * The names in the returned `Statement`, must match that of the parent struct.
    /// * The item from the parsed `create_sql` must be fully qualified.
    ///
    fn check_items(&self) -> Result<(), Vec<ItemInconsistency>> {
        let mut item_inconsistencies = vec![];

        for (db_id, db) in &self.database_by_id {
            for (schema_name, schema_id) in &db.schemas_by_name {
                // Make sure the schema themselves are consistent.
                let Some(schema) = db.schemas_by_id.get(schema_id) else {
                    item_inconsistencies.push(ItemInconsistency::MissingSchema {
                        db_id: *db_id,
                        schema_name: schema_name.clone(),
                    });
                    continue;
                };
                if schema_name != &schema.name.schema {
                    item_inconsistencies.push(ItemInconsistency::KeyedName {
                        db_schema_by_name: schema_name.clone(),
                        struct_name: schema.name.schema.clone(),
                    });
                }

                // Make sure the items in the schema are consistent.
                for (item_name, item_id) in &schema.items {
                    let Some(entry) = self.entry_by_id.get(item_id) else {
                        item_inconsistencies.push(ItemInconsistency::NonExistentItem {
                            db_id: *db_id,
                            schema_id: schema.id,
                            item_id: *item_id,
                        });
                        continue;
                    };
                    if item_name != &entry.name().item {
                        item_inconsistencies.push(ItemInconsistency::ItemNameMismatch {
                            item_id: *item_id,
                            map_name: item_name.clone(),
                            entry_name: entry.name().clone(),
                        });
                    }
                    let statement = match mz_sql::parse::parse(entry.create_sql()) {
                        Ok(mut statements) if statements.len() == 1 => {
                            let statement = statements.pop().expect("checked length");
                            statement.ast
                        }
                        Ok(_) => {
                            item_inconsistencies.push(ItemInconsistency::MultiCreateStatement {
                                create_sql: entry.create_sql().to_string(),
                            });
                            continue;
                        }
                        Err(e) => {
                            item_inconsistencies.push(ItemInconsistency::StatementParseFailure {
                                create_sql: entry.create_sql().to_string(),
                                e,
                            });
                            continue;
                        }
                    };
                    match statement {
                        Statement::CreateConnection(ast::CreateConnectionStatement {
                            name,
                            ..
                        })
                        | Statement::CreateWebhookSource(ast::CreateWebhookSourceStatement {
                            name,
                            ..
                        })
                        | Statement::CreateSource(ast::CreateSourceStatement { name, .. })
                        | Statement::CreateSubsource(ast::CreateSubsourceStatement {
                            name, ..
                        })
                        | Statement::CreateSink(ast::CreateSinkStatement {
                            name: Some(name),
                            ..
                        })
                        | Statement::CreateView(ast::CreateViewStatement {
                            definition: ast::ViewDefinition { name, .. },
                            ..
                        })
                        | Statement::CreateMaterializedView(
                            ast::CreateMaterializedViewStatement { name, .. },
                        )
                        | Statement::CreateTable(ast::CreateTableStatement { name, .. })
                        | Statement::CreateType(ast::CreateTypeStatement { name, .. })
                        | Statement::CreateSecret(ast::CreateSecretStatement { name, .. }) => {
                            let [db_component, schema_component, item_component] = &name.0[..]
                            else {
                                let name =
                                    name.0.into_iter().map(|ident| ident.to_string()).collect();
                                item_inconsistencies.push(
                                    ItemInconsistency::NonFullyQualifiedItemName {
                                        create_sql: entry.create_sql().to_string(),
                                        name,
                                    },
                                );
                                continue;
                            };
                            if db_component.as_str() != &db.name
                                || schema_component.as_str() != &schema.name.schema
                                || item_component.as_str() != &entry.name().item
                            {
                                item_inconsistencies.push(
                                    ItemInconsistency::CreateSqlItemNameMismatch {
                                        item_name: vec![
                                            db.name.clone(),
                                            schema.name.schema.clone(),
                                            entry.name().item.clone(),
                                        ],
                                        create_sql: entry.create_sql().to_string(),
                                    },
                                );
                            }
                        }
                        Statement::CreateSchema(ast::CreateSchemaStatement { name, .. }) => {
                            let [db_component, schema_component] = &name.0[..] else {
                                let name =
                                    name.0.into_iter().map(|ident| ident.to_string()).collect();
                                item_inconsistencies.push(
                                    ItemInconsistency::NonFullyQualifiedSchemaName {
                                        create_sql: entry.create_sql().to_string(),
                                        name,
                                    },
                                );
                                continue;
                            };
                            if db_component.as_str() != &db.name
                                || schema_component.as_str() != &schema.name.schema
                            {
                                item_inconsistencies.push(
                                    ItemInconsistency::CreateSqlSchemaNameMismatch {
                                        schema_name: vec![
                                            db.name.clone(),
                                            schema.name.schema.clone(),
                                        ],
                                        create_sql: entry.create_sql().to_string(),
                                    },
                                );
                            }
                        }
                        Statement::CreateDatabase(ast::CreateDatabaseStatement {
                            name, ..
                        }) => {
                            if db.name != name.0.as_str() {
                                item_inconsistencies.push(
                                    ItemInconsistency::CreateSqlDatabaseNameMismatch {
                                        database_name: db.name.clone(),
                                        create_sql: entry.create_sql().to_string(),
                                    },
                                );
                            }
                        }
                        _ => (),
                    }
                }
            }
        }

        if item_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(item_inconsistencies)
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum InternalFieldsInconsistency {
    Database(String, DatabaseId),
    AmbientSchema(String, SchemaId),
    Cluster(String, ClusterId),
    Role(String, RoleId),
    SourceReferences(CatalogItemId),
    EntryMissingGlobalIds(CatalogItemId, Vec<GlobalId>),
    GlobalIdsMissingEntry(GlobalId, CatalogItemId),
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum RoleInconsistency {
    Database(DatabaseId, RoleId),
    Schema(SchemaId, RoleId),
    Entry(CatalogItemId, RoleId),
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
    Membership {
        parent: Option<RoleId>,
        grantor: Option<RoleId>,
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
        object_a: CatalogItemId,
        object_b: CatalogItemId,
    },
    /// Object A is used by Object B, but Object B does not exist.
    MissingUsedBy {
        object_a: CatalogItemId,
        object_b: CatalogItemId,
    },
    /// Object A uses Object B, but Object B does not record that it is used by Object A.
    InconsistentUsedBy {
        object_a: CatalogItemId,
        object_b: CatalogItemId,
    },
    /// Object B is used by Object A, but Object B does not record that is uses Object A.
    InconsistentUses {
        object_a: CatalogItemId,
        object_b: CatalogItemId,
    },
}

#[derive(Debug, Serialize, Clone, PartialEq)]
enum ItemInconsistency {
    /// The name in a `Database` `schemas_by_name` does not match the name on the `Schema` struct.
    KeyedName {
        db_schema_by_name: String,
        struct_name: String,
    },
    /// A schema present in a `Database` `schemas_by_name` map is not in the `schema_by_id` map.
    MissingSchema {
        db_id: DatabaseId,
        schema_name: String,
    },
    /// An item in a `Schema` `items` collection does not exist.
    NonExistentItem {
        db_id: DatabaseId,
        schema_id: SchemaSpecifier,
        item_id: CatalogItemId,
    },
    /// An item in the `Schema` `items` collection has a mismatched name.
    ItemNameMismatch {
        item_id: CatalogItemId,
        /// Name from the `items` map.
        map_name: String,
        /// Name on the entry itself.
        entry_name: QualifiedItemName,
    },
    /// Failed to parse the `create_sql` persisted with an item.
    StatementParseFailure {
        create_sql: String,
        e: ParserStatementError,
    },
    /// Parsing the `create_sql` returned multiple Statements.
    MultiCreateStatement { create_sql: String },
    /// The name from a parsed `create_sql` statement, is not fully qualified.
    NonFullyQualifiedItemName {
        create_sql: String,
        name: Vec<String>,
    },
    /// The name from a parsed `create_sql` statement, is not fully qualified.
    NonFullyQualifiedSchemaName {
        create_sql: String,
        name: Vec<String>,
    },
    /// The name from a parsed `create_sql` statement, did not match that from the parent item.
    CreateSqlItemNameMismatch {
        item_name: Vec<String>,
        create_sql: String,
    },
    /// The name from a parsed `create_sql` statement, did not match that from the parent schema.
    CreateSqlSchemaNameMismatch {
        schema_name: Vec<String>,
        create_sql: String,
    },
    /// The name from a parsed `create_sql` statement, did not match that from the parent database.
    CreateSqlDatabaseNameMismatch {
        database_name: String,
        create_sql: String,
    },
}
