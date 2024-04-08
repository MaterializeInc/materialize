// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to applying updates from a [`mz_catalog::durable::DurableCatalogState`] to a
//! [`CatalogState`].

use crate::catalog::CatalogState;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{Database, Role, Schema, StateUpdate, StateUpdateKind};
use mz_ore::instrument;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::Diff;
use mz_sql::names::{QualifiedSchemaName, ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_sql::session::vars::{VarError, VarInput};
use std::collections::BTreeMap;
use std::fmt::Debug;
use tracing::warn;

impl CatalogState {
    /// Update in-memory catalog state from a list of updates made to the durable catalog state.
    ///
    /// This is meant specifically for bootstrapping because it does not produce builtin table
    /// updates. The builtin tables need to be loaded before we can produce builtin table updates
    /// which creates a bootstrapping problem.
    // TODO(jkosh44) It is very IMPORTANT that per timestamp, the updates are sorted retractions
    // then additions. Within the retractions the objects should be sorted in reverse dependency
    // order (objects->schema->database, replica->cluster, etc.). Within the additions the objects
    // should be sorted in dependency order (database->schema->objects, cluster->replica, etc.).
    // Objects themselves also need to be sorted by dependency order, this will be tricky but we can
    // look at the existing bootstrap code for ways of doing this. For now we rely on the caller
    // providing objects in dependency order.
    #[instrument]
    pub(crate) fn apply_updates_for_bootstrap(&mut self, updates: Vec<StateUpdate>) {
        for StateUpdate { kind, diff } in updates {
            assert_eq!(
                diff, 1,
                "initial catalog updates should be consolidated: ({kind:?}, {diff:?})"
            );
            self.apply_update(kind, diff);
        }
    }

    #[instrument(level = "debug")]
    fn apply_update(&mut self, kind: StateUpdateKind, diff: Diff) {
        assert!(
            diff == 1 || diff == -1,
            "invalid update in catalog updates: ({kind:?}, {diff:?})"
        );
        match kind {
            StateUpdateKind::Role(role) => self.apply_role_update(role, diff),
            StateUpdateKind::Database(database) => self.apply_database_update(database, diff),
            StateUpdateKind::Schema(schema) => {
                self.apply_schema_update(schema, diff);
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                self.apply_default_privilege_update(default_privilege, diff)
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                self.apply_system_privilege_update(system_privilege, diff)
            }
            StateUpdateKind::SystemConfiguration(system_configuration) => {
                self.apply_system_configuration_update(system_configuration, diff)
            }
            StateUpdateKind::Comment(comment) => self.apply_comment_update(comment, diff),
        }
    }

    #[instrument(level = "debug")]
    fn apply_role_update(&mut self, role: mz_catalog::durable::Role, diff: Diff) {
        apply(
            &mut self.roles_by_id,
            role.id,
            || Role {
                name: role.name.clone(),
                id: role.id,
                oid: role.oid,
                attributes: role.attributes,
                membership: role.membership,
                vars: role.vars,
            },
            diff,
        );
        apply(&mut self.roles_by_name, role.name, || role.id, diff);
    }

    #[instrument(level = "debug")]
    fn apply_database_update(&mut self, database: mz_catalog::durable::Database, diff: Diff) {
        apply(
            &mut self.database_by_id,
            database.id.clone(),
            || Database {
                name: database.name.clone(),
                id: database.id.clone(),
                oid: database.oid,
                schemas_by_id: BTreeMap::new(),
                schemas_by_name: BTreeMap::new(),
                owner_id: database.owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(database.privileges),
            },
            diff,
        );
        apply(
            &mut self.database_by_name,
            database.name,
            || database.id.clone(),
            diff,
        );
    }

    #[instrument(level = "debug")]
    fn apply_schema_update(&mut self, schema: mz_catalog::durable::Schema, diff: Diff) {
        let (schemas_by_id, schemas_by_name, database_spec) = match &schema.database_id {
            Some(database_id) => {
                let db = self
                    .database_by_id
                    .get_mut(database_id)
                    .expect("catalog out of sync");
                (
                    &mut db.schemas_by_id,
                    &mut db.schemas_by_name,
                    ResolvedDatabaseSpecifier::Id(*database_id),
                )
            }
            None => (
                &mut self.ambient_schemas_by_id,
                &mut self.ambient_schemas_by_name,
                ResolvedDatabaseSpecifier::Ambient,
            ),
        };
        apply(
            schemas_by_id,
            schema.id.clone(),
            || Schema {
                name: QualifiedSchemaName {
                    database: database_spec,
                    schema: schema.name.clone(),
                },
                id: SchemaSpecifier::Id(schema.id.clone()),
                oid: schema.oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                types: BTreeMap::new(),
                owner_id: schema.owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(schema.privileges),
            },
            diff,
        );
        apply(schemas_by_name, schema.name.clone(), || schema.id, diff);
    }

    #[instrument(level = "debug")]
    fn apply_default_privilege_update(
        &mut self,
        default_privilege: mz_catalog::durable::DefaultPrivilege,
        diff: Diff,
    ) {
        match diff {
            1 => self
                .default_privileges
                .grant(default_privilege.object, default_privilege.acl_item),
            -1 => self
                .default_privileges
                .revoke(&default_privilege.object, &default_privilege.acl_item),
            _ => unreachable!("invalid diff: {diff}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_privilege_update(&mut self, system_privilege: MzAclItem, diff: Diff) {
        match diff {
            1 => self.system_privileges.grant(system_privilege),
            -1 => self.system_privileges.revoke(&system_privilege),
            _ => unreachable!("invalid diff: {diff}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_configuration_update(
        &mut self,
        system_configuration: mz_catalog::durable::SystemConfiguration,
        diff: Diff,
    ) {
        let res = match diff {
            1 => self.insert_system_configuration(
                &system_configuration.name,
                VarInput::Flat(&system_configuration.value),
            ),
            -1 => self.remove_system_configuration(&system_configuration.name),
            _ => unreachable!("invalid diff: {diff}"),
        };
        match res {
            Ok(_) => (),
            // When system variables are deleted, nothing deletes them from the underlying
            // durable catalog, which isn't great. Still, we need to be able to ignore
            // unknown variables.
            Err(Error {
                kind: ErrorKind::VarError(VarError::UnknownParameter(name)),
            }) => {
                warn!(%name, "unknown system parameter from catalog storage");
            }
            Err(e) => panic!("unable to update system variable: {e:?}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_comment_update(&mut self, comment: mz_catalog::durable::Comment, diff: Diff) {
        match diff {
            1 => {
                let prev = self.comments.update_comment(
                    comment.object_id,
                    comment.sub_component,
                    Some(comment.comment),
                );
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            }
            -1 => {
                let prev =
                    self.comments
                        .update_comment(comment.object_id, comment.sub_component, None);
                assert_eq!(
                    prev,
                    Some(comment.comment),
                    "retraction does not match existing value: ({:?}, {:?})",
                    comment.object_id,
                    comment.sub_component,
                );
            }
            _ => unreachable!("invalid diff: {diff}"),
        }
    }
}

fn apply<K, V>(map: &mut BTreeMap<K, V>, key: K, value: impl FnOnce() -> V, diff: Diff)
where
    K: Ord + Debug,
    V: PartialEq + Eq + Debug,
{
    if diff == 1 {
        let prev = map.insert(key, value());
        assert_eq!(
            prev, None,
            "values must be explicitly retracted before inserting a new value"
        );
    } else if diff == -1 {
        let prev = map.remove(&key);
        // We can't assert the exact contents of the previous value, since we don't know
        // what it should look like.
        assert!(
            prev.is_some(),
            "retraction does not match existing value: {key:?}"
        );
    }
}
