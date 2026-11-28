// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog-dependent DROP validation and cleanup.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_catalog::memory::objects::Index;
use mz_compute_client::controller::ComputeController;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::explain::ExprHumanizer;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogItem, CatalogRole,
    CatalogSchema, ErrorMessageObjectDescription, SessionCatalog,
};
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier, SchemaSpecifier, SystemObjectId};
use mz_sql::plan::{DropObjectsPlan, Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;

use crate::catalog::{CatalogState, ConnCatalog, DropObjectInfo, Op, UpdatePrivilegeVariant};
use crate::notice::DroppedInUseIndex;
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

use super::CommandExecution;

#[derive(Clone, Debug, Default)]
pub(crate) struct IndexDropNotices {
    pub orphaned: Vec<DroppedInUseIndex>,
    pub in_flight: Vec<(String, Vec<GlobalId>)>,
}

#[derive(Clone, Debug)]
pub struct DropStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    notices: Vec<PlanNotice>,
    adapter_notices: Vec<AdapterNotice>,
    index_notices: IndexDropNotices,
}

impl DropStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            notices: Vec::new(),
            adapter_notices: Vec::new(),
            index_notices: IndexDropNotices::default(),
        }
    }

    pub(crate) fn plan(&mut self, state: &CatalogState) -> Result<DropObjectsPlan, AdapterError> {
        self.adapter_notices.clear();
        self.index_notices = IndexDropNotices::default();
        let (result, notices) =
            super::plan_command_with_notices(state, &self.statement, &self.params, &self.session);
        self.notices = notices;
        let (plan, _) = result?;
        let Plan::DropObjects(plan) = plan else {
            return Err(AdapterError::Unsupported("non-DROP relation intent"));
        };
        Ok(plan)
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
        compute: Option<&ComputeController>,
    ) -> Result<CommandExecution<'_>, AdapterError> {
        let plan = self.plan(state)?;
        // Item dependency expansion cannot drop roles or namespaces. Their cleanup
        // requires a different compiler, not an unconditional DropObjects operation.
        if plan
            .drop_ids
            .iter()
            .any(|id| !matches!(id, ObjectId::Item(_)))
        {
            return Err(AdapterError::Internal(
                "relation DROP expanded to a non-item".into(),
            ));
        }
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let humanizer = state.for_session_metadata(&*self.session, tx);
        self.index_notices = check_index_dependents(state, &humanizer, compute, &plan.drop_ids)?;
        let referenced: BTreeSet<_> = plan.referenced_ids.iter().collect();
        let objects: Vec<_> = plan
            .drop_ids
            .iter()
            .filter(|id| !referenced.contains(id))
            .map(|id| ErrorMessageObjectDescription::from_id(id, &humanizer).to_string())
            .collect();
        self.adapter_notices.clear();
        if !objects.is_empty() {
            self.adapter_notices
                .push(AdapterNotice::CascadeDroppedObject { objects });
        }
        Ok(CommandExecution::raw(vec![Op::DropObjects {
            objects: plan
                .drop_ids
                .into_iter()
                .map(DropObjectInfo::manual_drop_from_object_id)
                .collect(),
            cascade: plan.cascade,
        }]))
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }
    pub(crate) fn adapter_notices(&self) -> &[AdapterNotice] {
        &self.adapter_notices
    }
    pub(crate) fn index_notices(&self) -> &IndexDropNotices {
        &self.index_notices
    }
}

pub(crate) fn check_index_dependents(
    state: &CatalogState,
    humanizer: &ConnCatalog<'_>,
    compute: Option<&ComputeController>,
    drop_ids: &[ObjectId],
) -> Result<IndexDropNotices, AdapterError> {
    let drop_in_use_allowed = state.system_config().enable_drop_in_use_index();
    let humanize_index = |index: &Index, item_id: &CatalogItemId| {
        humanizer
            .humanize_id(index.global_id())
            .unwrap_or_else(|| item_id.to_string())
    };

    let drop_set: BTreeSet<ObjectId> = drop_ids.iter().cloned().collect();
    let mut orphaned = Vec::new();
    let mut blocked = Vec::new();
    let mut transient_by_index = Vec::new();
    for id in drop_ids {
        let ObjectId::Item(item_id) = id else {
            continue;
        };
        let Some(index) = state.get_entry(item_id).index() else {
            continue;
        };
        let (durable, transient) = index_readers(
            state,
            compute.ok_or_else(|| {
                AdapterError::Internal("index drop requires live compute readers".into())
            })?,
            index,
            &drop_set,
        );
        if !transient.is_empty() {
            transient_by_index.push((index, item_id, transient));
        }
        if durable.is_empty() {
            continue;
        }
        if !drop_in_use_allowed {
            let dependents = durable
                .into_iter()
                .map(|dependent_id| {
                    let entry = state.get_entry(&dependent_id);
                    (
                        entry.item_type().to_string(),
                        humanizer.minimal_qualification(entry.name()).to_string(),
                    )
                })
                .collect();
            // Minimal qualification, like the planner's RESTRICT error.
            let index_name = humanizer
                .minimal_qualification(state.get_entry(item_id).name())
                .to_string();
            blocked.push((index_name, dependents));
            continue;
        }
        let dependant_objects = durable
            .into_iter()
            .filter_map(|dependent_id| {
                let entry = state.get_entry(&dependent_id);
                humanizer.humanize_id(entry.latest_global_id())
            })
            .collect();
        orphaned.push(DroppedInUseIndex {
            index_name: humanize_index(index, item_id),
            dependant_objects,
        });
    }

    if !blocked.is_empty() {
        return Err(AdapterError::IndexInUse { blocked });
    }

    Ok(IndexDropNotices {
        orphaned,
        in_flight: transient_by_index
            .into_iter()
            .map(|(index, item_id, readers)| (humanize_index(index, item_id), readers))
            .collect(),
    })
}

fn index_readers(
    state: &CatalogState,
    compute: &ComputeController,
    index: &Index,
    drop_set: &BTreeSet<ObjectId>,
) -> (Vec<CatalogItemId>, Vec<GlobalId>) {
    let mut durable = Vec::new();
    let mut transient = Vec::new();
    let readers = compute
        .collection_reverse_dependencies(index.cluster_id, index.global_id())
        .ok()
        .into_iter()
        .flatten();
    for gid in readers {
        if gid.is_transient() {
            transient.push(gid);
            continue;
        }
        // A collection whose item is gone from the catalog is already being torn down.
        let Some(entry) = state.try_get_entry_by_global_id(&gid) else {
            continue;
        };
        if !drop_set.contains(&ObjectId::Item(entry.id())) {
            durable.push(entry.id());
        }
    }
    (durable, transient)
}

pub(crate) struct DropOps {
    pub ops: Vec<Op>,
    pub dropped_active_db: bool,
    pub dropped_active_cluster: bool,
}

fn validate_dropped_role_ownership(
    state: &CatalogState,
    session: &dyn SessionMetadata,
    dropped_roles: &BTreeMap<RoleId, &str>,
) -> Result<(), AdapterError> {
    fn privilege_check(
        privileges: &PrivilegeMap,
        dropped_roles: &BTreeMap<RoleId, &str>,
        dependent_objects: &mut BTreeMap<String, Vec<String>>,
        object_id: &SystemObjectId,
        catalog: &ConnCatalog,
    ) {
        for privilege in privileges.all_values() {
            if let Some(role_name) = dropped_roles.get(&privilege.grantee) {
                let grantor_name = catalog.get_role(&privilege.grantor).name();
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(object_id, catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!(
                        "privileges on {object_description} granted by {grantor_name}",
                    ));
            }
            if let Some(role_name) = dropped_roles.get(&privilege.grantor) {
                let grantee_name = catalog.get_role(&privilege.grantee).name();
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(object_id, catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!(
                        "privileges granted on {object_description} to {grantee_name}"
                    ));
            }
        }
    }

    if dropped_roles.is_empty() {
        return Ok(());
    }
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let catalog = state.for_session_metadata(session, tx);
    let mut dependent_objects: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for entry in state.entries() {
        let id = SystemObjectId::Object(entry.id().into());
        if let Some(role_name) = dropped_roles.get(entry.owner_id()) {
            let object_description = ErrorMessageObjectDescription::from_sys_id(&id, &catalog);
            dependent_objects
                .entry(role_name.to_string())
                .or_default()
                .push(format!("owner of {object_description}"));
        }
        privilege_check(
            entry.privileges(),
            dropped_roles,
            &mut dependent_objects,
            &id,
            &catalog,
        );
    }
    for database in state.databases() {
        let database_id = SystemObjectId::Object(database.id().into());
        if let Some(role_name) = dropped_roles.get(&database.owner_id) {
            let object_description =
                ErrorMessageObjectDescription::from_sys_id(&database_id, &catalog);
            dependent_objects
                .entry(role_name.to_string())
                .or_default()
                .push(format!("owner of {object_description}"));
        }
        privilege_check(
            &database.privileges,
            dropped_roles,
            &mut dependent_objects,
            &database_id,
            &catalog,
        );
        for schema in database.schemas_by_id.values() {
            let schema_id = SystemObjectId::Object(
                (ResolvedDatabaseSpecifier::Id(database.id()), *schema.id()).into(),
            );
            if let Some(role_name) = dropped_roles.get(&schema.owner_id) {
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(&schema_id, &catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!("owner of {object_description}"));
            }
            privilege_check(
                &schema.privileges,
                dropped_roles,
                &mut dependent_objects,
                &schema_id,
                &catalog,
            );
        }
    }
    for cluster in state.clusters() {
        let cluster_id = SystemObjectId::Object(cluster.id().into());
        if let Some(role_name) = dropped_roles.get(&cluster.owner_id) {
            let object_description =
                ErrorMessageObjectDescription::from_sys_id(&cluster_id, &catalog);
            dependent_objects
                .entry(role_name.to_string())
                .or_default()
                .push(format!("owner of {object_description}"));
        }
        privilege_check(
            &cluster.privileges,
            dropped_roles,
            &mut dependent_objects,
            &cluster_id,
            &catalog,
        );
        for replica in cluster.replicas() {
            if let Some(role_name) = dropped_roles.get(&replica.owner_id) {
                let replica_id =
                    SystemObjectId::Object((replica.cluster_id(), replica.replica_id()).into());
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(&replica_id, &catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!("owner of {object_description}"));
            }
        }
    }
    for policy in catalog.get_query_policies() {
        let id = SystemObjectId::Object(ObjectId::QueryPolicy(policy.id()));
        if let Some(role_name) = dropped_roles.get(&policy.owner_id()) {
            let object_description = ErrorMessageObjectDescription::from_sys_id(&id, &catalog);
            dependent_objects
                .entry(role_name.to_string())
                .or_default()
                .push(format!("owner of {object_description}"));
        }
        privilege_check(
            policy.privileges(),
            dropped_roles,
            &mut dependent_objects,
            &id,
            &catalog,
        );
    }
    privilege_check(
        &state.system_privileges,
        dropped_roles,
        &mut dependent_objects,
        &SystemObjectId::System,
        &catalog,
    );
    for (default_privilege_object, default_privilege_acl_items) in state.default_privileges.iter() {
        if let Some(role_name) = dropped_roles.get(&default_privilege_object.role_id) {
            dependent_objects
                .entry(role_name.to_string())
                .or_default()
                .push(format!(
                    "default privileges on {}S created by {}",
                    default_privilege_object.object_type, role_name
                ));
        }
        for default_privilege_acl_item in default_privilege_acl_items {
            if let Some(role_name) = dropped_roles.get(&default_privilege_acl_item.grantee) {
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!(
                        "default privileges on {}S granted to {}",
                        default_privilege_object.object_type, role_name
                    ));
            }
        }
    }

    if !dependent_objects.is_empty() {
        Err(AdapterError::DependentObject(dependent_objects))
    } else {
        Ok(())
    }
}

pub(crate) fn drop_objects_ops(
    state: &CatalogState,
    session: &dyn SessionMetadata,
    ids: Vec<ObjectId>,
    cascade: bool,
) -> Result<DropOps, AdapterError> {
    let mut dropped_active_db = false;
    let mut dropped_active_cluster = false;
    let mut dropped_roles = BTreeMap::new();
    let mut dropped_databases = BTreeSet::new();
    let mut dropped_schemas = BTreeSet::new();
    // Dropping either the group role or the member role of a role membership will trigger a
    // revoke role. We use a Set for the revokes to avoid trying to attempt to revoke the same
    // role membership twice.
    let mut role_revokes = BTreeSet::new();
    // Dropping a database or a schema will revoke all default roles associated with that
    // database or schema.
    let mut default_privilege_revokes = BTreeSet::new();

    // Clusters we're dropping
    let mut clusters_to_drop = BTreeSet::new();

    for id in &ids {
        match id {
            ObjectId::Database(id) => {
                let name = state.get_database(id).name();
                if name == session.vars().database() {
                    dropped_active_db = true;
                }
                dropped_databases.insert(id);
            }
            ObjectId::Schema((_, spec)) => {
                if let SchemaSpecifier::Id(id) = spec {
                    dropped_schemas.insert(id);
                }
            }
            ObjectId::Cluster(id) => {
                clusters_to_drop.insert(*id);
                if let Some(active_id) = state
                    .active_cluster(session)
                    .ok()
                    .map(|cluster| cluster.id())
                {
                    if id == &active_id {
                        dropped_active_cluster = true;
                    }
                }
            }
            ObjectId::Role(id) => {
                let role = state.get_role(id);
                let name = role.name();
                dropped_roles.insert(*id, name);
                // We must revoke all role memberships that the dropped roles belongs to.
                for (group_id, grantor_id) in &role.membership.map {
                    role_revokes.insert((*group_id, *id, *grantor_id));
                }
            }
            _ => {}
        }
    }

    for id in &ids {
        match id {
            // Validate that `ClusterReplica` drops do not drop replicas of managed clusters,
            // unless they are internal replicas, which exist outside the scope
            // of managed clusters.
            ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                if !clusters_to_drop.contains(cluster_id) {
                    let cluster = state.get_cluster(*cluster_id);
                    if cluster.is_managed() {
                        let replica = cluster.replica(*replica_id).expect("Catalog out of sync");
                        if !replica.config.location.internal() {
                            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                                "cannot drop replica of managed cluster"
                            )));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    for role_id in dropped_roles.keys() {
        state.ensure_not_reserved_role(role_id)?;
    }
    validate_dropped_role_ownership(state, session, &dropped_roles)?;
    // If any role is a member of a dropped role, then we must revoke that membership.
    if !dropped_roles.is_empty() {
        let dropped_role_ids: BTreeSet<_> = dropped_roles.keys().collect();
        for role in state.user_roles() {
            for dropped_role_id in
                dropped_role_ids.intersection(&role.membership.map.keys().collect())
            {
                role_revokes.insert((
                    **dropped_role_id,
                    role.id(),
                    *role
                        .membership
                        .map
                        .get(*dropped_role_id)
                        .expect("included in keys above"),
                ));
            }
        }
    }

    if !dropped_databases.is_empty() || !dropped_schemas.is_empty() {
        for (default_privilege_object, default_privilege_acls) in state.default_privileges.iter() {
            if matches!(
                &default_privilege_object.database_id,
                Some(database_id) if dropped_databases.contains(database_id),
            ) || matches!(
                &default_privilege_object.schema_id,
                Some(schema_id) if dropped_schemas.contains(schema_id),
            ) {
                for default_privilege_acl in default_privilege_acls {
                    default_privilege_revokes.insert((
                        default_privilege_object.clone(),
                        default_privilege_acl.clone(),
                    ));
                }
            }
        }
    }

    let ops = role_revokes
        .into_iter()
        .map(|(role_id, member_id, grantor_id)| Op::RevokeRole {
            role_id,
            member_id,
            grantor_id,
        })
        .chain(default_privilege_revokes.into_iter().map(
            |(privilege_object, privilege_acl_item)| Op::UpdateDefaultPrivilege {
                privilege_object,
                privilege_acl_item,
                variant: UpdatePrivilegeVariant::Revoke,
            },
        ))
        .chain(std::iter::once(Op::DropObjects {
            objects: ids
                .into_iter()
                .map(DropObjectInfo::manual_drop_from_object_id)
                .collect(),
            cascade,
        }))
        .collect();

    Ok(DropOps {
        ops,
        dropped_active_db,
        dropped_active_cluster,
    })
}
