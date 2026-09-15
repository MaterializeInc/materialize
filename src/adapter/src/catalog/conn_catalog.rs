// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving-session additions to the state-only catalog view.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::now::EpochMillis;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersionSelector, SqlScalarType};
use mz_sql::catalog::{
    CatalogClusterReplica, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogRole, CatalogSchema, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, SessionCatalog,
};
use mz_sql::names::{
    DatabaseId, FullItemName, FullSchemaName, ObjectId, PartialItemName, QualifiedItemName,
    QualifiedSchemaName, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{PlanNotice, StatementDesc};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_types::connections::inline::{ConnectionResolver, InlinedConnection};
use tokio::sync::mpsc::UnboundedSender;

use crate::AdapterNotice;
use crate::catalog::CatalogStateView;
use crate::session::{Portal, PreparedStatement};

/// A catalog view with access to a serving session's statements, portals, and notices.
#[derive(Debug)]
pub struct ConnCatalog<'a> {
    pub(super) view: CatalogStateView<'a>,
    pub(super) prepared_statements: Option<&'a BTreeMap<String, PreparedStatement>>,
    pub(super) portals: Option<&'a BTreeMap<String, Portal>>,
    pub(super) notices_tx: Option<UnboundedSender<AdapterNotice>>,
}

impl<'a> From<CatalogStateView<'a>> for ConnCatalog<'a> {
    fn from(view: CatalogStateView<'a>) -> Self {
        Self {
            view,
            prepared_statements: None,
            portals: None,
            notices_tx: None,
        }
    }
}

impl<'a> Deref for ConnCatalog<'a> {
    type Target = CatalogStateView<'a>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl ConnCatalog<'_> {
    /// Prevent resolution of an item while replanning, only for the system role.
    ///
    /// See [`CatalogStateView::mark_id_unresolvable_for_replanning`].
    pub fn mark_id_unresolvable_for_replanning(&mut self, id: CatalogItemId) {
        self.view.mark_id_unresolvable_for_replanning(id);
    }
}

impl ConnectionResolver for ConnCatalog<'_> {
    fn resolve_connection(
        &self,
        id: CatalogItemId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        self.view.resolve_connection(id)
    }
}

impl ExprHumanizer for ConnCatalog<'_> {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.view.humanize_id(id)
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        self.view.humanize_id_unqualified(id)
    }

    fn humanize_id_parts(&self, id: GlobalId) -> Option<Vec<String>> {
        self.view.humanize_id_parts(id)
    }

    fn humanize_sql_scalar_type(&self, typ: &SqlScalarType, postgres_compat: bool) -> String {
        self.view.humanize_sql_scalar_type(typ, postgres_compat)
    }

    fn column_names_for_id(&self, id: GlobalId) -> Option<Vec<String>> {
        self.view.column_names_for_id(id)
    }

    fn humanize_column(&self, id: GlobalId, column: usize) -> Option<String> {
        self.view.humanize_column(id, column)
    }

    fn id_exists(&self, id: GlobalId) -> bool {
        self.view.id_exists(id)
    }
}

impl SessionCatalog for ConnCatalog<'_> {
    fn active_role_id(&self) -> &RoleId {
        self.view.active_role_id()
    }

    fn restrict_to_user_objects(&self) -> bool {
        self.view.restrict_to_user_objects()
    }

    fn get_prepared_statement_desc(&self, name: &str) -> Option<&StatementDesc> {
        self.prepared_statements
            .as_ref()
            .map(|ps| ps.get(name).map(|ps| ps.desc()))
            .flatten()
    }

    fn get_portal_desc_unverified(&self, portal_name: &str) -> Option<&StatementDesc> {
        self.portals
            .and_then(|portals| portals.get(portal_name).map(|portal| &portal.desc))
    }

    fn active_database(&self) -> Option<&DatabaseId> {
        self.view.active_database()
    }

    fn active_cluster(&self) -> &str {
        self.view.active_cluster()
    }

    fn search_path(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)] {
        self.view.search_path()
    }

    fn resolve_database(
        &self,
        database_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogDatabase, SqlCatalogError> {
        self.view.resolve_database(database_name)
    }

    fn get_database(&self, id: &DatabaseId) -> &dyn mz_sql::catalog::CatalogDatabase {
        self.view.get_database(id)
    }

    fn get_databases(&self) -> Vec<&dyn CatalogDatabase> {
        self.view.get_databases()
    }

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        self.view.resolve_schema(database_name, schema_name)
    }

    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        self.view
            .resolve_schema_in_database(database_spec, schema_name)
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.view.get_schema(database_spec, schema_spec)
    }

    fn get_schemas(&self) -> Vec<&dyn CatalogSchema> {
        self.view.get_schemas()
    }

    fn get_mz_internal_schema_id(&self) -> SchemaId {
        self.view.get_mz_internal_schema_id()
    }

    fn get_mz_unsafe_schema_id(&self) -> SchemaId {
        self.view.get_mz_unsafe_schema_id()
    }

    fn is_system_schema_specifier(&self, schema: SchemaSpecifier) -> bool {
        self.view.is_system_schema_specifier(schema)
    }

    fn resolve_role(
        &self,
        role_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogRole, SqlCatalogError> {
        self.view.resolve_role(role_name)
    }

    fn resolve_network_policy(
        &self,
        policy_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogNetworkPolicy, SqlCatalogError> {
        self.view.resolve_network_policy(policy_name)
    }

    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole> {
        self.view.try_get_role(id)
    }

    fn get_role(&self, id: &RoleId) -> &dyn mz_sql::catalog::CatalogRole {
        self.view.get_role(id)
    }

    fn get_roles(&self) -> Vec<&dyn CatalogRole> {
        self.view.get_roles()
    }

    fn mz_system_role_id(&self) -> RoleId {
        self.view.mz_system_role_id()
    }

    fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        self.view.collect_role_membership(id)
    }

    fn get_network_policy(
        &self,
        id: &NetworkPolicyId,
    ) -> &dyn mz_sql::catalog::CatalogNetworkPolicy {
        self.view.get_network_policy(id)
    }

    fn get_network_policies(&self) -> Vec<&dyn mz_sql::catalog::CatalogNetworkPolicy> {
        self.view.get_network_policies()
    }

    fn resolve_cluster(
        &self,
        cluster_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogCluster<'_>, SqlCatalogError> {
        self.view.resolve_cluster(cluster_name)
    }

    fn resolve_cluster_replica(
        &self,
        cluster_replica_name: &QualifiedReplica,
    ) -> Result<&dyn CatalogClusterReplica<'_>, SqlCatalogError> {
        self.view.resolve_cluster_replica(cluster_replica_name)
    }

    fn resolve_item(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        self.view.resolve_item(name)
    }

    fn resolve_function(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        self.view.resolve_function(name)
    }

    fn resolve_type(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        self.view.resolve_type(name)
    }

    fn get_system_type(&self, name: &str) -> &dyn mz_sql::catalog::CatalogItem {
        self.view.get_system_type(name)
    }

    fn try_get_item(&self, id: &CatalogItemId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        self.view.try_get_item(id)
    }

    fn try_get_item_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Option<Box<dyn mz_sql::catalog::CatalogCollectionItem + '_>> {
        self.view.try_get_item_by_global_id(id)
    }

    fn get_item(&self, id: &CatalogItemId) -> &dyn mz_sql::catalog::CatalogItem {
        self.view.get_item(id)
    }

    fn get_item_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem + '_> {
        self.view.get_item_by_global_id(id)
    }

    fn get_items(&self) -> Vec<&dyn mz_sql::catalog::CatalogItem> {
        self.view.get_items()
    }

    fn get_item_by_name(&self, name: &QualifiedItemName) -> Option<&dyn SqlCatalogItem> {
        self.view.get_item_by_name(name)
    }

    fn get_type_by_name(&self, name: &QualifiedItemName) -> Option<&dyn SqlCatalogItem> {
        self.view.get_type_by_name(name)
    }

    fn get_cluster(&self, id: ClusterId) -> &dyn mz_sql::catalog::CatalogCluster<'_> {
        self.view.get_cluster(id)
    }

    fn get_clusters(&self) -> Vec<&dyn mz_sql::catalog::CatalogCluster<'_>> {
        self.view.get_clusters()
    }

    fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &dyn mz_sql::catalog::CatalogClusterReplica<'_> {
        self.view.get_cluster_replica(cluster_id, replica_id)
    }

    fn get_cluster_replicas(&self) -> Vec<&dyn mz_sql::catalog::CatalogClusterReplica<'_>> {
        self.view.get_cluster_replicas()
    }

    fn get_system_privileges(&self) -> &PrivilegeMap {
        self.view.get_system_privileges()
    }

    fn get_default_privileges(
        &self,
    ) -> Vec<(&DefaultPrivilegeObject, Vec<&DefaultPrivilegeAclItem>)> {
        self.view.get_default_privileges()
    }

    fn find_available_name(&self, name: QualifiedItemName) -> QualifiedItemName {
        self.view.find_available_name(name)
    }

    fn resolve_full_name(&self, name: &QualifiedItemName) -> FullItemName {
        self.view.resolve_full_name(name)
    }

    fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
        self.view.resolve_full_schema_name(name)
    }

    fn resolve_item_id(&self, global_id: &GlobalId) -> CatalogItemId {
        self.view.resolve_item_id(global_id)
    }

    fn resolve_global_id(
        &self,
        item_id: &CatalogItemId,
        version: RelationVersionSelector,
    ) -> GlobalId {
        self.view.resolve_global_id(item_id, version)
    }

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.view.config()
    }

    fn now(&self) -> EpochMillis {
        self.view.now()
    }

    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>> {
        self.view.aws_privatelink_availability_zones()
    }

    fn system_vars(&self) -> &SystemVars {
        self.view.system_vars()
    }

    fn system_vars_mut(&mut self) -> &mut SystemVars {
        self.view.system_vars_mut()
    }

    fn get_owner_id(&self, id: &ObjectId) -> Option<RoleId> {
        self.view.get_owner_id(id)
    }

    fn get_privileges(&self, id: &SystemObjectId) -> Option<&PrivilegeMap> {
        self.view.get_privileges(id)
    }

    fn object_dependents(&self, ids: &Vec<ObjectId>) -> Vec<ObjectId> {
        self.view.object_dependents(ids)
    }

    fn item_dependents(&self, id: CatalogItemId) -> Vec<ObjectId> {
        self.view.item_dependents(id)
    }

    fn all_object_privileges(&self, object_type: mz_sql::catalog::SystemObjectType) -> AclMode {
        self.view.all_object_privileges(object_type)
    }

    fn get_object_type(&self, object_id: &ObjectId) -> mz_sql::catalog::ObjectType {
        self.view.get_object_type(object_id)
    }

    fn get_system_object_type(&self, id: &SystemObjectId) -> mz_sql::catalog::SystemObjectType {
        self.view.get_system_object_type(id)
    }

    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName {
        self.view.minimal_qualification(qualified_name)
    }

    fn add_notice(&self, notice: PlanNotice) {
        if let Some(notices_tx) = &self.notices_tx {
            let _ = notices_tx.send(notice.into());
        }
    }

    fn get_item_comments(&self, id: &CatalogItemId) -> Option<&BTreeMap<Option<usize>, String>> {
        self.view.get_item_comments(id)
    }

    fn is_cluster_size_cc(&self, size: &str) -> bool {
        self.view.is_cluster_size_cc(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::session::{Session, StateRevision};
    use mz_repr::RelationDesc;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn serving_catalog_retains_session_descriptions_and_notices() {
        Catalog::with_debug(|catalog| async move {
            let mut session = Session::dummy();
            let desc = StatementDesc::new(Some(RelationDesc::empty()));
            let revision = StateRevision {
                catalog_revision: catalog.transient_revision(),
                session_state_revision: session.state_revision(),
            };
            session.set_prepared_statement(
                "statement".into(), None, String::new(), desc.clone(), revision, 0,
            );
            let logging = std::sync::Arc::clone(
                session.get_prepared_statement_unverified("statement")
                    .expect("registered statement").logging(),
            );
            session.set_portal(
                "portal".into(), desc.clone(), None, logging, Vec::new(), Vec::new(), revision,
            ).expect("bind portal");

            let serving = catalog.for_session(&session);
            assert_eq!(serving.get_prepared_statement_desc("statement"), Some(&desc));
            assert_eq!(serving.get_portal_desc_unverified("portal"), Some(&desc));
            let sessionless = catalog.state().for_system_session();
            assert!(sessionless.get_prepared_statement_desc("statement").is_none());
            assert!(sessionless.get_portal_desc_unverified("portal").is_none());

            let notice = PlanNotice::ReplicaDiskOptionDeprecated;
            sessionless.add_notice(notice.clone());
            serving.add_notice(notice.clone());
            drop(serving);
            let notices = session.drain_notices();
            assert!(matches!(notices.as_slice(), [AdapterNotice::PlanNotice(actual)] if actual == &notice));
        }).await;
    }
}
