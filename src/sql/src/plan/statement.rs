// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Statement planning.
//!
//! This module houses the entry points for planning a SQL statement.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use mz_repr::namespaces::is_system_schema;
use mz_repr::{
    CatalogItemId, ColumnIndex, ColumnType, RelationDesc, RelationVersionSelector, ScalarType,
};
use mz_sql_parser::ast::{
    ColumnDef, ColumnName, ConnectionDefaultAwsPrivatelink, CreateMaterializedViewStatement,
    RawItemName, ShowStatement, StatementKind, TableConstraint, UnresolvedDatabaseName,
    UnresolvedSchemaName,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::{AwsPrivatelink, Connection, SshTunnel, Tunnel};

use crate::ast::{Ident, Statement, UnresolvedItemName};
use crate::catalog::{
    CatalogCluster, CatalogCollectionItem, CatalogDatabase, CatalogItem, CatalogItemType,
    CatalogSchema, ObjectType, SessionCatalog, SystemObjectType,
};
use crate::names::{
    self, Aug, DatabaseId, FullItemName, ItemQualifiers, ObjectId, PartialItemName,
    QualifiedItemName, RawDatabaseSpecifier, ResolvedColumnReference, ResolvedDataType,
    ResolvedDatabaseSpecifier, ResolvedIds, ResolvedItemName, ResolvedSchemaName, SchemaSpecifier,
    SystemObjectId,
};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::{Params, Plan, PlanContext, PlanKind, query, with_options};
use crate::session::vars::FeatureFlag;

mod acl;
pub(crate) mod ddl;
mod dml;
mod raise;
mod scl;
pub(crate) mod show;
mod tcl;
mod validate;

use crate::session::vars;
pub(crate) use ddl::PgConfigOptionExtracted;
use mz_controller_types::ClusterId;
use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_USER_OID};
use mz_repr::role_id::RoleId;

/// Describes the output of a SQL statement.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatementDesc {
    /// The shape of the rows produced by the statement, if the statement
    /// produces rows.
    pub relation_desc: Option<RelationDesc>,
    /// The determined types of the parameters in the statement, if any.
    pub param_types: Vec<ScalarType>,
    /// Whether the statement is a `COPY` statement.
    pub is_copy: bool,
}

impl StatementDesc {
    pub fn new(relation_desc: Option<RelationDesc>) -> Self {
        StatementDesc {
            relation_desc,
            param_types: vec![],
            is_copy: false,
        }
    }

    /// Reports the number of columns in the statement's result set, or zero if
    /// the statement does not return rows.
    pub fn arity(&self) -> usize {
        self.relation_desc
            .as_ref()
            .map(|desc| desc.typ().column_types.len())
            .unwrap_or(0)
    }

    fn with_params(mut self, param_types: Vec<ScalarType>) -> Self {
        self.param_types = param_types;
        self
    }

    fn with_is_copy(mut self) -> Self {
        self.is_copy = true;
        self
    }
}

/// Creates a description of the purified statement `stmt`.
///
/// See the documentation of [`StatementDesc`] for details.
pub fn describe(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    stmt: Statement<Aug>,
    param_types_in: &[Option<ScalarType>],
) -> Result<StatementDesc, PlanError> {
    let mut param_types = BTreeMap::new();
    for (i, ty) in param_types_in.iter().enumerate() {
        if let Some(ty) = ty {
            param_types.insert(i + 1, ty.clone());
        }
    }

    let scx = StatementContext {
        pcx: Some(pcx),
        catalog,
        param_types: RefCell::new(param_types),
        ambiguous_columns: RefCell::new(false),
    };

    let desc = match stmt {
        // DDL statements.
        Statement::AlterCluster(stmt) => ddl::describe_alter_cluster_set_options(&scx, stmt)?,
        Statement::AlterConnection(stmt) => ddl::describe_alter_connection(&scx, stmt)?,
        Statement::AlterIndex(stmt) => ddl::describe_alter_index_options(&scx, stmt)?,
        Statement::AlterObjectRename(stmt) => ddl::describe_alter_object_rename(&scx, stmt)?,
        Statement::AlterObjectSwap(stmt) => ddl::describe_alter_object_swap(&scx, stmt)?,
        Statement::AlterRetainHistory(stmt) => ddl::describe_alter_retain_history(&scx, stmt)?,
        Statement::AlterRole(stmt) => ddl::describe_alter_role(&scx, stmt)?,
        Statement::AlterSecret(stmt) => ddl::describe_alter_secret_options(&scx, stmt)?,
        Statement::AlterSetCluster(stmt) => ddl::describe_alter_set_cluster(&scx, stmt)?,
        Statement::AlterSink(stmt) => ddl::describe_alter_sink(&scx, stmt)?,
        Statement::AlterSource(stmt) => ddl::describe_alter_source(&scx, stmt)?,
        Statement::AlterSystemSet(stmt) => ddl::describe_alter_system_set(&scx, stmt)?,
        Statement::AlterSystemReset(stmt) => ddl::describe_alter_system_reset(&scx, stmt)?,
        Statement::AlterSystemResetAll(stmt) => ddl::describe_alter_system_reset_all(&scx, stmt)?,
        Statement::AlterTableAddColumn(stmt) => ddl::describe_alter_table_add_column(&scx, stmt)?,
        Statement::AlterNetworkPolicy(stmt) => ddl::describe_alter_network_policy(&scx, stmt)?,
        Statement::Comment(stmt) => ddl::describe_comment(&scx, stmt)?,
        Statement::CreateCluster(stmt) => ddl::describe_create_cluster(&scx, stmt)?,
        Statement::CreateClusterReplica(stmt) => ddl::describe_create_cluster_replica(&scx, stmt)?,
        Statement::CreateConnection(stmt) => ddl::describe_create_connection(&scx, stmt)?,
        Statement::CreateDatabase(stmt) => ddl::describe_create_database(&scx, stmt)?,
        Statement::CreateIndex(stmt) => ddl::describe_create_index(&scx, stmt)?,
        Statement::CreateRole(stmt) => ddl::describe_create_role(&scx, stmt)?,
        Statement::CreateSchema(stmt) => ddl::describe_create_schema(&scx, stmt)?,
        Statement::CreateSecret(stmt) => ddl::describe_create_secret(&scx, stmt)?,
        Statement::CreateSink(stmt) => ddl::describe_create_sink(&scx, stmt)?,
        Statement::CreateWebhookSource(stmt) => ddl::describe_create_webhook_source(&scx, stmt)?,
        Statement::CreateSource(stmt) => ddl::describe_create_source(&scx, stmt)?,
        Statement::CreateSubsource(stmt) => ddl::describe_create_subsource(&scx, stmt)?,
        Statement::CreateTable(stmt) => ddl::describe_create_table(&scx, stmt)?,
        Statement::CreateTableFromSource(stmt) => {
            ddl::describe_create_table_from_source(&scx, stmt)?
        }
        Statement::CreateType(stmt) => ddl::describe_create_type(&scx, stmt)?,
        Statement::CreateView(stmt) => ddl::describe_create_view(&scx, stmt)?,
        Statement::CreateMaterializedView(stmt) => {
            ddl::describe_create_materialized_view(&scx, stmt)?
        }
        Statement::CreateContinualTask(stmt) => ddl::describe_create_continual_task(&scx, stmt)?,
        Statement::CreateNetworkPolicy(stmt) => ddl::describe_create_network_policy(&scx, stmt)?,
        Statement::DropObjects(stmt) => ddl::describe_drop_objects(&scx, stmt)?,
        Statement::DropOwned(stmt) => ddl::describe_drop_owned(&scx, stmt)?,

        // `ACL` statements.
        Statement::AlterOwner(stmt) => acl::describe_alter_owner(&scx, stmt)?,
        Statement::GrantRole(stmt) => acl::describe_grant_role(&scx, stmt)?,
        Statement::RevokeRole(stmt) => acl::describe_revoke_role(&scx, stmt)?,
        Statement::GrantPrivileges(stmt) => acl::describe_grant_privileges(&scx, stmt)?,
        Statement::RevokePrivileges(stmt) => acl::describe_revoke_privileges(&scx, stmt)?,
        Statement::AlterDefaultPrivileges(stmt) => {
            acl::describe_alter_default_privileges(&scx, stmt)?
        }
        Statement::ReassignOwned(stmt) => acl::describe_reassign_owned(&scx, stmt)?,

        // `SHOW` statements.
        Statement::Show(ShowStatement::ShowColumns(stmt)) => {
            show::show_columns(&scx, stmt)?.describe()?
        }
        Statement::Show(ShowStatement::ShowCreateConnection(stmt)) => {
            show::describe_show_create_connection(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateCluster(stmt)) => {
            show::describe_show_create_cluster(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateIndex(stmt)) => {
            show::describe_show_create_index(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateSink(stmt)) => {
            show::describe_show_create_sink(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateSource(stmt)) => {
            show::describe_show_create_source(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateTable(stmt)) => {
            show::describe_show_create_table(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateView(stmt)) => {
            show::describe_show_create_view(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowCreateMaterializedView(stmt)) => {
            show::describe_show_create_materialized_view(&scx, stmt)?
        }
        Statement::Show(ShowStatement::ShowObjects(stmt)) => {
            show::show_objects(&scx, stmt)?.describe()?
        }

        // SCL statements.
        Statement::Close(stmt) => scl::describe_close(&scx, stmt)?,
        Statement::Deallocate(stmt) => scl::describe_deallocate(&scx, stmt)?,
        Statement::Declare(stmt) => scl::describe_declare(&scx, stmt, param_types_in)?,
        Statement::Discard(stmt) => scl::describe_discard(&scx, stmt)?,
        Statement::Execute(stmt) => scl::describe_execute(&scx, stmt)?,
        Statement::Fetch(stmt) => scl::describe_fetch(&scx, stmt)?,
        Statement::Prepare(stmt) => scl::describe_prepare(&scx, stmt)?,
        Statement::ResetVariable(stmt) => scl::describe_reset_variable(&scx, stmt)?,
        Statement::SetVariable(stmt) => scl::describe_set_variable(&scx, stmt)?,
        Statement::Show(ShowStatement::ShowVariable(stmt)) => {
            scl::describe_show_variable(&scx, stmt)?
        }

        // DML statements.
        Statement::Copy(stmt) => dml::describe_copy(&scx, stmt)?,
        Statement::Delete(stmt) => dml::describe_delete(&scx, stmt)?,
        Statement::ExplainPlan(stmt) => dml::describe_explain_plan(&scx, stmt)?,
        Statement::ExplainPushdown(stmt) => dml::describe_explain_pushdown(&scx, stmt)?,
        Statement::ExplainAnalyze(stmt) => dml::describe_explain_analyze(&scx, stmt)?,
        Statement::ExplainTimestamp(stmt) => dml::describe_explain_timestamp(&scx, stmt)?,
        Statement::ExplainSinkSchema(stmt) => dml::describe_explain_schema(&scx, stmt)?,
        Statement::Insert(stmt) => dml::describe_insert(&scx, stmt)?,
        Statement::Select(stmt) => dml::describe_select(&scx, stmt)?,
        Statement::Subscribe(stmt) => dml::describe_subscribe(&scx, stmt)?,
        Statement::Update(stmt) => dml::describe_update(&scx, stmt)?,

        // TCL statements.
        Statement::Commit(stmt) => tcl::describe_commit(&scx, stmt)?,
        Statement::Rollback(stmt) => tcl::describe_rollback(&scx, stmt)?,
        Statement::SetTransaction(stmt) => tcl::describe_set_transaction(&scx, stmt)?,
        Statement::StartTransaction(stmt) => tcl::describe_start_transaction(&scx, stmt)?,

        // Other statements.
        Statement::Raise(stmt) => raise::describe_raise(&scx, stmt)?,
        Statement::Show(ShowStatement::InspectShard(stmt)) => {
            scl::describe_inspect_shard(&scx, stmt)?
        }
        Statement::ValidateConnection(stmt) => validate::describe_validate_connection(&scx, stmt)?,
    };

    let desc = desc.with_params(scx.finalize_param_types()?);
    Ok(desc)
}

/// Produces a [`Plan`] from the purified statement `stmt`.
///
/// Planning is a pure, synchronous function and so requires that the provided
/// `stmt` does does not depend on any external state. Statements that rely on
/// external state must remove that state prior to calling this function via
/// [`crate::pure::purify_statement`] or
/// [`crate::pure::purify_create_materialized_view_options`].
///
/// The returned plan is tied to the state of the provided catalog. If the state
/// of the catalog changes after planning, the validity of the plan is not
/// guaranteed.
///
/// Note that if you want to do something else asynchronously (e.g. validating
/// connections), these might want to take different code paths than
/// `purify_statement`. Feel free to rationalize this by thinking of those
/// statements as not necessarily depending on external state.
#[mz_ore::instrument(level = "debug")]
pub fn plan(
    pcx: Option<&PlanContext>,
    catalog: &dyn SessionCatalog,
    stmt: Statement<Aug>,
    params: &Params,
    resolved_ids: &ResolvedIds,
) -> Result<Plan, PlanError> {
    let param_types = params
        // We need the `expected_types` here, not the `actual_types`! This is because
        // `expected_types` is how the parameter expression (e.g. `$1`) looks "from the outside":
        // `bind_parameters` will insert a cast from the actual type to the expected type.
        .expected_types
        .iter()
        .enumerate()
        .map(|(i, ty)| (i + 1, ty.clone()))
        .collect();

    let kind: StatementKind = (&stmt).into();
    let permitted_plans = Plan::generated_from(&kind);

    let scx = &mut StatementContext {
        pcx,
        catalog,
        param_types: RefCell::new(param_types),
        ambiguous_columns: RefCell::new(false),
    };

    if resolved_ids
        .items()
        // Filter out items that may not have been created yet, such as sub-sources.
        .filter_map(|id| catalog.try_get_item(id))
        .any(|item| {
            item.func().is_ok()
                && item.name().qualifiers.schema_spec
                    == SchemaSpecifier::Id(catalog.get_mz_unsafe_schema_id())
        })
    {
        scx.require_feature_flag(&vars::UNSAFE_ENABLE_UNSAFE_FUNCTIONS)?;
    }

    let plan = match stmt {
        // DDL statements.
        Statement::AlterCluster(stmt) => ddl::plan_alter_cluster(scx, stmt),
        Statement::AlterConnection(stmt) => ddl::plan_alter_connection(scx, stmt),
        Statement::AlterIndex(stmt) => ddl::plan_alter_index_options(scx, stmt),
        Statement::AlterObjectRename(stmt) => ddl::plan_alter_object_rename(scx, stmt),
        Statement::AlterObjectSwap(stmt) => ddl::plan_alter_object_swap(scx, stmt),
        Statement::AlterRetainHistory(stmt) => ddl::plan_alter_retain_history(scx, stmt),
        Statement::AlterRole(stmt) => ddl::plan_alter_role(scx, stmt),
        Statement::AlterSecret(stmt) => ddl::plan_alter_secret(scx, stmt),
        Statement::AlterSetCluster(stmt) => ddl::plan_alter_item_set_cluster(scx, stmt),
        Statement::AlterSink(stmt) => ddl::plan_alter_sink(scx, stmt),
        Statement::AlterSource(stmt) => ddl::plan_alter_source(scx, stmt),
        Statement::AlterSystemSet(stmt) => ddl::plan_alter_system_set(scx, stmt),
        Statement::AlterSystemReset(stmt) => ddl::plan_alter_system_reset(scx, stmt),
        Statement::AlterSystemResetAll(stmt) => ddl::plan_alter_system_reset_all(scx, stmt),
        Statement::AlterTableAddColumn(stmt) => ddl::plan_alter_table_add_column(scx, stmt),
        Statement::AlterNetworkPolicy(stmt) => ddl::plan_alter_network_policy(scx, stmt),
        Statement::Comment(stmt) => ddl::plan_comment(scx, stmt),
        Statement::CreateCluster(stmt) => ddl::plan_create_cluster(scx, stmt),
        Statement::CreateClusterReplica(stmt) => ddl::plan_create_cluster_replica(scx, stmt),
        Statement::CreateConnection(stmt) => ddl::plan_create_connection(scx, stmt),
        Statement::CreateDatabase(stmt) => ddl::plan_create_database(scx, stmt),
        Statement::CreateIndex(stmt) => ddl::plan_create_index(scx, stmt),
        Statement::CreateRole(stmt) => ddl::plan_create_role(scx, stmt),
        Statement::CreateSchema(stmt) => ddl::plan_create_schema(scx, stmt),
        Statement::CreateSecret(stmt) => ddl::plan_create_secret(scx, stmt),
        Statement::CreateSink(stmt) => ddl::plan_create_sink(scx, stmt),
        Statement::CreateWebhookSource(stmt) => ddl::plan_create_webhook_source(scx, stmt),
        Statement::CreateSource(stmt) => ddl::plan_create_source(scx, stmt),
        Statement::CreateSubsource(stmt) => ddl::plan_create_subsource(scx, stmt),
        Statement::CreateTable(stmt) => ddl::plan_create_table(scx, stmt),
        Statement::CreateTableFromSource(stmt) => ddl::plan_create_table_from_source(scx, stmt),
        Statement::CreateType(stmt) => ddl::plan_create_type(scx, stmt),
        Statement::CreateView(stmt) => ddl::plan_create_view(scx, stmt),
        Statement::CreateMaterializedView(stmt) => ddl::plan_create_materialized_view(scx, stmt),
        Statement::CreateContinualTask(stmt) => ddl::plan_create_continual_task(scx, stmt),
        Statement::CreateNetworkPolicy(stmt) => ddl::plan_create_network_policy(scx, stmt),
        Statement::DropObjects(stmt) => ddl::plan_drop_objects(scx, stmt),
        Statement::DropOwned(stmt) => ddl::plan_drop_owned(scx, stmt),

        // `ACL` statements.
        Statement::AlterOwner(stmt) => acl::plan_alter_owner(scx, stmt),
        Statement::GrantRole(stmt) => acl::plan_grant_role(scx, stmt),
        Statement::RevokeRole(stmt) => acl::plan_revoke_role(scx, stmt),
        Statement::GrantPrivileges(stmt) => acl::plan_grant_privileges(scx, stmt),
        Statement::RevokePrivileges(stmt) => acl::plan_revoke_privileges(scx, stmt),
        Statement::AlterDefaultPrivileges(stmt) => acl::plan_alter_default_privileges(scx, stmt),
        Statement::ReassignOwned(stmt) => acl::plan_reassign_owned(scx, stmt),

        // DML statements.
        Statement::Copy(stmt) => dml::plan_copy(scx, stmt),
        Statement::Delete(stmt) => dml::plan_delete(scx, stmt, params),
        Statement::ExplainPlan(stmt) => dml::plan_explain_plan(scx, stmt, params),
        Statement::ExplainPushdown(stmt) => dml::plan_explain_pushdown(scx, stmt, params),
        Statement::ExplainAnalyze(stmt) => dml::plan_explain_analyze(scx, stmt, params),
        Statement::ExplainTimestamp(stmt) => dml::plan_explain_timestamp(scx, stmt),
        Statement::ExplainSinkSchema(stmt) => dml::plan_explain_schema(scx, stmt),
        Statement::Insert(stmt) => dml::plan_insert(scx, stmt, params),
        Statement::Select(stmt) => dml::plan_select(scx, stmt, params, None),
        Statement::Subscribe(stmt) => dml::plan_subscribe(scx, stmt, params, None),
        Statement::Update(stmt) => dml::plan_update(scx, stmt, params),

        // `SHOW` statements.
        Statement::Show(ShowStatement::ShowColumns(stmt)) => show::show_columns(scx, stmt)?.plan(),
        Statement::Show(ShowStatement::ShowCreateConnection(stmt)) => {
            show::plan_show_create_connection(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateCluster(stmt)) => {
            show::plan_show_create_cluster(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateIndex(stmt)) => {
            show::plan_show_create_index(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateSink(stmt)) => {
            show::plan_show_create_sink(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateSource(stmt)) => {
            show::plan_show_create_source(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateTable(stmt)) => {
            show::plan_show_create_table(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateView(stmt)) => {
            show::plan_show_create_view(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowCreateMaterializedView(stmt)) => {
            show::plan_show_create_materialized_view(scx, stmt).map(Plan::ShowCreate)
        }
        Statement::Show(ShowStatement::ShowObjects(stmt)) => show::show_objects(scx, stmt)?.plan(),

        // SCL statements.
        Statement::Close(stmt) => scl::plan_close(scx, stmt),
        Statement::Deallocate(stmt) => scl::plan_deallocate(scx, stmt),
        Statement::Declare(stmt) => scl::plan_declare(scx, stmt, params),
        Statement::Discard(stmt) => scl::plan_discard(scx, stmt),
        Statement::Execute(stmt) => scl::plan_execute(scx, stmt),
        Statement::Fetch(stmt) => scl::plan_fetch(scx, stmt),
        Statement::Prepare(stmt) => scl::plan_prepare(scx, stmt),
        Statement::ResetVariable(stmt) => scl::plan_reset_variable(scx, stmt),
        Statement::SetVariable(stmt) => scl::plan_set_variable(scx, stmt),
        Statement::Show(ShowStatement::ShowVariable(stmt)) => scl::plan_show_variable(scx, stmt),

        // TCL statements.
        Statement::Commit(stmt) => tcl::plan_commit(scx, stmt),
        Statement::Rollback(stmt) => tcl::plan_rollback(scx, stmt),
        Statement::SetTransaction(stmt) => tcl::plan_set_transaction(scx, stmt),
        Statement::StartTransaction(stmt) => tcl::plan_start_transaction(scx, stmt),

        // Other statements.
        Statement::Raise(stmt) => raise::plan_raise(scx, stmt),
        Statement::Show(ShowStatement::InspectShard(stmt)) => scl::plan_inspect_shard(scx, stmt),
        Statement::ValidateConnection(stmt) => validate::plan_validate_connection(scx, stmt),
    };

    if let Ok(plan) = &plan {
        mz_ore::soft_assert_no_log!(
            permitted_plans.contains(&PlanKind::from(plan)),
            "plan {:?}, permitted plans {:?}",
            plan,
            permitted_plans
        );
    }

    plan
}

pub fn plan_copy_from(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    id: CatalogItemId,
    columns: Vec<ColumnIndex>,
    rows: Vec<mz_repr::Row>,
) -> Result<super::HirRelationExpr, PlanError> {
    query::plan_copy_from_rows(pcx, catalog, id, columns, rows)
}

/// Whether a SQL object type can be interpreted as matching the type of the given catalog item.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
impl PartialEq<ObjectType> for CatalogItemType {
    fn eq(&self, other: &ObjectType) -> bool {
        match (self, other) {
            (CatalogItemType::Source, ObjectType::Source)
            | (CatalogItemType::Table, ObjectType::Table)
            | (CatalogItemType::Sink, ObjectType::Sink)
            | (CatalogItemType::View, ObjectType::View)
            | (CatalogItemType::MaterializedView, ObjectType::MaterializedView)
            | (CatalogItemType::Index, ObjectType::Index)
            | (CatalogItemType::Type, ObjectType::Type)
            | (CatalogItemType::Secret, ObjectType::Secret)
            | (CatalogItemType::Connection, ObjectType::Connection) => true,
            (_, _) => false,
        }
    }
}

impl PartialEq<CatalogItemType> for ObjectType {
    fn eq(&self, other: &CatalogItemType) -> bool {
        other == self
    }
}

/// Immutable state that applies to the planning of an entire `Statement`.
#[derive(Debug, Clone)]
pub struct StatementContext<'a> {
    /// The optional PlanContext, which will be present for statements that execute
    /// within the OneShot QueryLifetime and None otherwise (views). This is an
    /// awkward field and should probably be relocated to a place that fits our
    /// execution model more closely.
    pcx: Option<&'a PlanContext>,
    pub catalog: &'a dyn SessionCatalog,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    pub param_types: RefCell<BTreeMap<usize, ScalarType>>,
    /// Whether the statement contains an expression that can make the exact column list
    /// ambiguous. For example `NATURAL JOIN` or `SELECT *`. This is filled in as planning occurs.
    pub ambiguous_columns: RefCell<bool>,
}

impl<'a> StatementContext<'a> {
    pub fn new(
        pcx: Option<&'a PlanContext>,
        catalog: &'a dyn SessionCatalog,
    ) -> StatementContext<'a> {
        StatementContext {
            pcx,
            catalog,
            param_types: Default::default(),
            ambiguous_columns: RefCell::new(false),
        }
    }

    /// Returns the schemas in order of search_path that exist in the catalog.
    pub fn current_schemas(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)] {
        self.catalog.search_path()
    }

    /// Returns the first schema from the search_path that exist in the catalog,
    /// or None if there are none.
    pub fn current_schema(&self) -> Option<&(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        self.current_schemas().into_iter().next()
    }

    pub fn pcx(&self) -> Result<&PlanContext, PlanError> {
        self.pcx.ok_or_else(|| sql_err!("no plan context"))
    }

    pub fn allocate_full_name(&self, name: PartialItemName) -> Result<FullItemName, PlanError> {
        let (database, schema): (RawDatabaseSpecifier, String) = match (name.database, name.schema)
        {
            (None, None) => {
                let Some((database, schema)) = self.current_schema() else {
                    return Err(PlanError::InvalidSchemaName);
                };
                let schema = self.get_schema(database, schema);
                let database = match schema.database() {
                    ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
                    ResolvedDatabaseSpecifier::Id(id) => {
                        RawDatabaseSpecifier::Name(self.catalog.get_database(id).name().to_string())
                    }
                };
                (database, schema.name().schema.clone())
            }
            (None, Some(schema)) => {
                if is_system_schema(&schema) {
                    (RawDatabaseSpecifier::Ambient, schema)
                } else {
                    match self.catalog.active_database_name() {
                        Some(name) => (RawDatabaseSpecifier::Name(name.to_string()), schema),
                        None => {
                            sql_bail!(
                                "no database specified for non-system schema and no active database"
                            )
                        }
                    }
                }
            }
            (Some(_database), None) => {
                // This shouldn't be possible. Refactor the datastructure to
                // make it not exist.
                sql_bail!("unreachable: specified the database but no schema")
            }
            (Some(database), Some(schema)) => (RawDatabaseSpecifier::Name(database), schema),
        };
        let item = name.item;
        Ok(FullItemName {
            database,
            schema,
            item,
        })
    }

    pub fn allocate_qualified_name(
        &self,
        name: PartialItemName,
    ) -> Result<QualifiedItemName, PlanError> {
        let full_name = self.allocate_full_name(name)?;
        let database_spec = match full_name.database {
            RawDatabaseSpecifier::Ambient => ResolvedDatabaseSpecifier::Ambient,
            RawDatabaseSpecifier::Name(name) => ResolvedDatabaseSpecifier::Id(
                self.resolve_database(&UnresolvedDatabaseName(Ident::new(name)?))?
                    .id(),
            ),
        };
        let schema_spec = self
            .resolve_schema_in_database(&database_spec, &Ident::new(full_name.schema)?)?
            .id()
            .clone();
        Ok(QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec,
                schema_spec,
            },
            item: full_name.item,
        })
    }

    pub fn allocate_temporary_full_name(&self, name: PartialItemName) -> FullItemName {
        FullItemName {
            database: RawDatabaseSpecifier::Ambient,
            schema: name
                .schema
                .unwrap_or_else(|| mz_repr::namespaces::MZ_TEMP_SCHEMA.to_owned()),
            item: name.item,
        }
    }

    pub fn allocate_temporary_qualified_name(
        &self,
        name: PartialItemName,
    ) -> Result<QualifiedItemName, PlanError> {
        if let Some(name) = name.schema {
            if name
                != self
                    .get_schema(
                        &ResolvedDatabaseSpecifier::Ambient,
                        &SchemaSpecifier::Temporary,
                    )
                    .name()
                    .schema
            {
                return Err(PlanError::InvalidTemporarySchema);
            }
        }

        Ok(QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Temporary,
            },
            item: name.item,
        })
    }

    // Creates a `ResolvedItemName::Item` from a `GlobalId` and an
    // `UnresolvedItemName`.
    pub fn allocate_resolved_item_name(
        &self,
        id: CatalogItemId,
        name: UnresolvedItemName,
    ) -> Result<ResolvedItemName, PlanError> {
        let partial = normalize::unresolved_item_name(name)?;
        let qualified = self.allocate_qualified_name(partial.clone())?;
        let full_name = self.allocate_full_name(partial)?;
        Ok(ResolvedItemName::Item {
            id,
            qualifiers: qualified.qualifiers,
            full_name,
            print_id: true,
            version: RelationVersionSelector::Latest,
        })
    }

    pub fn active_database(&self) -> Option<&DatabaseId> {
        self.catalog.active_database()
    }

    pub fn resolve_optional_schema(
        &self,
        schema_name: &Option<ResolvedSchemaName>,
    ) -> Result<SchemaSpecifier, PlanError> {
        match schema_name {
            Some(ResolvedSchemaName::Schema { schema_spec, .. }) => Ok(schema_spec.clone()),
            None => self.resolve_active_schema().map(|spec| spec.clone()),
            Some(ResolvedSchemaName::Error) => {
                unreachable!("should have been handled by name resolution")
            }
        }
    }

    pub fn resolve_active_schema(&self) -> Result<&SchemaSpecifier, PlanError> {
        match self.current_schema() {
            Some((_db, schema)) => Ok(schema),
            None => Err(PlanError::InvalidSchemaName),
        }
    }

    pub fn get_cluster(&self, id: &ClusterId) -> &dyn CatalogCluster {
        self.catalog.get_cluster(*id)
    }

    pub fn resolve_database(
        &self,
        name: &UnresolvedDatabaseName,
    ) -> Result<&dyn CatalogDatabase, PlanError> {
        let name = normalize::ident_ref(&name.0);
        Ok(self.catalog.resolve_database(name)?)
    }

    pub fn get_database(&self, id: &DatabaseId) -> &dyn CatalogDatabase {
        self.catalog.get_database(id)
    }

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema: &Ident,
    ) -> Result<&dyn CatalogSchema, PlanError> {
        let schema = normalize::ident_ref(schema);
        Ok(self
            .catalog
            .resolve_schema_in_database(database_spec, schema)?)
    }

    pub fn resolve_schema(
        &self,
        name: UnresolvedSchemaName,
    ) -> Result<&dyn CatalogSchema, PlanError> {
        let name = normalize::unresolved_schema_name(name)?;
        Ok(self
            .catalog
            .resolve_schema(name.database.as_deref(), &name.schema)?)
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.catalog.get_schema(database_spec, schema_spec)
    }

    pub fn resolve_item(&self, name: RawItemName) -> Result<&dyn CatalogItem, PlanError> {
        match name {
            RawItemName::Name(name) => {
                let name = normalize::unresolved_item_name(name)?;
                Ok(self.catalog.resolve_item(&name)?)
            }
            RawItemName::Id(id, _, _) => {
                let gid = id.parse()?;
                Ok(self.catalog.get_item(&gid))
            }
        }
    }

    pub fn get_item(&self, id: &CatalogItemId) -> &dyn CatalogItem {
        self.catalog.get_item(id)
    }

    pub fn get_item_by_resolved_name(
        &self,
        name: &ResolvedItemName,
    ) -> Result<Box<dyn CatalogCollectionItem + '_>, PlanError> {
        match name {
            ResolvedItemName::Item { id, version, .. } => {
                Ok(self.get_item(id).at_version(*version))
            }
            ResolvedItemName::Cte { .. } => sql_bail!("non-user item"),
            ResolvedItemName::ContinualTask { .. } => sql_bail!("non-user item"),
            ResolvedItemName::Error => unreachable!("should have been caught in name resolution"),
        }
    }

    pub fn get_column_by_resolved_name(
        &self,
        name: &ColumnName<Aug>,
    ) -> Result<(Box<dyn CatalogCollectionItem + '_>, usize), PlanError> {
        match (&name.relation, &name.column) {
            (
                ResolvedItemName::Item { id, version, .. },
                ResolvedColumnReference::Column { index, .. },
            ) => {
                let item = self.get_item(id).at_version(*version);
                Ok((item, *index))
            }
            _ => unreachable!(
                "get_column_by_resolved_name errors should have been caught in name resolution"
            ),
        }
    }

    pub fn resolve_function(
        &self,
        name: UnresolvedItemName,
    ) -> Result<&dyn CatalogItem, PlanError> {
        let name = normalize::unresolved_item_name(name)?;
        Ok(self.catalog.resolve_function(&name)?)
    }

    pub fn resolve_cluster(&self, name: Option<&Ident>) -> Result<&dyn CatalogCluster, PlanError> {
        let name = name.map(|name| name.as_str());
        Ok(self.catalog.resolve_cluster(name)?)
    }

    pub fn resolve_type(&self, mut ty: mz_pgrepr::Type) -> Result<ResolvedDataType, PlanError> {
        // Ignore precision constraints on date/time types until we support
        // it. This should be safe enough because our types are wide enough
        // to support the maximum possible precision.
        //
        // See: https://github.com/MaterializeInc/database-issues/issues/3179
        match &mut ty {
            mz_pgrepr::Type::Interval { constraints } => *constraints = None,
            mz_pgrepr::Type::Time { precision } => *precision = None,
            mz_pgrepr::Type::TimeTz { precision } => *precision = None,
            mz_pgrepr::Type::Timestamp { precision } => *precision = None,
            mz_pgrepr::Type::TimestampTz { precision } => *precision = None,
            _ => (),
        }
        // NOTE(benesch): this *looks* gross, but it is
        // safe enough. The `fmt::Display`
        // representation on `pgrepr::Type` promises to
        // produce an unqualified type name that does
        // not require quoting.
        let mut ty = if ty.oid() >= FIRST_USER_OID {
            sql_bail!("internal error, unexpected user type: {ty:?} ");
        } else if ty.oid() < FIRST_MATERIALIZE_OID {
            format!("pg_catalog.{}", ty)
        } else {
            // This relies on all non-PG types existing in `mz_catalog`, which is annoying.
            format!("mz_catalog.{}", ty)
        };
        // TODO(benesch): converting `json` to `jsonb`
        // is wrong. We ought to support the `json` type
        // directly.
        if ty == "pg_catalog.json" {
            ty = "pg_catalog.jsonb".into();
        }
        let data_type = mz_sql_parser::parser::parse_data_type(&ty)?;
        let (data_type, _) = names::resolve(self.catalog, data_type)?;
        Ok(data_type)
    }

    pub fn get_object_type(&self, id: &ObjectId) -> ObjectType {
        self.catalog.get_object_type(id)
    }

    pub fn get_system_object_type(&self, id: &SystemObjectId) -> SystemObjectType {
        match id {
            SystemObjectId::Object(id) => SystemObjectType::Object(self.get_object_type(id)),
            SystemObjectId::System => SystemObjectType::System,
        }
    }

    /// Returns an error if the named `FeatureFlag` is not set to `on`.
    pub fn require_feature_flag(&self, flag: &'static FeatureFlag) -> Result<(), PlanError> {
        flag.require(self.catalog.system_vars())?;
        Ok(())
    }

    /// Returns true if the named [`FeatureFlag`] is set to `on`, returns false otherwise.
    pub fn is_feature_flag_enabled(&self, flag: &'static FeatureFlag) -> bool {
        self.require_feature_flag(flag).is_ok()
    }

    pub fn finalize_param_types(self) -> Result<Vec<ScalarType>, PlanError> {
        let param_types = self.param_types.into_inner();
        let mut out = vec![];
        for (i, (n, typ)) in param_types.into_iter().enumerate() {
            if n != i + 1 {
                sql_bail!("unable to infer type for parameter ${}", i + 1);
            }
            out.push(typ);
        }
        Ok(out)
    }

    /// The returned String is more detailed when the `postgres_compat` flag is not set. However,
    /// the flag should be set in, e.g., the implementation of the `pg_typeof` function.
    pub fn humanize_scalar_type(&self, typ: &ScalarType, postgres_compat: bool) -> String {
        self.catalog.humanize_scalar_type(typ, postgres_compat)
    }

    /// The returned String is more detailed when the `postgres_compat` flag is not set. However,
    /// the flag should be set in, e.g., the implementation of the `pg_typeof` function.
    pub fn humanize_column_type(&self, typ: &ColumnType, postgres_compat: bool) -> String {
        self.catalog.humanize_column_type(typ, postgres_compat)
    }

    pub(crate) fn build_tunnel_definition(
        &self,
        ssh_tunnel: Option<with_options::Object>,
        aws_privatelink: Option<ConnectionDefaultAwsPrivatelink<Aug>>,
    ) -> Result<Tunnel<ReferencedConnection>, PlanError> {
        match (ssh_tunnel, aws_privatelink) {
            (None, None) => Ok(Tunnel::Direct),
            (Some(ssh_tunnel), None) => {
                let id = CatalogItemId::from(ssh_tunnel);
                let ssh_tunnel = self.catalog.get_item(&id);
                match ssh_tunnel.connection()? {
                    Connection::Ssh(_connection) => Ok(Tunnel::Ssh(SshTunnel {
                        connection_id: id,
                        connection: id,
                    })),
                    _ => sql_bail!("{} is not an SSH connection", ssh_tunnel.name().item),
                }
            }
            (None, Some(aws_privatelink)) => {
                let id = aws_privatelink.connection.item_id().clone();
                let entry = self.catalog.get_item(&id);
                match entry.connection()? {
                    Connection::AwsPrivatelink(_) => Ok(Tunnel::AwsPrivatelink(AwsPrivatelink {
                        connection_id: id,
                        // By default we do not specify an availability zone for the tunnel.
                        availability_zone: None,
                        // We always use the port as specified by the top-level connection.
                        port: aws_privatelink.port,
                    })),
                    _ => sql_bail!("{} is not an AWS PRIVATELINK connection", entry.name().item),
                }
            }
            (Some(_), Some(_)) => {
                sql_bail!("cannot specify both SSH TUNNEL and AWS PRIVATELINK");
            }
        }
    }

    pub fn relation_desc_into_table_defs(
        &self,
        desc: &RelationDesc,
    ) -> Result<(Vec<ColumnDef<Aug>>, Vec<TableConstraint<Aug>>), PlanError> {
        let mut columns = vec![];
        let mut null_cols = BTreeSet::new();
        for (column_name, column_type) in desc.iter() {
            let name = Ident::new(column_name.as_str().to_owned())?;

            let ty = mz_pgrepr::Type::from(&column_type.scalar_type);
            let data_type = self.resolve_type(ty)?;

            let options = if !column_type.nullable {
                null_cols.insert(columns.len());
                vec![mz_sql_parser::ast::ColumnOptionDef {
                    name: None,
                    option: mz_sql_parser::ast::ColumnOption::NotNull,
                }]
            } else {
                vec![]
            };

            columns.push(ColumnDef {
                name,
                data_type,
                collation: None,
                options,
            });
        }

        let mut table_constraints = vec![];
        for key in desc.typ().keys.iter() {
            let mut col_names = vec![];
            for col_idx in key {
                if !null_cols.contains(col_idx) {
                    // Note that alternatively we could support NULL values in keys with `NULLS NOT
                    // DISTINCT` semantics, which treats `NULL` as a distinct value.
                    sql_bail!(
                        "[internal error] key columns must be NOT NULL when generating table constraints"
                    );
                }
                col_names.push(columns[*col_idx].name.clone());
            }
            table_constraints.push(TableConstraint::Unique {
                name: None,
                columns: col_names,
                is_primary: false,
                nulls_not_distinct: false,
            });
        }

        Ok((columns, table_constraints))
    }

    pub fn get_owner_id(&self, id: &ObjectId) -> Option<RoleId> {
        self.catalog.get_owner_id(id)
    }

    pub fn humanize_resolved_name(
        &self,
        name: &ResolvedItemName,
    ) -> Result<PartialItemName, PlanError> {
        let item = self.get_item_by_resolved_name(name)?;
        Ok(self.catalog.minimal_qualification(item.name()))
    }

    /// WARNING! This style of name resolution assumes the referred-to objects exists (i.e. panics
    /// if objects do not exist) so should never be used to handle user input.
    pub fn dangerous_resolve_name(&self, name: Vec<&str>) -> ResolvedItemName {
        tracing::trace!("dangerous_resolve_name {:?}", name);
        // Note: Using unchecked here is okay because this function is already dangerous.
        let name: Vec<_> = name.into_iter().map(Ident::new_unchecked).collect();
        let name = UnresolvedItemName::qualified(&name);
        let entry = match self.resolve_item(RawItemName::Name(name.clone())) {
            Ok(entry) => entry,
            Err(_) => self
                .resolve_function(name.clone())
                .expect("name referred to an existing object"),
        };

        let partial = normalize::unresolved_item_name(name).unwrap();
        let full_name = self.allocate_full_name(partial).unwrap();

        ResolvedItemName::Item {
            id: entry.id(),
            qualifiers: entry.name().qualifiers.clone(),
            full_name,
            print_id: true,
            version: RelationVersionSelector::Latest,
        }
    }
}

pub fn resolve_cluster_for_materialized_view<'a>(
    catalog: &'a dyn SessionCatalog,
    stmt: &CreateMaterializedViewStatement<Aug>,
) -> Result<ClusterId, PlanError> {
    Ok(match &stmt.in_cluster {
        None => catalog.resolve_cluster(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    })
}

/// Statement classification as documented by [`plan`].
#[derive(Debug, Clone, Copy)]
pub enum StatementClassification {
    ACL,
    DDL,
    DML,
    Other,
    SCL,
    Show,
    TCL,
}

impl StatementClassification {
    pub fn is_ddl(&self) -> bool {
        matches!(self, StatementClassification::DDL)
    }
}

impl<T: mz_sql_parser::ast::AstInfo> From<&Statement<T>> for StatementClassification {
    fn from(value: &Statement<T>) -> Self {
        use StatementClassification::*;

        match value {
            // DDL statements.
            Statement::AlterCluster(_) => DDL,
            Statement::AlterConnection(_) => DDL,
            Statement::AlterIndex(_) => DDL,
            Statement::AlterObjectRename(_) => DDL,
            Statement::AlterObjectSwap(_) => DDL,
            Statement::AlterNetworkPolicy(_) => DDL,
            Statement::AlterRetainHistory(_) => DDL,
            Statement::AlterRole(_) => DDL,
            Statement::AlterSecret(_) => DDL,
            Statement::AlterSetCluster(_) => DDL,
            Statement::AlterSink(_) => DDL,
            Statement::AlterSource(_) => DDL,
            Statement::AlterSystemSet(_) => DDL,
            Statement::AlterSystemReset(_) => DDL,
            Statement::AlterSystemResetAll(_) => DDL,
            Statement::AlterTableAddColumn(_) => DDL,
            Statement::Comment(_) => DDL,
            Statement::CreateCluster(_) => DDL,
            Statement::CreateClusterReplica(_) => DDL,
            Statement::CreateConnection(_) => DDL,
            Statement::CreateContinualTask(_) => DDL,
            Statement::CreateDatabase(_) => DDL,
            Statement::CreateIndex(_) => DDL,
            Statement::CreateRole(_) => DDL,
            Statement::CreateSchema(_) => DDL,
            Statement::CreateSecret(_) => DDL,
            Statement::CreateSink(_) => DDL,
            Statement::CreateWebhookSource(_) => DDL,
            Statement::CreateSource(_) => DDL,
            Statement::CreateSubsource(_) => DDL,
            Statement::CreateTable(_) => DDL,
            Statement::CreateTableFromSource(_) => DDL,
            Statement::CreateType(_) => DDL,
            Statement::CreateView(_) => DDL,
            Statement::CreateMaterializedView(_) => DDL,
            Statement::CreateNetworkPolicy(_) => DDL,
            Statement::DropObjects(_) => DDL,
            Statement::DropOwned(_) => DDL,

            // `ACL` statements.
            Statement::AlterOwner(_) => ACL,
            Statement::GrantRole(_) => ACL,
            Statement::RevokeRole(_) => ACL,
            Statement::GrantPrivileges(_) => ACL,
            Statement::RevokePrivileges(_) => ACL,
            Statement::AlterDefaultPrivileges(_) => ACL,
            Statement::ReassignOwned(_) => ACL,

            // DML statements.
            Statement::Copy(_) => DML,
            Statement::Delete(_) => DML,
            Statement::ExplainPlan(_) => DML,
            Statement::ExplainPushdown(_) => DML,
            Statement::ExplainAnalyze(_) => DML,
            Statement::ExplainTimestamp(_) => DML,
            Statement::ExplainSinkSchema(_) => DML,
            Statement::Insert(_) => DML,
            Statement::Select(_) => DML,
            Statement::Subscribe(_) => DML,
            Statement::Update(_) => DML,

            // `SHOW` statements.
            Statement::Show(ShowStatement::ShowColumns(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateConnection(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateCluster(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateIndex(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateSink(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateSource(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateTable(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateView(_)) => Show,
            Statement::Show(ShowStatement::ShowCreateMaterializedView(_)) => Show,
            Statement::Show(ShowStatement::ShowObjects(_)) => Show,

            // SCL statements.
            Statement::Close(_) => SCL,
            Statement::Deallocate(_) => SCL,
            Statement::Declare(_) => SCL,
            Statement::Discard(_) => SCL,
            Statement::Execute(_) => SCL,
            Statement::Fetch(_) => SCL,
            Statement::Prepare(_) => SCL,
            Statement::ResetVariable(_) => SCL,
            Statement::SetVariable(_) => SCL,
            Statement::Show(ShowStatement::ShowVariable(_)) => SCL,

            // TCL statements.
            Statement::Commit(_) => TCL,
            Statement::Rollback(_) => TCL,
            Statement::SetTransaction(_) => TCL,
            Statement::StartTransaction(_) => TCL,

            // Other statements.
            Statement::Raise(_) => Other,
            Statement::Show(ShowStatement::InspectShard(_)) => Other,
            Statement::ValidateConnection(_) => Other,
        }
    }
}
