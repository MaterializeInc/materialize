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
use std::collections::BTreeMap;

use anyhow::bail;

use mz_expr::{GlobalId, Id};
use mz_ore::collections::CollectionExt;
use mz_repr::{ColumnType, RelationDesc, ScalarType};

use crate::ast::{Ident, ObjectType, Raw, Statement, UnresolvedObjectName};
use crate::catalog::{
    CatalogComputeInstance, CatalogDatabase, CatalogItem, CatalogItemType, CatalogSchema,
    SessionCatalog,
};
use crate::names::{
    resolve_names_stmt, DatabaseSpecifier, FullName, PartialName, ResolvedObjectName,
};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::query;
use crate::plan::{Params, Plan, PlanContext};

mod ddl;
mod dml;
mod raise;
mod scl;
mod show;
mod tcl;

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
    stmt: Statement<Raw>,
    param_types_in: &[Option<ScalarType>],
) -> Result<StatementDesc, anyhow::Error> {
    let mut param_types = BTreeMap::new();
    for (i, ty) in param_types_in.iter().enumerate() {
        if let Some(ty) = ty {
            param_types.insert(i + 1, ty.clone());
        }
    }

    let mut scx = StatementContext {
        pcx: Some(pcx),
        catalog,
        param_types: RefCell::new(param_types),
    };

    // The following statement types require augmented statements to describe
    let stmt_resolved = match &stmt {
        Statement::ShowColumns(_)
        | Statement::ShowDatabases(_)
        | Statement::ShowObjects(_)
        | Statement::ShowIndexes(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Select(_)
        | Statement::Explain(_)
        | Statement::Tail(_)
        | Statement::Copy(_)
        | Statement::Execute(_) => Some(resolve_names_stmt(&mut scx, stmt.clone())?.0),
        _ => None,
    };

    let desc = match (stmt, stmt_resolved) {
        // DDL statements.
        (Statement::CreateDatabase(stmt), None) => ddl::describe_create_database(&scx, stmt)?,
        (Statement::CreateSchema(stmt), None) => ddl::describe_create_schema(&scx, stmt)?,
        (Statement::CreateTable(stmt), None) => ddl::describe_create_table(&scx, stmt)?,
        (Statement::CreateSource(stmt), None) => ddl::describe_create_source(&scx, stmt)?,
        (Statement::CreateView(stmt), None) => ddl::describe_create_view(&scx, stmt)?,
        (Statement::CreateViews(stmt), None) => ddl::describe_create_views(&scx, stmt)?,
        (Statement::CreateSink(stmt), None) => ddl::describe_create_sink(&scx, stmt)?,
        (Statement::CreateIndex(stmt), None) => ddl::describe_create_index(&scx, stmt)?,
        (Statement::CreateType(stmt), None) => ddl::describe_create_type(&scx, stmt)?,
        (Statement::CreateRole(stmt), None) => ddl::describe_create_role(&scx, stmt)?,
        (Statement::CreateCluster(stmt), None) => ddl::describe_create_cluster(&scx, stmt)?,
        (Statement::CreateSecret(stmt), None) => ddl::describe_create_secret(&scx, stmt)?,
        (Statement::DropDatabase(stmt), None) => ddl::describe_drop_database(&scx, stmt)?,
        (Statement::DropObjects(stmt), None) => ddl::describe_drop_objects(&scx, stmt)?,
        (Statement::AlterObjectRename(stmt), None) => {
            ddl::describe_alter_object_rename(&scx, stmt)?
        }
        (Statement::AlterIndex(stmt), None) => ddl::describe_alter_index_options(&scx, stmt)?,
        (Statement::AlterSecret(stmt), None) => ddl::describe_alter_secret_options(&scx, stmt)?,
        (Statement::AlterCluster(stmt), None) => ddl::describe_alter_cluster(&scx, stmt)?,

        // `SHOW` statements.
        (Statement::ShowCreateTable(stmt), None) => show::describe_show_create_table(&scx, stmt)?,
        (Statement::ShowCreateSource(stmt), None) => show::describe_show_create_source(&scx, stmt)?,
        (Statement::ShowCreateView(stmt), None) => show::describe_show_create_view(&scx, stmt)?,
        (Statement::ShowCreateSink(stmt), None) => show::describe_show_create_sink(&scx, stmt)?,
        (Statement::ShowCreateIndex(stmt), None) => show::describe_show_create_index(&scx, stmt)?,
        (_, Some(Statement::ShowColumns(stmt))) => show::show_columns(&scx, stmt)?.describe()?,
        (_, Some(Statement::ShowDatabases(stmt))) => {
            show::show_databases(&scx, stmt)?.describe()?
        }
        (_, Some(Statement::ShowObjects(stmt))) => show::show_objects(&scx, stmt)?.describe()?,
        (_, Some(Statement::ShowIndexes(stmt))) => show::show_indexes(&scx, stmt)?.describe()?,

        // SCL statements.
        (Statement::SetVariable(stmt), None) => scl::describe_set_variable(&scx, stmt)?,
        (Statement::ShowVariable(stmt), None) => scl::describe_show_variable(&scx, stmt)?,
        (Statement::Discard(stmt), None) => scl::describe_discard(&scx, stmt)?,
        (Statement::Declare(stmt), None) => scl::describe_declare(&scx, stmt)?,
        (Statement::Fetch(stmt), None) => scl::describe_fetch(&scx, stmt)?,
        (Statement::Close(stmt), None) => scl::describe_close(&scx, stmt)?,
        (Statement::Prepare(stmt), None) => scl::describe_prepare(&scx, stmt)?,
        (_, Some(Statement::Execute(stmt))) => scl::describe_execute(&scx, stmt)?,
        (Statement::Deallocate(stmt), None) => scl::describe_deallocate(&scx, stmt)?,

        // DML statements.
        (_, Some(Statement::Insert(stmt))) => dml::describe_insert(&scx, stmt)?,
        (_, Some(Statement::Update(stmt))) => dml::describe_update(&scx, stmt)?,
        (_, Some(Statement::Delete(stmt))) => dml::describe_delete(&scx, stmt)?,
        (_, Some(Statement::Select(stmt))) => dml::describe_select(&scx, stmt)?,
        (_, Some(Statement::Explain(stmt))) => dml::describe_explain(&scx, stmt)?,
        (_, Some(Statement::Tail(stmt))) => dml::describe_tail(&scx, stmt)?,
        (_, Some(Statement::Copy(stmt))) => dml::describe_copy(&scx, stmt)?,

        // TCL statements.
        (Statement::StartTransaction(stmt), None) => tcl::describe_start_transaction(&scx, stmt)?,
        (Statement::SetTransaction(stmt), None) => tcl::describe_set_transaction(&scx, stmt)?,
        (Statement::Rollback(stmt), None) => tcl::describe_rollback(&scx, stmt)?,
        (Statement::Commit(stmt), None) => tcl::describe_commit(&scx, stmt)?,

        // RAISE statements.
        (Statement::Raise(stmt), None) => raise::describe_raise(&scx, stmt)?,

        (_, _) => unreachable!(),
    };

    let desc = desc.with_params(scx.finalize_param_types()?);
    Ok(desc)
}

/// Produces a [`Plan`] from the purified statement `stmt`.
///
/// Planning is a pure, synchronous function and so requires that the provided
/// `stmt` does does not depend on any external state. Only `CREATE SOURCE`
/// statements can depend on external state; remove that state prior to calling
/// this function via [`crate::pure::purify_create_source`].
///
/// The returned plan is tied to the state of the provided catalog. If the state
/// of the catalog changes after planning, the validity of the plan is not
/// guaranteed.
pub fn plan(
    pcx: Option<&PlanContext>,
    catalog: &dyn SessionCatalog,
    stmt: Statement<Raw>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let param_types = params
        .types
        .iter()
        .enumerate()
        .map(|(i, ty)| (i + 1, ty.clone()))
        .collect();

    let scx = &mut StatementContext {
        pcx,
        catalog,
        param_types: RefCell::new(param_types),
    };

    // Delay name resolution of DECLARE and PREPARE until they're executed
    if let Statement::Declare(stmt) = stmt {
        return scl::plan_declare(scx, stmt);
    } else if let Statement::Prepare(stmt) = stmt {
        return scl::plan_prepare(scx, stmt);
    }

    let (stmt, depends_on) = resolve_names_stmt(scx, stmt)?;

    match stmt {
        // DDL statements.
        Statement::CreateDatabase(stmt) => ddl::plan_create_database(scx, stmt),
        Statement::CreateSchema(stmt) => ddl::plan_create_schema(scx, stmt),
        Statement::CreateTable(stmt) => ddl::plan_create_table(scx, stmt, depends_on),
        Statement::CreateSource(stmt) => ddl::plan_create_source(scx, stmt),
        Statement::CreateView(stmt) => ddl::plan_create_view(scx, stmt, params, depends_on),
        Statement::CreateViews(stmt) => ddl::plan_create_views(scx, stmt),
        Statement::CreateSink(stmt) => ddl::plan_create_sink(scx, stmt, depends_on),
        Statement::CreateIndex(stmt) => ddl::plan_create_index(scx, stmt, depends_on),
        Statement::CreateType(stmt) => ddl::plan_create_type(scx, stmt),
        Statement::CreateRole(stmt) => ddl::plan_create_role(scx, stmt),
        Statement::CreateCluster(stmt) => ddl::plan_create_cluster(scx, stmt),
        Statement::CreateSecret(stmt) => ddl::plan_create_secret(scx, stmt),
        Statement::DropDatabase(stmt) => ddl::plan_drop_database(scx, stmt),
        Statement::DropObjects(stmt) => ddl::plan_drop_objects(scx, stmt),
        Statement::AlterIndex(stmt) => ddl::plan_alter_index_options(scx, stmt),
        Statement::AlterObjectRename(stmt) => ddl::plan_alter_object_rename(scx, stmt),
        Statement::AlterSecret(stmt) => ddl::plan_alter_secret(scx, stmt),
        Statement::AlterCluster(stmt) => ddl::plan_alter_cluster(scx, stmt),

        // DML statements.
        Statement::Insert(stmt) => dml::plan_insert(scx, stmt, params),
        Statement::Update(stmt) => dml::plan_update(scx, stmt, params),
        Statement::Delete(stmt) => dml::plan_delete(scx, stmt, params),
        Statement::Select(stmt) => dml::plan_select(scx, stmt, params, None),
        Statement::Explain(stmt) => dml::plan_explain(scx, stmt, params),
        Statement::Tail(stmt) => dml::plan_tail(scx, stmt, None, depends_on),
        Statement::Copy(stmt) => dml::plan_copy(scx, stmt, depends_on),

        // `SHOW` statements.
        Statement::ShowCreateTable(stmt) => show::plan_show_create_table(scx, stmt),
        Statement::ShowCreateSource(stmt) => show::plan_show_create_source(scx, stmt),
        Statement::ShowCreateView(stmt) => show::plan_show_create_view(scx, stmt),
        Statement::ShowCreateSink(stmt) => show::plan_show_create_sink(scx, stmt),
        Statement::ShowCreateIndex(stmt) => show::plan_show_create_index(scx, stmt),
        Statement::ShowColumns(stmt) => show::show_columns(scx, stmt)?.plan(),
        Statement::ShowIndexes(stmt) => show::show_indexes(scx, stmt)?.plan(),
        Statement::ShowDatabases(stmt) => show::show_databases(scx, stmt)?.plan(),
        Statement::ShowObjects(stmt) => show::show_objects(scx, stmt)?.plan(),

        // SCL statements.
        Statement::SetVariable(stmt) => scl::plan_set_variable(scx, stmt),
        Statement::ShowVariable(stmt) => scl::plan_show_variable(scx, stmt),
        Statement::Discard(stmt) => scl::plan_discard(scx, stmt),
        Statement::Declare(_) => unreachable!("planned with raw statement"),
        Statement::Fetch(stmt) => scl::plan_fetch(scx, stmt),
        Statement::Close(stmt) => scl::plan_close(scx, stmt),
        Statement::Prepare(_) => unreachable!("planned with raw statement"),
        Statement::Execute(stmt) => scl::plan_execute(scx, stmt),
        Statement::Deallocate(stmt) => scl::plan_deallocate(scx, stmt),

        // TCL statements.
        Statement::StartTransaction(stmt) => tcl::plan_start_transaction(scx, stmt),
        Statement::SetTransaction(stmt) => tcl::plan_set_transaction(scx, stmt),
        Statement::Rollback(stmt) => tcl::plan_rollback(scx, stmt),
        Statement::Commit(stmt) => tcl::plan_commit(scx, stmt),

        // RAISE statements.
        Statement::Raise(stmt) => raise::plan_raise(scx, stmt),
    }
}

pub fn plan_copy_from(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    id: GlobalId,
    columns: Vec<usize>,
    rows: Vec<mz_repr::Row>,
) -> Result<super::HirRelationExpr, anyhow::Error> {
    Ok(query::plan_copy_from_rows(pcx, catalog, id, columns, rows)?)
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
            | (CatalogItemType::Index, ObjectType::Index)
            | (CatalogItemType::Type, ObjectType::Type)
            | (CatalogItemType::Secret, ObjectType::Secret) => true,
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
        }
    }

    pub fn pcx(&self) -> Result<&PlanContext, anyhow::Error> {
        self.pcx.ok_or_else(|| anyhow::anyhow!("no plan context"))
    }

    pub fn allocate_name(&self, name: PartialName) -> FullName {
        FullName {
            database: match name.database {
                Some(name) => DatabaseSpecifier::Name(name),
                None => DatabaseSpecifier::Name(self.catalog.active_database().into()),
            },
            schema: name.schema.unwrap_or_else(|| "public".into()),
            item: name.item,
        }
    }

    pub fn allocate_temporary_name(&self, name: PartialName) -> FullName {
        FullName {
            database: DatabaseSpecifier::Ambient,
            schema: name.schema.unwrap_or_else(|| "mz_temp".to_owned()),
            item: name.item,
        }
    }

    pub fn resolve_active_database(&self) -> Result<&dyn CatalogDatabase, PlanError> {
        let name = self.catalog.active_database();
        Ok(self.catalog.resolve_database(name)?)
    }

    pub fn resolve_active_schema(&self) -> Result<&dyn CatalogSchema, PlanError> {
        self.resolve_schema(UnresolvedObjectName::unqualified("public"))
    }

    pub fn resolve_database(
        &self,
        name: UnresolvedObjectName,
    ) -> Result<&dyn CatalogDatabase, PlanError> {
        if name.0.len() != 1 {
            return Err(PlanError::OverqualifiedDatabaseName(name.to_string()));
        }
        self.resolve_database_ident(name.0.into_element())
    }

    pub fn resolve_database_ident(&self, name: Ident) -> Result<&dyn CatalogDatabase, PlanError> {
        let name = normalize::ident(name);
        Ok(self.catalog.resolve_database(&name)?)
    }

    pub fn resolve_schema(
        &self,
        mut name: UnresolvedObjectName,
    ) -> Result<&dyn CatalogSchema, PlanError> {
        if name.0.len() > 2 {
            return Err(PlanError::OverqualifiedSchemaName(name.to_string()));
        }
        let schema_name = normalize::ident(name.0.pop().unwrap());
        let database_spec = name.0.pop().map(normalize::ident);
        Ok(self.catalog.resolve_schema(database_spec, &schema_name)?)
    }

    pub fn item_exists(&self, name: PartialName) -> bool {
        self.catalog.resolve_item(&name).is_ok()
    }

    pub fn get_item_by_name(
        &self,
        name: &ResolvedObjectName,
    ) -> Result<&dyn CatalogItem, PlanError> {
        let id = match &name.id {
            Id::Global(id) => id,
            _ => sql_bail!("non-user item"),
        };
        Ok(self.get_item_by_id(id))
    }

    pub fn get_item_by_id(&self, id: &GlobalId) -> &dyn CatalogItem {
        self.catalog.get_item_by_id(id)
    }

    pub fn resolve_function(
        &self,
        name: UnresolvedObjectName,
    ) -> Result<&dyn CatalogItem, PlanError> {
        let name = normalize::unresolved_object_name(name)?;
        Ok(self.catalog.resolve_function(&name)?)
    }

    pub fn resolve_compute_instance(
        &self,
        name: Option<&Ident>,
    ) -> Result<&dyn CatalogComputeInstance, PlanError> {
        let name = name.map(|name| name.as_str());
        Ok(self.catalog.resolve_compute_instance(name)?)
    }

    pub fn experimental_mode(&self) -> bool {
        self.catalog.config().experimental_mode
    }

    pub fn require_experimental_mode(&self, feature_name: &str) -> Result<(), anyhow::Error> {
        if !self.experimental_mode() {
            bail!(
                "{} requires experimental mode; see \
                https://materialize.com/docs/cli/#experimental-mode",
                feature_name
            )
        }
        Ok(())
    }

    pub fn finalize_param_types(self) -> Result<Vec<ScalarType>, anyhow::Error> {
        let param_types = self.param_types.into_inner();
        let mut out = vec![];
        for (i, (n, typ)) in param_types.into_iter().enumerate() {
            if n != i + 1 {
                bail!("unable to infer type for parameter ${}", i + 1);
            }
            out.push(typ);
        }
        Ok(out)
    }

    pub fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        self.catalog.humanize_scalar_type(typ)
    }

    pub fn humanize_column_type(&self, typ: &ColumnType) -> String {
        self.catalog.humanize_column_type(typ)
    }
}
