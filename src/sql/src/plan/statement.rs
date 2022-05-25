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

use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use mz_sql_parser::ast::{RawObjectName, UnresolvedDatabaseName, UnresolvedSchemaName};

use crate::ast::{Ident, ObjectType, Raw, Statement, UnresolvedObjectName};
use crate::catalog::{
    CatalogComputeInstance, CatalogDatabase, CatalogItem, CatalogItemType, CatalogSchema,
    SessionCatalog,
};
use crate::names::{
    resolve_names_stmt, DatabaseId, FullObjectName, ObjectQualifiers, PartialObjectName,
    QualifiedObjectName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier, ResolvedObjectName,
    ResolvedSchemaName, SchemaSpecifier,
};
use crate::plan::error::PlanError;
use crate::plan::query;
use crate::plan::{Params, Plan, PlanContext};
use crate::{normalize, DEFAULT_SCHEMA};

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

    // most statements can be described with a raw statement
    let desc = match &stmt {
        // DDL statements.
        Statement::CreateConnector(stmt) => Some(ddl::describe_create_connector(&scx, stmt)?),
        Statement::CreateDatabase(stmt) => Some(ddl::describe_create_database(&scx, stmt)?),
        Statement::CreateSchema(stmt) => Some(ddl::describe_create_schema(&scx, stmt)?),
        Statement::CreateTable(stmt) => Some(ddl::describe_create_table(&scx, stmt)?),
        Statement::CreateSource(stmt) => Some(ddl::describe_create_source(&scx, stmt)?),
        Statement::CreateView(stmt) => Some(ddl::describe_create_view(&scx, stmt)?),
        Statement::CreateViews(stmt) => Some(ddl::describe_create_views(&scx, stmt)?),
        Statement::CreateSink(stmt) => Some(ddl::describe_create_sink(&scx, stmt)?),
        Statement::CreateIndex(stmt) => Some(ddl::describe_create_index(&scx, stmt)?),
        Statement::CreateType(stmt) => Some(ddl::describe_create_type(&scx, stmt)?),
        Statement::CreateRole(stmt) => Some(ddl::describe_create_role(&scx, stmt)?),
        Statement::CreateCluster(stmt) => Some(ddl::describe_create_cluster(&scx, stmt)?),
        Statement::CreateClusterReplica(stmt) => {
            Some(ddl::describe_create_cluster_replica(&scx, stmt)?)
        }
        Statement::CreateSecret(stmt) => Some(ddl::describe_create_secret(&scx, stmt)?),
        Statement::DropDatabase(stmt) => Some(ddl::describe_drop_database(&scx, stmt)?),
        Statement::DropSchema(stmt) => Some(ddl::describe_drop_schema(&scx, stmt)?),
        Statement::DropObjects(stmt) => Some(ddl::describe_drop_objects(&scx, stmt)?),
        Statement::DropRoles(stmt) => Some(ddl::describe_drop_role(&scx, stmt)?),
        Statement::DropClusters(stmt) => Some(ddl::describe_drop_cluster(&scx, stmt)?),
        Statement::DropClusterReplicas(stmt) => {
            Some(ddl::describe_drop_cluster_replica(&scx, stmt)?)
        }
        Statement::AlterObjectRename(stmt) => Some(ddl::describe_alter_object_rename(&scx, stmt)?),
        Statement::AlterIndex(stmt) => Some(ddl::describe_alter_index_options(&scx, stmt)?),
        Statement::AlterSecret(stmt) => Some(ddl::describe_alter_secret_options(&scx, stmt)?),

        // `SHOW` statements.
        Statement::ShowCreateTable(stmt) => Some(show::describe_show_create_table(&scx, stmt)?),
        Statement::ShowCreateSource(stmt) => Some(show::describe_show_create_source(&scx, stmt)?),
        Statement::ShowCreateView(stmt) => Some(show::describe_show_create_view(&scx, stmt)?),
        Statement::ShowCreateSink(stmt) => Some(show::describe_show_create_sink(&scx, stmt)?),
        Statement::ShowCreateIndex(stmt) => Some(show::describe_show_create_index(&scx, stmt)?),
        Statement::ShowCreateConnector(stmt) => {
            Some(show::describe_show_create_connector(&scx, stmt)?)
        }
        Statement::ShowColumns(_) => None,
        Statement::ShowDatabases(_) => None,
        Statement::ShowSchemas(_) => None,
        Statement::ShowObjects(_) => None,
        Statement::ShowIndexes(_) => None,

        // SCL statements.
        Statement::SetVariable(stmt) => Some(scl::describe_set_variable(&scx, stmt)?),
        Statement::ResetVariable(stmt) => Some(scl::describe_reset_variable(&scx, stmt)?),
        Statement::ShowVariable(stmt) => Some(scl::describe_show_variable(&scx, stmt)?),
        Statement::Discard(stmt) => Some(scl::describe_discard(&scx, stmt)?),
        Statement::Declare(stmt) => Some(scl::describe_declare(&scx, stmt)?),
        Statement::Fetch(stmt) => Some(scl::describe_fetch(&scx, stmt)?),
        Statement::Close(stmt) => Some(scl::describe_close(&scx, stmt)?),
        Statement::Prepare(stmt) => Some(scl::describe_prepare(&scx, stmt)?),
        Statement::Execute(_) => None,
        Statement::Deallocate(stmt) => Some(scl::describe_deallocate(&scx, stmt)?),

        // DML statements.
        Statement::Insert(_) => None,
        Statement::Update(_) => None,
        Statement::Delete(_) => None,
        Statement::Select(_) => None,
        Statement::Explain(_) => None,
        Statement::Tail(_) => None,
        Statement::Copy(_) => None,

        // TCL statements.
        Statement::StartTransaction(stmt) => Some(tcl::describe_start_transaction(&scx, stmt)?),
        Statement::SetTransaction(stmt) => Some(tcl::describe_set_transaction(&scx, stmt)?),
        Statement::Rollback(stmt) => Some(tcl::describe_rollback(&scx, stmt)?),
        Statement::Commit(stmt) => Some(tcl::describe_commit(&scx, stmt)?),

        // RAISE statements.
        Statement::Raise(stmt) => Some(raise::describe_raise(&scx, stmt)?),
    };

    // The following statement types require augmented statements to describe
    let stmt = match &stmt {
        Statement::ShowColumns(_)
        | Statement::ShowDatabases(_)
        | Statement::ShowSchemas(_)
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

    let desc = match stmt {
        // `SHOW` statements.
        Some(Statement::ShowColumns(stmt)) => show::show_columns(&scx, stmt)?.describe()?,
        Some(Statement::ShowDatabases(stmt)) => show::show_databases(&scx, stmt)?.describe()?,
        Some(Statement::ShowSchemas(stmt)) => show::show_schemas(&scx, stmt)?.describe()?,
        Some(Statement::ShowObjects(stmt)) => show::show_objects(&scx, stmt)?.describe()?,
        Some(Statement::ShowIndexes(stmt)) => show::show_indexes(&scx, stmt)?.describe()?,

        // SCL statements.
        Some(Statement::Execute(stmt)) => scl::describe_execute(&scx, stmt)?,

        // DML statements.
        Some(Statement::Insert(stmt)) => dml::describe_insert(&scx, stmt)?,
        Some(Statement::Update(stmt)) => dml::describe_update(&scx, stmt)?,
        Some(Statement::Delete(stmt)) => dml::describe_delete(&scx, stmt)?,
        Some(Statement::Select(stmt)) => dml::describe_select(&scx, stmt)?,
        Some(Statement::Explain(stmt)) => dml::describe_explain(&scx, stmt)?,
        Some(Statement::Tail(stmt)) => dml::describe_tail(&scx, stmt)?,
        Some(Statement::Copy(stmt)) => dml::describe_copy(&scx, stmt)?,

        _ => desc.expect("desc created with raw statement"),
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
    macro_rules! resolve_stmt {
        ($statement_type:path, $scx:expr, $stmt:expr) => {{
            let (stmt, depends_on) = resolve_names_stmt($scx, $stmt)?;
            if let $statement_type(stmt) = stmt {
                (stmt, depends_on)
            } else {
                panic!(
                    "tried to extract the wrong inner statement type for statement {:?}",
                    stmt
                )
            }
        }};
    }

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

    match stmt {
        // DDL statements.
        stmt @ Statement::CreateDatabase(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateDatabase, scx, stmt);
            ddl::plan_create_database(scx, stmt)
        }
        stmt @ Statement::CreateSchema(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateSchema, scx, stmt);
            ddl::plan_create_schema(scx, stmt)
        }
        stmt @ Statement::CreateTable(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::CreateTable, scx, stmt);
            ddl::plan_create_table(scx, stmt, depends_on)
        }
        stmt @ Statement::CreateSource(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateSource, scx, stmt);
            ddl::plan_create_source(scx, stmt)
        }
        stmt @ Statement::CreateView(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::CreateView, scx, stmt);
            ddl::plan_create_view(scx, stmt, params, depends_on)
        }
        stmt @ Statement::CreateViews(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateViews, scx, stmt);
            ddl::plan_create_views(scx, stmt)
        }
        stmt @ Statement::CreateSink(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::CreateSink, scx, stmt);
            ddl::plan_create_sink(scx, stmt, depends_on)
        }
        stmt @ Statement::CreateIndex(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::CreateIndex, scx, stmt);
            ddl::plan_create_index(scx, stmt, depends_on)
        }
        stmt @ Statement::CreateType(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateType, scx, stmt);
            ddl::plan_create_type(scx, stmt)
        }
        stmt @ Statement::CreateRole(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateRole, scx, stmt);
            ddl::plan_create_role(scx, stmt)
        }
        stmt @ Statement::CreateCluster(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateCluster, scx, stmt);
            ddl::plan_create_cluster(scx, stmt)
        }
        stmt @ Statement::CreateClusterReplica(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateClusterReplica, scx, stmt);
            ddl::plan_create_cluster_replica(scx, stmt)
        }
        stmt @ Statement::CreateSecret(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateSecret, scx, stmt);
            ddl::plan_create_secret(scx, stmt)
        }
        stmt @ Statement::CreateConnector(_) => {
            let (stmt, _) = resolve_stmt!(Statement::CreateConnector, scx, stmt);
            ddl::plan_create_connector(scx, stmt)
        }
        Statement::DropDatabase(stmt) => ddl::plan_drop_database(scx, stmt),
        Statement::DropSchema(stmt) => ddl::plan_drop_schema(scx, stmt),
        Statement::DropObjects(stmt) => ddl::plan_drop_objects(scx, stmt),
        stmt @ Statement::DropRoles(_) => {
            let (stmt, _) = resolve_stmt!(Statement::DropRoles, scx, stmt);
            ddl::plan_drop_role(scx, stmt)
        }
        stmt @ Statement::DropClusters(_) => {
            let (stmt, _) = resolve_stmt!(Statement::DropClusters, scx, stmt);
            ddl::plan_drop_cluster(scx, stmt)
        }
        stmt @ Statement::DropClusterReplicas(_) => {
            let (stmt, _) = resolve_stmt!(Statement::DropClusterReplicas, scx, stmt);
            ddl::plan_drop_cluster_replica(scx, stmt)
        }
        stmt @ Statement::AlterIndex(_) => {
            let (stmt, _) = resolve_stmt!(Statement::AlterIndex, scx, stmt);
            ddl::plan_alter_index_options(scx, stmt)
        }
        Statement::AlterObjectRename(stmt) => ddl::plan_alter_object_rename(scx, stmt),

        stmt @ Statement::AlterSecret(_) => {
            let (stmt, _) = resolve_stmt!(Statement::AlterSecret, scx, stmt);
            ddl::plan_alter_secret(scx, stmt)
        }

        // DML statements.
        stmt @ Statement::Insert(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Insert, scx, stmt);
            dml::plan_insert(scx, stmt, params)
        }
        stmt @ Statement::Update(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Update, scx, stmt);
            dml::plan_update(scx, stmt, params)
        }
        stmt @ Statement::Delete(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Delete, scx, stmt);
            dml::plan_delete(scx, stmt, params)
        }
        stmt @ Statement::Select(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Select, scx, stmt);
            dml::plan_select(scx, stmt, params, None)
        }
        stmt @ Statement::Explain(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Explain, scx, stmt);
            dml::plan_explain(scx, stmt, params)
        }
        stmt @ Statement::Tail(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::Tail, scx, stmt);
            dml::plan_tail(scx, stmt, None, depends_on)
        }
        stmt @ Statement::Copy(_) => {
            let (stmt, depends_on) = resolve_stmt!(Statement::Copy, scx, stmt);
            dml::plan_copy(scx, stmt, depends_on)
        }

        // `SHOW` statements.
        stmt @ Statement::ShowCreateTable(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateTable, scx, stmt);
            show::plan_show_create_table(scx, stmt)
        }
        stmt @ Statement::ShowCreateSource(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateSource, scx, stmt);
            show::plan_show_create_source(scx, stmt)
        }
        stmt @ Statement::ShowCreateView(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateView, scx, stmt);
            show::plan_show_create_view(scx, stmt)
        }
        stmt @ Statement::ShowCreateSink(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateSink, scx, stmt);
            show::plan_show_create_sink(scx, stmt)
        }
        stmt @ Statement::ShowCreateIndex(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateIndex, scx, stmt);
            show::plan_show_create_index(scx, stmt)
        }
        stmt @ Statement::ShowCreateConnector(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowCreateConnector, scx, stmt);
            show::plan_show_create_connector(scx, stmt)
        }
        stmt @ Statement::ShowColumns(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowColumns, scx, stmt);
            show::show_columns(scx, stmt)?.plan()
        }
        stmt @ Statement::ShowIndexes(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowIndexes, scx, stmt);
            show::show_indexes(scx, stmt)?.plan()
        }
        stmt @ Statement::ShowDatabases(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowDatabases, scx, stmt);
            show::show_databases(scx, stmt)?.plan()
        }
        stmt @ Statement::ShowSchemas(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowSchemas, scx, stmt);
            show::show_schemas(scx, stmt)?.plan()
        }
        stmt @ Statement::ShowObjects(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowObjects, scx, stmt);
            show::show_objects(scx, stmt)?.plan()
        }

        // SCL statements.
        stmt @ Statement::SetVariable(_) => {
            let (stmt, _) = resolve_stmt!(Statement::SetVariable, scx, stmt);
            scl::plan_set_variable(scx, stmt)
        }
        stmt @ Statement::ResetVariable(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ResetVariable, scx, stmt);
            scl::plan_reset_variable(scx, stmt)
        }
        stmt @ Statement::ShowVariable(_) => {
            let (stmt, _) = resolve_stmt!(Statement::ShowVariable, scx, stmt);
            scl::plan_show_variable(scx, stmt)
        }
        stmt @ Statement::Discard(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Discard, scx, stmt);
            scl::plan_discard(scx, stmt)
        }
        Statement::Declare(stmt) => scl::plan_declare(scx, stmt),
        stmt @ Statement::Fetch(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Fetch, scx, stmt);
            scl::plan_fetch(scx, stmt)
        }
        stmt @ Statement::Close(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Close, scx, stmt);
            scl::plan_close(scx, stmt)
        }
        Statement::Prepare(stmt) => scl::plan_prepare(scx, stmt),
        stmt @ Statement::Execute(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Execute, scx, stmt);
            scl::plan_execute(scx, stmt)
        }
        stmt @ Statement::Deallocate(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Deallocate, scx, stmt);
            scl::plan_deallocate(scx, stmt)
        }

        // TCL statements.
        stmt @ Statement::StartTransaction(_) => {
            let (stmt, _) = resolve_stmt!(Statement::StartTransaction, scx, stmt);
            tcl::plan_start_transaction(scx, stmt)
        }
        stmt @ Statement::SetTransaction(_) => {
            let (stmt, _) = resolve_stmt!(Statement::SetTransaction, scx, stmt);
            tcl::plan_set_transaction(scx, stmt)
        }
        stmt @ Statement::Rollback(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Rollback, scx, stmt);
            tcl::plan_rollback(scx, stmt)
        }
        stmt @ Statement::Commit(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Commit, scx, stmt);
            tcl::plan_commit(scx, stmt)
        }

        // RAISE statements.
        stmt @ Statement::Raise(_) => {
            let (stmt, _) = resolve_stmt!(Statement::Raise, scx, stmt);
            raise::plan_raise(scx, stmt)
        }
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
            | (CatalogItemType::Secret, ObjectType::Secret)
            | (CatalogItemType::Connector, ObjectType::Connector) => true,
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

    pub fn allocate_full_name(
        &self,
        name: PartialObjectName,
    ) -> Result<FullObjectName, anyhow::Error> {
        let schema = name.schema.unwrap_or_else(|| DEFAULT_SCHEMA.into());
        let database = match name.database {
            Some(name) => RawDatabaseSpecifier::Name(name),
            None if self.catalog.is_system_schema(&schema) => RawDatabaseSpecifier::Ambient,
            None => match self.catalog.active_database_name() {
                Some(name) => RawDatabaseSpecifier::Name(name.to_string()),
                None => bail!("no database specified for non-system schema and no active database"),
            },
        };
        let item = name.item;
        Ok(FullObjectName {
            database,
            schema,
            item,
        })
    }

    pub fn allocate_qualified_name(
        &self,
        name: PartialObjectName,
    ) -> Result<QualifiedObjectName, anyhow::Error> {
        let full_name = self.allocate_full_name(name)?;
        let database_spec = match full_name.database {
            RawDatabaseSpecifier::Ambient => ResolvedDatabaseSpecifier::Ambient,
            RawDatabaseSpecifier::Name(name) => ResolvedDatabaseSpecifier::Id(
                self.resolve_database(&UnresolvedDatabaseName(Ident::new(name)))?
                    .id(),
            ),
        };
        let schema_spec = self
            .resolve_schema_in_database(&database_spec, &Ident::new(full_name.schema))?
            .id()
            .clone();
        Ok(QualifiedObjectName {
            qualifiers: ObjectQualifiers {
                database_spec,
                schema_spec,
            },
            item: full_name.item,
        })
    }

    pub fn allocate_temporary_full_name(&self, name: PartialObjectName) -> FullObjectName {
        FullObjectName {
            database: RawDatabaseSpecifier::Ambient,
            schema: name.schema.unwrap_or_else(|| "mz_temp".to_owned()),
            item: name.item,
        }
    }

    pub fn allocate_temporary_qualified_name(
        &self,
        name: PartialObjectName,
    ) -> Result<QualifiedObjectName, PlanError> {
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

        Ok(QualifiedObjectName {
            qualifiers: ObjectQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Temporary,
            },
            item: name.item,
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
        Ok(self.catalog.resolve_schema(None, DEFAULT_SCHEMA)?.id())
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

    pub fn item_exists(&self, name: &QualifiedObjectName) -> bool {
        self.catalog.item_exists(name)
    }

    pub fn resolve_item(&self, name: RawObjectName) -> Result<&dyn CatalogItem, PlanError> {
        match name {
            RawObjectName::Name(name) => {
                let name = normalize::unresolved_object_name(name)?;
                Ok(self.catalog.resolve_item(&name)?)
            }
            RawObjectName::Id(id, _) => {
                let gid = id.parse()?;
                Ok(self.catalog.get_item(&gid))
            }
        }
    }

    pub fn get_item(&self, id: &GlobalId) -> &dyn CatalogItem {
        self.catalog.get_item(id)
    }

    pub fn get_item_by_resolved_name(
        &self,
        name: &ResolvedObjectName,
    ) -> Result<&dyn CatalogItem, anyhow::Error> {
        match name {
            ResolvedObjectName::Object { id, .. } => Ok(self.get_item(id)),
            ResolvedObjectName::Cte { .. } => bail!("non-user item"),
            ResolvedObjectName::Error => unreachable!("should have been caught in name resolution"),
        }
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
