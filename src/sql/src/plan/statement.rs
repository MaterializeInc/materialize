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

use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use mz_sql_parser::ast::{RawObjectName, UnresolvedDatabaseName, UnresolvedSchemaName};

use crate::ast::{Ident, ObjectType, Statement, UnresolvedObjectName};
use crate::catalog::{
    CatalogComputeInstance, CatalogDatabase, CatalogItem, CatalogItemType, CatalogSchema,
    SessionCatalog,
};
use crate::names::{
    self, Aug, DatabaseId, FullObjectName, ObjectQualifiers, PartialObjectName,
    QualifiedObjectName, RawDatabaseSpecifier, ResolvedDataType, ResolvedDatabaseSpecifier,
    ResolvedObjectName, ResolvedSchemaName, SchemaSpecifier,
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
    };

    let desc = match stmt {
        // DDL statements.
        Statement::AlterIndex(stmt) => ddl::describe_alter_index_options(&scx, stmt)?,
        Statement::AlterObjectRename(stmt) => ddl::describe_alter_object_rename(&scx, stmt)?,
        Statement::AlterSecret(stmt) => ddl::describe_alter_secret_options(&scx, stmt)?,
        Statement::AlterSystemSet(stmt) => ddl::describe_alter_system_set(&scx, stmt)?,
        Statement::AlterSystemReset(stmt) => ddl::describe_alter_system_reset(&scx, stmt)?,
        Statement::AlterSystemResetAll(stmt) => ddl::describe_alter_system_reset_all(&scx, stmt)?,
        Statement::CreateCluster(stmt) => ddl::describe_create_cluster(&scx, stmt)?,
        Statement::CreateClusterReplica(stmt) => ddl::describe_create_cluster_replica(&scx, stmt)?,
        Statement::CreateConnection(stmt) => ddl::describe_create_connection(&scx, stmt)?,
        Statement::CreateDatabase(stmt) => ddl::describe_create_database(&scx, stmt)?,
        Statement::CreateIndex(stmt) => ddl::describe_create_index(&scx, stmt)?,
        Statement::CreateRole(stmt) => ddl::describe_create_role(&scx, stmt)?,
        Statement::CreateSchema(stmt) => ddl::describe_create_schema(&scx, stmt)?,
        Statement::CreateSecret(stmt) => ddl::describe_create_secret(&scx, stmt)?,
        Statement::CreateSink(stmt) => ddl::describe_create_sink(&scx, stmt)?,
        Statement::CreateSource(stmt) => ddl::describe_create_source(&scx, stmt)?,
        Statement::CreateTable(stmt) => ddl::describe_create_table(&scx, stmt)?,
        Statement::CreateType(stmt) => ddl::describe_create_type(&scx, stmt)?,
        Statement::CreateView(stmt) => ddl::describe_create_view(&scx, stmt)?,
        Statement::CreateViews(stmt) => ddl::describe_create_views(&scx, stmt)?,
        Statement::CreateMaterializedView(stmt) => {
            ddl::describe_create_materialized_view(&scx, stmt)?
        }
        Statement::DropClusterReplicas(stmt) => ddl::describe_drop_cluster_replica(&scx, stmt)?,
        Statement::DropClusters(stmt) => ddl::describe_drop_cluster(&scx, stmt)?,
        Statement::DropDatabase(stmt) => ddl::describe_drop_database(&scx, stmt)?,
        Statement::DropObjects(stmt) => ddl::describe_drop_objects(&scx, stmt)?,
        Statement::DropRoles(stmt) => ddl::describe_drop_role(&scx, stmt)?,
        Statement::DropSchema(stmt) => ddl::describe_drop_schema(&scx, stmt)?,

        // `SHOW` statements.
        Statement::ShowColumns(stmt) => show::show_columns(&scx, stmt)?.describe()?,
        Statement::ShowCreateConnection(stmt) => show::describe_show_create_connection(&scx, stmt)?,
        Statement::ShowCreateIndex(stmt) => show::describe_show_create_index(&scx, stmt)?,
        Statement::ShowCreateSink(stmt) => show::describe_show_create_sink(&scx, stmt)?,
        Statement::ShowCreateSource(stmt) => show::describe_show_create_source(&scx, stmt)?,
        Statement::ShowCreateTable(stmt) => show::describe_show_create_table(&scx, stmt)?,
        Statement::ShowCreateView(stmt) => show::describe_show_create_view(&scx, stmt)?,
        Statement::ShowCreateMaterializedView(stmt) => {
            show::describe_show_create_materialized_view(&scx, stmt)?
        }
        Statement::ShowDatabases(stmt) => show::show_databases(&scx, stmt)?.describe()?,
        Statement::ShowIndexes(stmt) => show::show_indexes(&scx, stmt)?.describe()?,
        Statement::ShowObjects(stmt) => show::show_objects(&scx, stmt)?.describe()?,
        Statement::ShowSchemas(stmt) => show::show_schemas(&scx, stmt)?.describe()?,

        // SCL statements.
        Statement::Close(stmt) => scl::describe_close(&scx, stmt)?,
        Statement::Deallocate(stmt) => scl::describe_deallocate(&scx, stmt)?,
        Statement::Declare(stmt) => scl::describe_declare(&scx, stmt)?,
        Statement::Discard(stmt) => scl::describe_discard(&scx, stmt)?,
        Statement::Execute(stmt) => scl::describe_execute(&scx, stmt)?,
        Statement::Fetch(stmt) => scl::describe_fetch(&scx, stmt)?,
        Statement::Prepare(stmt) => scl::describe_prepare(&scx, stmt)?,
        Statement::ResetVariable(stmt) => scl::describe_reset_variable(&scx, stmt)?,
        Statement::SetVariable(stmt) => scl::describe_set_variable(&scx, stmt)?,
        Statement::ShowVariable(stmt) => scl::describe_show_variable(&scx, stmt)?,

        // DML statements.
        Statement::Copy(stmt) => dml::describe_copy(&scx, stmt)?,
        Statement::Delete(stmt) => dml::describe_delete(&scx, stmt)?,
        Statement::Explain(stmt) => dml::describe_explain(&scx, stmt)?,
        Statement::Insert(stmt) => dml::describe_insert(&scx, stmt)?,
        Statement::Select(stmt) => dml::describe_select(&scx, stmt)?,
        Statement::Tail(stmt) => dml::describe_tail(&scx, stmt)?,
        Statement::Update(stmt) => dml::describe_update(&scx, stmt)?,

        // TCL statements.
        Statement::Commit(stmt) => tcl::describe_commit(&scx, stmt)?,
        Statement::Rollback(stmt) => tcl::describe_rollback(&scx, stmt)?,
        Statement::SetTransaction(stmt) => tcl::describe_set_transaction(&scx, stmt)?,
        Statement::StartTransaction(stmt) => tcl::describe_start_transaction(&scx, stmt)?,

        // Other statements.
        Statement::Raise(stmt) => raise::describe_raise(&scx, stmt)?,
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
    stmt: Statement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
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

    match stmt {
        // DDL statements.
        Statement::AlterIndex(stmt) => ddl::plan_alter_index_options(scx, stmt),
        Statement::AlterObjectRename(stmt) => ddl::plan_alter_object_rename(scx, stmt),
        Statement::AlterSecret(stmt) => ddl::plan_alter_secret(scx, stmt),
        Statement::AlterSystemSet(stmt) => ddl::plan_alter_system_set(scx, stmt),
        Statement::AlterSystemReset(stmt) => ddl::plan_alter_system_reset(scx, stmt),
        Statement::AlterSystemResetAll(stmt) => ddl::plan_alter_system_reset_all(scx, stmt),
        Statement::CreateCluster(stmt) => ddl::plan_create_cluster(scx, stmt),
        Statement::CreateClusterReplica(stmt) => ddl::plan_create_cluster_replica(scx, stmt),
        Statement::CreateConnection(stmt) => ddl::plan_create_connection(scx, stmt),
        Statement::CreateDatabase(stmt) => ddl::plan_create_database(scx, stmt),
        Statement::CreateIndex(stmt) => ddl::plan_create_index(scx, stmt),
        Statement::CreateRole(stmt) => ddl::plan_create_role(scx, stmt),
        Statement::CreateSchema(stmt) => ddl::plan_create_schema(scx, stmt),
        Statement::CreateSecret(stmt) => ddl::plan_create_secret(scx, stmt),
        Statement::CreateSink(stmt) => ddl::plan_create_sink(scx, stmt),
        Statement::CreateSource(stmt) => ddl::plan_create_source(scx, stmt),
        Statement::CreateTable(stmt) => ddl::plan_create_table(scx, stmt),
        Statement::CreateType(stmt) => ddl::plan_create_type(scx, stmt),
        Statement::CreateView(stmt) => ddl::plan_create_view(scx, stmt, params),
        Statement::CreateViews(stmt) => ddl::plan_create_views(scx, stmt),
        Statement::CreateMaterializedView(stmt) => {
            ddl::plan_create_materialized_view(scx, stmt, params)
        }
        Statement::DropClusterReplicas(stmt) => ddl::plan_drop_cluster_replica(scx, stmt),
        Statement::DropClusters(stmt) => ddl::plan_drop_cluster(scx, stmt),
        Statement::DropDatabase(stmt) => ddl::plan_drop_database(scx, stmt),
        Statement::DropObjects(stmt) => ddl::plan_drop_objects(scx, stmt),
        Statement::DropRoles(stmt) => ddl::plan_drop_role(scx, stmt),
        Statement::DropSchema(stmt) => ddl::plan_drop_schema(scx, stmt),

        // DML statements.
        Statement::Copy(stmt) => dml::plan_copy(scx, stmt),
        Statement::Delete(stmt) => dml::plan_delete(scx, stmt, params),
        Statement::Explain(stmt) => dml::plan_explain(scx, stmt, params),
        Statement::Insert(stmt) => dml::plan_insert(scx, stmt, params),
        Statement::Select(stmt) => dml::plan_select(scx, stmt, params, None),
        Statement::Tail(stmt) => dml::plan_tail(scx, stmt, None),
        Statement::Update(stmt) => dml::plan_update(scx, stmt, params),

        // `SHOW` statements.
        Statement::ShowColumns(stmt) => show::show_columns(scx, stmt)?.plan(),
        Statement::ShowCreateConnection(stmt) => show::plan_show_create_connection(scx, stmt),
        Statement::ShowCreateIndex(stmt) => show::plan_show_create_index(scx, stmt),
        Statement::ShowCreateSink(stmt) => show::plan_show_create_sink(scx, stmt),
        Statement::ShowCreateSource(stmt) => show::plan_show_create_source(scx, stmt),
        Statement::ShowCreateTable(stmt) => show::plan_show_create_table(scx, stmt),
        Statement::ShowCreateView(stmt) => show::plan_show_create_view(scx, stmt),
        Statement::ShowCreateMaterializedView(stmt) => {
            show::plan_show_create_materialized_view(scx, stmt)
        }
        Statement::ShowDatabases(stmt) => show::show_databases(scx, stmt)?.plan(),
        Statement::ShowIndexes(stmt) => show::show_indexes(scx, stmt)?.plan(),
        Statement::ShowObjects(stmt) => show::show_objects(scx, stmt)?.plan(),
        Statement::ShowSchemas(stmt) => show::show_schemas(scx, stmt)?.plan(),

        // SCL statements.
        Statement::Close(stmt) => scl::plan_close(scx, stmt),
        Statement::Deallocate(stmt) => scl::plan_deallocate(scx, stmt),
        Statement::Declare(stmt) => scl::plan_declare(scx, stmt),
        Statement::Discard(stmt) => scl::plan_discard(scx, stmt),
        Statement::Execute(stmt) => scl::plan_execute(scx, stmt),
        Statement::Fetch(stmt) => scl::plan_fetch(scx, stmt),
        Statement::Prepare(stmt) => scl::plan_prepare(scx, stmt),
        Statement::ResetVariable(stmt) => scl::plan_reset_variable(scx, stmt),
        Statement::SetVariable(stmt) => scl::plan_set_variable(scx, stmt),
        Statement::ShowVariable(stmt) => scl::plan_show_variable(scx, stmt),

        // TCL statements.
        Statement::Commit(stmt) => tcl::plan_commit(scx, stmt),
        Statement::Rollback(stmt) => tcl::plan_rollback(scx, stmt),
        Statement::SetTransaction(stmt) => tcl::plan_set_transaction(scx, stmt),
        Statement::StartTransaction(stmt) => tcl::plan_start_transaction(scx, stmt),

        // Other statements.
        Statement::Raise(stmt) => raise::plan_raise(scx, stmt),
    }
}

pub fn plan_copy_from(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    id: GlobalId,
    columns: Vec<usize>,
    rows: Vec<mz_repr::Row>,
) -> Result<super::HirRelationExpr, PlanError> {
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

    pub fn pcx(&self) -> Result<&PlanContext, PlanError> {
        self.pcx.ok_or_else(|| sql_err!("no plan context"))
    }

    pub fn allocate_full_name(&self, name: PartialObjectName) -> Result<FullObjectName, PlanError> {
        let schema = name.schema.unwrap_or_else(|| DEFAULT_SCHEMA.into());
        let database = match name.database {
            Some(name) => RawDatabaseSpecifier::Name(name),
            None if self.catalog.is_system_schema(&schema) => RawDatabaseSpecifier::Ambient,
            None => match self.catalog.active_database_name() {
                Some(name) => RawDatabaseSpecifier::Name(name.to_string()),
                None => {
                    sql_bail!("no database specified for non-system schema and no active database")
                }
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
    ) -> Result<QualifiedObjectName, PlanError> {
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
    ) -> Result<&dyn CatalogItem, PlanError> {
        match name {
            ResolvedObjectName::Object { id, .. } => Ok(self.get_item(id)),
            ResolvedObjectName::Cte { .. } => sql_bail!("non-user item"),
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

    pub fn resolve_type(&self, mut ty: mz_pgrepr::Type) -> Result<ResolvedDataType, PlanError> {
        // Ignore precision constraints on date/time types until we support
        // it. This should be safe enough because our types are wide enough
        // to support the maximum possible precision.
        //
        // See: https://github.com/MaterializeInc/materialize/issues/10837
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
        //
        // TODO(benesch): converting `json` to `jsonb`
        // is wrong. We ought to support the `json` type
        // directly.
        let mut ty = format!("pg_catalog.{}", ty);
        if ty == "pg_catalog.json" {
            ty = "pg_catalog.jsonb".into();
        }
        let data_type = mz_sql_parser::parser::parse_data_type(&ty)?;
        let (data_type, _) = names::resolve(self.catalog, data_type)?;
        Ok(data_type)
    }

    pub fn unsafe_mode(&self) -> bool {
        self.catalog.config().unsafe_mode
    }

    pub fn require_unsafe_mode(&self, feature_name: &str) -> Result<(), PlanError> {
        if !self.unsafe_mode() {
            sql_bail!("{} is unsupported", feature_name)
        }
        Ok(())
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

    pub fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        self.catalog.humanize_scalar_type(typ)
    }

    pub fn humanize_column_type(&self, typ: &ColumnType) -> String {
        self.catalog.humanize_column_type(typ)
    }
}
