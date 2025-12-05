// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Queries that show the state of the database system.
//!
//! This module houses the handlers for the `SHOW` suite of statements, like
//! `SHOW CREATE TABLE` and `SHOW VIEWS`. Note that `SHOW <var>` is considered
//! an SCL statement.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use mz_ore::assert_none;
use mz_ore::collections::CollectionExt;
use mz_repr::{CatalogItemId, Datum, RelationDesc, Row, SqlScalarType};
use mz_sql_parser::ast::display::{AstDisplay, FormatMode};
use mz_sql_parser::ast::{
    CreateSubsourceOptionName, ExternalReferenceExport, ExternalReferences, ObjectType,
    ShowCreateClusterStatement, ShowCreateConnectionStatement, ShowCreateMaterializedViewStatement,
    ShowCreateTypeStatement, ShowObjectType, SqlServerConfigOptionName, SystemObjectType,
    UnresolvedItemName, WithOptionValue,
};
use mz_sql_pretty::PrettyConfig;
use query::QueryContext;

use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    SelectStatement, ShowColumnsStatement, ShowCreateIndexStatement, ShowCreateSinkStatement,
    ShowCreateSourceStatement, ShowCreateTableStatement, ShowCreateViewStatement,
    ShowObjectsStatement, ShowStatementFilter, Statement, Value,
};
use crate::catalog::{CatalogItemType, SessionCatalog};
use crate::names::{
    self, Aug, NameSimplifier, ObjectId, ResolvedClusterName, ResolvedDataType,
    ResolvedDatabaseName, ResolvedIds, ResolvedItemName, ResolvedRoleName, ResolvedSchemaName,
};
use crate::parse;
use crate::plan::scope::Scope;
use crate::plan::statement::ddl::unplan_create_cluster;
use crate::plan::statement::{StatementContext, StatementDesc, dml};
use crate::plan::{
    HirRelationExpr, Params, Plan, PlanError, ShowColumnsPlan, ShowCreatePlan, query, transform_ast,
};
use crate::session::vars::ENABLE_REPLACEMENT_MATERIALIZED_VIEWS;

pub fn describe_show_create_view(
    _: &StatementContext,
    _: ShowCreateViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_view(
    scx: &StatementContext,
    ShowCreateViewStatement {
        view_name,
        redacted,
    }: ShowCreateViewStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &view_name, CatalogItemType::View, redacted)
}

pub fn describe_show_create_materialized_view(
    _: &StatementContext,
    _: ShowCreateMaterializedViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_materialized_view(
    scx: &StatementContext,
    ShowCreateMaterializedViewStatement {
        materialized_view_name,
        redacted,
    }: ShowCreateMaterializedViewStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(
        scx,
        &materialized_view_name,
        CatalogItemType::MaterializedView,
        redacted,
    )
}

pub fn describe_show_create_table(
    _: &StatementContext,
    _: ShowCreateTableStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

fn plan_show_create_item(
    scx: &StatementContext,
    name: &ResolvedItemName,
    expect_type: CatalogItemType,
    redacted: bool,
) -> Result<ShowCreatePlan, PlanError> {
    let item = scx.get_item_by_resolved_name(name)?;
    let name = name.full_name_str();
    if item.id().is_system()
        && matches!(
            expect_type,
            CatalogItemType::Table | CatalogItemType::Source
        )
    {
        sql_bail!("cannot show create for system object {name}");
    }
    if item.item_type() == CatalogItemType::MaterializedView && expect_type == CatalogItemType::View
    {
        return Err(PlanError::ShowCreateViewOnMaterializedView(name));
    }
    if item.item_type() != expect_type {
        sql_bail!("{name} is not a {expect_type}");
    }
    let create_sql =
        humanize_sql_for_show_create(scx.catalog, item.id(), item.create_sql(), redacted)?;
    Ok(ShowCreatePlan {
        id: ObjectId::Item(item.id()),
        row: Row::pack_slice(&[Datum::String(&name), Datum::String(&create_sql)]),
    })
}

pub fn plan_show_create_table(
    scx: &StatementContext,
    ShowCreateTableStatement {
        table_name,
        redacted,
    }: ShowCreateTableStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &table_name, CatalogItemType::Table, redacted)
}

pub fn describe_show_create_source(
    _: &StatementContext,
    _: ShowCreateSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_source(
    scx: &StatementContext,
    ShowCreateSourceStatement {
        source_name,
        redacted,
    }: ShowCreateSourceStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &source_name, CatalogItemType::Source, redacted)
}

pub fn describe_show_create_sink(
    _: &StatementContext,
    _: ShowCreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_sink(
    scx: &StatementContext,
    ShowCreateSinkStatement {
        sink_name,
        redacted,
    }: ShowCreateSinkStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &sink_name, CatalogItemType::Sink, redacted)
}

pub fn describe_show_create_index(
    _: &StatementContext,
    _: ShowCreateIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_index(
    scx: &StatementContext,
    ShowCreateIndexStatement {
        index_name,
        redacted,
    }: ShowCreateIndexStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &index_name, CatalogItemType::Index, redacted)
}

pub fn describe_show_create_connection(
    _: &StatementContext,
    _: ShowCreateConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_cluster(
    scx: &StatementContext,
    ShowCreateClusterStatement { cluster_name }: ShowCreateClusterStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    let cluster = scx.get_cluster(&cluster_name.id);
    let name = cluster.name().to_string();
    let plan = cluster.try_to_plan()?;
    let stmt = unplan_create_cluster(scx, plan)?;
    let create_sql = stmt.to_ast_string_stable();
    Ok(ShowCreatePlan {
        id: ObjectId::Cluster(cluster_name.id),
        row: Row::pack_slice(&[Datum::String(&name), Datum::String(&create_sql)]),
    })
}

pub fn describe_show_create_cluster(
    _: &StatementContext,
    _: ShowCreateClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_type(
    scx: &StatementContext,
    ShowCreateTypeStatement {
        type_name,
        redacted,
    }: ShowCreateTypeStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    let ResolvedDataType::Named { id, full_name, .. } = type_name else {
        sql_bail!("{type_name} is not a named type");
    };

    let type_item = scx.get_item(&id);

    // check if a builtin type is being accessed
    if id.is_system() {
        // builtin types do not have a create sql
        sql_bail!("cannot show create for system type {full_name}");
    }

    let name = type_item.name().item.as_ref();

    // if custom type we make the sql human readable
    let create_sql = humanize_sql_for_show_create(
        scx.catalog,
        type_item.id(),
        type_item.create_sql(),
        redacted,
    )?;

    Ok(ShowCreatePlan {
        id: ObjectId::Item(id),
        row: Row::pack_slice(&[Datum::String(name), Datum::String(&create_sql)]),
    })
}

pub fn describe_show_create_type(
    _: &StatementContext,
    _: ShowCreateTypeStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .finish(),
    )))
}

pub fn plan_show_create_connection(
    scx: &StatementContext,
    ShowCreateConnectionStatement {
        connection_name,
        redacted,
    }: ShowCreateConnectionStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create_item(scx, &connection_name, CatalogItemType::Connection, redacted)
}

pub fn show_databases<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name, comment FROM mz_internal.mz_show_databases".to_string();
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

pub fn show_schemas<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedDatabaseName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let database_id = match from {
        Some(ResolvedDatabaseName::Database { id, .. }) => id.to_string(),
        None => match scx.active_database() {
            Some(id) => id.to_string(),
            None => sql_bail!("no database specified and no active database"),
        },
        Some(ResolvedDatabaseName::Error) => {
            unreachable!("should have been handled in name resolution")
        }
    };
    let query = format!(
        "SELECT name, comment
        FROM mz_internal.mz_show_schemas
        WHERE database_id IS NULL OR database_id = '{database_id}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

pub fn show_roles<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name, comment FROM mz_internal.mz_show_roles".to_string();
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

pub fn show_network_policies<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name, rules, comment FROM mz_internal.mz_show_network_policies".to_string();
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "rules", "comment"]),
    )
}

pub fn show_objects<'a>(
    scx: &'a StatementContext<'a>,
    ShowObjectsStatement {
        object_type,
        from,
        filter,
    }: ShowObjectsStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    match object_type {
        ShowObjectType::Table { on_source } => show_tables(scx, from, on_source, filter),
        ShowObjectType::Source { in_cluster } => show_sources(scx, from, in_cluster, filter),
        ShowObjectType::Subsource { on_source } => show_subsources(scx, from, on_source, filter),
        ShowObjectType::View => show_views(scx, from, filter),
        ShowObjectType::Sink { in_cluster } => show_sinks(scx, from, in_cluster, filter),
        ShowObjectType::Type => show_types(scx, from, filter),
        ShowObjectType::Object => show_all_objects(scx, from, filter),
        ShowObjectType::Role => {
            assert_none!(from, "parser should reject from");
            show_roles(scx, filter)
        }
        ShowObjectType::Cluster => {
            assert_none!(from, "parser should reject from");
            show_clusters(scx, filter)
        }
        ShowObjectType::ClusterReplica => {
            assert_none!(from, "parser should reject from");
            show_cluster_replicas(scx, filter)
        }
        ShowObjectType::Secret => show_secrets(scx, from, filter),
        ShowObjectType::Connection => show_connections(scx, from, filter),
        ShowObjectType::MaterializedView { in_cluster } => {
            show_materialized_views(scx, from, in_cluster, filter)
        }
        ShowObjectType::Index {
            in_cluster,
            on_object,
        } => show_indexes(scx, from, on_object, in_cluster, filter),
        ShowObjectType::Database => {
            assert_none!(from, "parser should reject from");
            show_databases(scx, filter)
        }
        ShowObjectType::Schema { from: db_from } => {
            assert_none!(from, "parser should reject from");
            show_schemas(scx, db_from, filter)
        }
        ShowObjectType::Privileges { object_type, role } => {
            assert_none!(from, "parser should reject from");
            show_privileges(scx, object_type, role, filter)
        }
        ShowObjectType::DefaultPrivileges { object_type, role } => {
            assert_none!(from, "parser should reject from");
            show_default_privileges(scx, object_type, role, filter)
        }
        ShowObjectType::RoleMembership { role } => {
            assert_none!(from, "parser should reject from");
            show_role_membership(scx, role, filter)
        }
        ShowObjectType::ContinualTask { in_cluster } => {
            show_continual_tasks(scx, from, in_cluster, filter)
        }
        ShowObjectType::NetworkPolicy => {
            assert_none!(from, "parser should reject from");
            show_network_policies(scx, filter)
        }
    }
}

fn show_connections<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, type, comment
        FROM mz_internal.mz_show_connections connections
        WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "type", "comment"]))
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    on_source: Option<ResolvedItemName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut query = format!(
        "SELECT name, comment
        FROM mz_internal.mz_show_tables tables
        WHERE tables.schema_id = '{schema_spec}'",
    );
    if let Some(on_source) = &on_source {
        let on_item = scx.get_item_by_resolved_name(on_source)?;
        if on_item.item_type() != CatalogItemType::Source {
            sql_bail!(
                "cannot show tables on {} because it is a {}",
                on_source.full_name_str(),
                on_item.item_type(),
            );
        }
        query += &format!(" AND tables.source_id = '{}'", on_item.id());
    }
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

fn show_sources<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut where_clause = format!("schema_id = '{schema_spec}'");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = '{}'", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = format!(
        "SELECT name, type, cluster, comment
        FROM mz_internal.mz_show_sources
        WHERE {where_clause}"
    );
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "type", "cluster", "comment"]),
    )
}

fn show_subsources<'a>(
    scx: &'a StatementContext<'a>,
    from_schema: Option<ResolvedSchemaName>,
    on_source: Option<ResolvedItemName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = Vec::new();

    if on_source.is_none() && from_schema.is_none() {
        query_filter.push("subsources.id NOT LIKE 's%'".into());
        let schema_spec = scx.resolve_active_schema().map(|spec| spec.clone())?;
        query_filter.push(format!("subsources.schema_id = '{schema_spec}'"));
    }

    if let Some(on_source) = &on_source {
        let on_item = scx.get_item_by_resolved_name(on_source)?;
        if on_item.item_type() != CatalogItemType::Source {
            sql_bail!(
                "cannot show subsources on {} because it is a {}",
                on_source.full_name_str(),
                on_item.item_type(),
            );
        }
        query_filter.push(format!("sources.id = '{}'", on_item.id()));
    }

    if let Some(schema) = from_schema {
        let schema_spec = schema.schema_spec();
        query_filter.push(format!("subsources.schema_id = '{schema_spec}'"));
    }

    // TODO(database-issues#8322): this looks in both directions for subsources as long as
    // progress collections still exist
    let query = format!(
        "SELECT DISTINCT
            subsources.name AS name,
            subsources.type AS type
        FROM
            mz_sources AS subsources
            JOIN mz_internal.mz_object_dependencies deps ON (subsources.id = deps.object_id OR subsources.id = deps.referenced_object_id)
            JOIN mz_sources AS sources ON (sources.id = deps.object_id OR sources.id = deps.referenced_object_id)
        WHERE (subsources.type = 'subsource' OR subsources.type = 'progress') AND {}",
        itertools::join(query_filter, " AND "),
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_views<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, comment
        FROM mz_internal.mz_show_views
        WHERE schema_id = '{schema_spec}'"
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

fn show_materialized_views<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut where_clause = format!("schema_id = '{schema_spec}'");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = '{}'", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = format!(
        "SELECT name, cluster, comment, replacing
            FROM mz_internal.mz_show_materialized_views
            WHERE {where_clause}"
    );

    let mut projection = vec!["name", "cluster", "comment"];
    if scx.is_feature_flag_enabled(&ENABLE_REPLACEMENT_MATERIALIZED_VIEWS) {
        projection.push("replacing");
    }

    ShowSelect::new(scx, query, filter, None, Some(&projection))
}

fn show_sinks<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = if let Some(ResolvedSchemaName::Schema { schema_spec, .. }) = from {
        schema_spec.to_string()
    } else {
        scx.resolve_active_schema()?.to_string()
    };

    let mut where_clause = format!("schema_id = '{schema_spec}'");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = '{}'", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = format!(
        "SELECT name, type, cluster, comment
        FROM mz_internal.mz_show_sinks sinks
        WHERE {where_clause}"
    );
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "type", "cluster", "comment"]),
    )
}

fn show_types<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, comment
        FROM mz_internal.mz_show_types
        WHERE schema_id = '{schema_spec}'"
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

fn show_all_objects<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, type, comment
         FROM mz_internal.mz_show_all_objects
         WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "type", "comment"]))
}

pub fn show_indexes<'a>(
    scx: &'a StatementContext<'a>,
    from_schema: Option<ResolvedSchemaName>,
    on_object: Option<ResolvedItemName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = Vec::new();

    if on_object.is_none() && from_schema.is_none() && in_cluster.is_none() {
        query_filter.push("on_id NOT LIKE 's%'".into());
        let schema_spec = scx.resolve_active_schema().map(|spec| spec.clone())?;
        query_filter.push(format!("schema_id = '{schema_spec}'"));
    }

    if let Some(on_object) = &on_object {
        let on_item = scx.get_item_by_resolved_name(on_object)?;
        if on_item.item_type() != CatalogItemType::View
            && on_item.item_type() != CatalogItemType::MaterializedView
            && on_item.item_type() != CatalogItemType::Source
            && on_item.item_type() != CatalogItemType::Table
        {
            sql_bail!(
                "cannot show indexes on {} because it is a {}",
                on_object.full_name_str(),
                on_item.item_type(),
            );
        }
        query_filter.push(format!("on_id = '{}'", on_item.id()));
    }

    if let Some(schema) = from_schema {
        let schema_spec = schema.schema_spec();
        query_filter.push(format!("schema_id = '{schema_spec}'"));
    }

    if let Some(cluster) = in_cluster {
        query_filter.push(format!("cluster_id = '{}'", cluster.id))
    };

    let query = format!(
        "SELECT name, on, cluster, key, comment
        FROM mz_internal.mz_show_indexes
        WHERE {}",
        itertools::join(query_filter.iter(), " AND ")
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "on", "cluster", "key", "comment"]),
    )
}

pub fn show_columns<'a>(
    scx: &'a StatementContext<'a>,
    ShowColumnsStatement { table_name, filter }: ShowColumnsStatement<Aug>,
) -> Result<ShowColumnsSelect<'a>, PlanError> {
    let entry = scx.get_item_by_resolved_name(&table_name)?;
    let full_name = scx.catalog.resolve_full_name(entry.name());

    match entry.item_type() {
        CatalogItemType::Source
        | CatalogItemType::Table
        | CatalogItemType::View
        | CatalogItemType::MaterializedView
        | CatalogItemType::ContinualTask => (),
        ty @ CatalogItemType::Connection
        | ty @ CatalogItemType::Index
        | ty @ CatalogItemType::Func
        | ty @ CatalogItemType::Secret
        | ty @ CatalogItemType::Type
        | ty @ CatalogItemType::Sink => {
            sql_bail!("{full_name} is a {ty} and so does not have columns");
        }
    }

    let query = format!(
        "SELECT name, nullable, type, position, comment
         FROM mz_internal.mz_show_columns columns
         WHERE columns.id = '{}'",
        entry.id(),
    );
    let (show_select, new_resolved_ids) = ShowSelect::new_with_resolved_ids(
        scx,
        query,
        filter,
        Some("position"),
        Some(&["name", "nullable", "type", "comment"]),
    )?;
    Ok(ShowColumnsSelect {
        id: entry.id(),
        show_select,
        new_resolved_ids,
    })
}

// The rationale for which fields to include in the tuples are those
// that are mandatory when creating a replica as part of the CREATE
// CLUSTER command, i.e., name and size.
pub fn show_clusters<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name, replicas, comment FROM mz_internal.mz_show_clusters".to_string();
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "replicas", "comment"]),
    )
}

pub fn show_cluster_replicas<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "
    SELECT cluster, replica, size, ready, comment
    FROM mz_internal.mz_show_cluster_replicas
    "
    .to_string();

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["cluster", "replica", "size", "ready", "comment"]),
    )
}

pub fn show_secrets<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let query = format!(
        "SELECT name, comment
        FROM mz_internal.mz_show_secrets
        WHERE schema_id = '{schema_spec}'",
    );

    ShowSelect::new(scx, query, filter, None, Some(&["name", "comment"]))
}

pub fn show_privileges<'a>(
    scx: &'a StatementContext<'a>,
    object_type: Option<SystemObjectType>,
    role: Option<ResolvedRoleName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = Vec::new();
    if let Some(object_type) = object_type {
        query_filter.push(format!(
            "object_type = '{}'",
            object_type.to_string().to_lowercase()
        ));
    }
    if let Some(role) = role {
        let name = role.name;
        query_filter.push(format!("CASE WHEN grantee = 'PUBLIC' THEN true ELSE pg_has_role('{name}', grantee, 'USAGE') END"));
    }
    let query_filter = if query_filter.len() > 0 {
        format!("WHERE {}", itertools::join(query_filter, " AND "))
    } else {
        "".to_string()
    };

    let query = format!(
        "SELECT grantor, grantee, database, schema, name, object_type, privilege_type
        FROM mz_internal.mz_show_all_privileges
        {query_filter}",
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&[
            "grantor",
            "grantee",
            "database",
            "schema",
            "name",
            "object_type",
            "privilege_type",
        ]),
    )
}

pub fn show_default_privileges<'a>(
    scx: &'a StatementContext<'a>,
    object_type: Option<ObjectType>,
    role: Option<ResolvedRoleName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = Vec::new();
    if let Some(object_type) = object_type {
        query_filter.push(format!(
            "object_type = '{}'",
            object_type.to_string().to_lowercase()
        ));
    }
    if let Some(role) = role {
        let name = role.name;
        query_filter.push(format!("CASE WHEN grantee = 'PUBLIC' THEN true ELSE pg_has_role('{name}', grantee, 'USAGE') END"));
    }
    let query_filter = if query_filter.len() > 0 {
        format!("WHERE {}", itertools::join(query_filter, " AND "))
    } else {
        "".to_string()
    };

    let query = format!(
        "SELECT object_owner, database, schema, object_type, grantee, privilege_type
        FROM mz_internal.mz_show_default_privileges
        {query_filter}",
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&[
            "object_owner",
            "database",
            "schema",
            "object_type",
            "grantee",
            "privilege_type",
        ]),
    )
}

pub fn show_role_membership<'a>(
    scx: &'a StatementContext<'a>,
    role: Option<ResolvedRoleName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = Vec::new();
    if let Some(role) = role {
        let name = role.name;
        query_filter.push(format!("pg_has_role('{name}', member, 'USAGE')"));
    }
    let query_filter = if query_filter.len() > 0 {
        format!("WHERE {}", itertools::join(query_filter, " AND "))
    } else {
        "".to_string()
    };

    let query = format!(
        "SELECT role, member, grantor
        FROM mz_internal.mz_show_role_members
        {query_filter}",
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["role", "member", "grantor"]),
    )
}

fn show_continual_tasks<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut where_clause = format!("schema_id = '{schema_spec}'");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = '{}'", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = format!(
        "SELECT name, cluster, comment
        FROM mz_internal.mz_show_continual_tasks
        WHERE {where_clause}"
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "cluster", "comment"]),
    )
}

/// An intermediate result when planning a `SHOW` query.
///
/// Can be interrogated for its columns, or converted into a proper [`Plan`].
pub struct ShowSelect<'a> {
    scx: &'a StatementContext<'a>,
    pub(crate) stmt: SelectStatement<Aug>,
}

impl<'a> ShowSelect<'a> {
    /// Constructs a new [`ShowSelect`] from a query that provides the base
    /// data and an optional user-supplied filter, order column, and
    /// projection on that data.
    ///
    /// Note that the query must return a column named `name`, as the filter
    /// may implicitly reference this column. Any `ORDER BY` in the query is
    /// ignored. `ShowSelects`s are always ordered in ascending order by all
    /// columns from left to right unless an order field is supplied.
    fn new(
        scx: &'a StatementContext,
        query: String,
        filter: Option<ShowStatementFilter<Aug>>,
        order: Option<&str>,
        projection: Option<&[&str]>,
    ) -> Result<ShowSelect<'a>, PlanError> {
        Self::new_with_resolved_ids(scx, query, filter, order, projection)
            .map(|(show_select, _)| show_select)
    }

    fn new_with_resolved_ids(
        scx: &'a StatementContext,
        query: String,
        filter: Option<ShowStatementFilter<Aug>>,
        order: Option<&str>,
        projection: Option<&[&str]>,
    ) -> Result<(ShowSelect<'a>, ResolvedIds), PlanError> {
        let filter = match filter {
            Some(ShowStatementFilter::Like(like)) => format!("name LIKE {}", Value::String(like)),
            Some(ShowStatementFilter::Where(expr)) => expr.to_string(),
            None => "true".to_string(),
        };
        let query = format!(
            "SELECT {} FROM ({}) q WHERE {} ORDER BY {}",
            projection
                .map(|ps| ps.join(", "))
                .unwrap_or_else(|| "*".into()),
            query,
            filter,
            order.unwrap_or("q.*")
        );

        Self::new_from_bare_query(scx, query)
    }

    pub fn new_from_bare_query(
        scx: &'a StatementContext,
        query: String,
    ) -> Result<(ShowSelect<'a>, ResolvedIds), PlanError> {
        let stmts = parse::parse(&query).expect("ShowSelect::new called with invalid SQL");
        let stmt = match stmts.into_element().ast {
            Statement::Select(select) => select,
            _ => panic!("ShowSelect::new called with non-SELECT statement"),
        };
        let (mut stmt, new_resolved_ids) = names::resolve(scx.catalog, stmt)?;
        transform_ast::transform(scx, &mut stmt)?;
        Ok((ShowSelect { scx, stmt }, new_resolved_ids))
    }

    /// Computes the shape of this `ShowSelect`.
    pub fn describe(self) -> Result<StatementDesc, PlanError> {
        dml::describe_select(self.scx, self.stmt)
    }

    /// Converts this `ShowSelect` into a [`Plan`].
    pub fn plan(self) -> Result<Plan, PlanError> {
        dml::plan_select(self.scx, self.stmt, &Params::empty(), None)
    }

    /// Converts this `ShowSelect` into a [`(HirRelationExpr, Scope)`].
    pub fn plan_hir(self, qcx: &QueryContext) -> Result<(HirRelationExpr, Scope), PlanError> {
        query::plan_nested_query(&mut qcx.clone(), &self.stmt.query)
    }
}

pub struct ShowColumnsSelect<'a> {
    id: CatalogItemId,
    new_resolved_ids: ResolvedIds,
    show_select: ShowSelect<'a>,
}

impl<'a> ShowColumnsSelect<'a> {
    pub fn describe(self) -> Result<StatementDesc, PlanError> {
        self.show_select.describe()
    }

    pub fn plan(self) -> Result<Plan, PlanError> {
        let select_plan = self.show_select.plan()?;
        match select_plan {
            Plan::Select(select_plan) => Ok(Plan::ShowColumns(ShowColumnsPlan {
                id: self.id,
                select_plan,
                new_resolved_ids: self.new_resolved_ids,
            })),
            _ => {
                tracing::error!(
                    "SHOW COLUMNS produced a non select plan. plan: {:?}",
                    select_plan
                );
                Err(PlanError::Unstructured(
                    "SHOW COLUMNS produced an unexpected plan. Please file a bug.".to_string(),
                ))
            }
        }
    }

    pub fn plan_hir(self, qcx: &QueryContext) -> Result<(HirRelationExpr, Scope), PlanError> {
        self.show_select.plan_hir(qcx)
    }
}

/// Convert a SQL statement into a form that could be used as input, as well as
/// is more amenable to human consumption.
fn humanize_sql_for_show_create(
    catalog: &dyn SessionCatalog,
    id: CatalogItemId,
    sql: &str,
    redacted: bool,
) -> Result<String, PlanError> {
    use mz_sql_parser::ast::{CreateSourceConnection, MySqlConfigOptionName, PgConfigOptionName};

    let parsed = parse::parse(sql)?.into_element().ast;
    let (mut resolved, _) = names::resolve(catalog, parsed)?;

    // Simplify names.
    let mut simplifier = NameSimplifier { catalog };
    simplifier.visit_statement_mut(&mut resolved);

    match &mut resolved {
        // Strip internal `AS OF` syntax.
        Statement::CreateMaterializedView(stmt) => stmt.as_of = None,
        Statement::CreateContinualTask(stmt) => stmt.as_of = None,
        // `CREATE SOURCE` statements should roundtrip. However, sources and
        // their subsources have a complex relationship, so we need to do a lot
        // of work to reconstruct the statement for multi-output sources.
        //
        // For instance, `DROP SOURCE` statements can leave dangling references
        // to subsources that must be filtered out here, that, due to catalog
        // transaction limitations, can only be cleaned up when a top-level
        // source is altered.
        Statement::CreateSource(stmt) => {
            // Collect all current subsource references.
            let mut curr_references: BTreeMap<UnresolvedItemName, Vec<UnresolvedItemName>> =
                catalog
                    .get_item(&id)
                    .used_by()
                    .into_iter()
                    .filter_map(|subsource| {
                        let item = catalog.get_item(subsource);
                        item.subsource_details().map(|(_id, reference, _details)| {
                            let name = item.name();
                            let subsource_name = catalog.resolve_full_name(name);
                            let subsource_name = UnresolvedItemName::from(subsource_name);
                            (reference.clone(), subsource_name)
                        })
                    })
                    .fold(BTreeMap::new(), |mut map, (reference, subsource_name)| {
                        map.entry(reference)
                            .or_insert_with(Vec::new)
                            .push(subsource_name);
                        map
                    });

            match &mut stmt.connection {
                CreateSourceConnection::Postgres { options, .. } => {
                    options.retain_mut(|o| {
                        match o.name {
                            // Dropping a subsource does not remove any `TEXT
                            // COLUMNS` values that refer to the table it
                            // ingests, which we'll handle below.
                            PgConfigOptionName::TextColumns => {}
                            // Drop details, which does not roundtrip.
                            PgConfigOptionName::Details => return false,
                            _ => return true,
                        };
                        match &mut o.value {
                            Some(WithOptionValue::Sequence(text_cols)) => {
                                text_cols.retain(|v| match v {
                                    WithOptionValue::UnresolvedItemName(n) => {
                                        let mut name = n.clone();
                                        // Remove the column reference.
                                        name.0.truncate(3);
                                        curr_references.contains_key(&name)
                                    }
                                    _ => unreachable!(
                                        "TEXT COLUMNS must be sequence of unresolved item names"
                                    ),
                                });
                                !text_cols.is_empty()
                            }
                            _ => unreachable!(
                                "TEXT COLUMNS must be sequence of unresolved item names"
                            ),
                        }
                    });
                }
                CreateSourceConnection::SqlServer { options, .. } => {
                    // TODO(sql_server2): TEXT and EXCLUDE columns are represented by
                    // `schema.table.column` whereas our external table references are
                    // `database.schema.table`. We handle the mismatch here but should
                    // probably fully qualify our TEXT and EXCLUDE column references.
                    let adjusted_references: BTreeSet<_> = curr_references
                        .keys()
                        .map(|name| {
                            if name.0.len() == 3 {
                                // Strip the database component of the name.
                                let adjusted_name = name.0[1..].to_vec();
                                UnresolvedItemName(adjusted_name)
                            } else {
                                name.clone()
                            }
                        })
                        .collect();

                    options.retain_mut(|o| {
                        match o.name {
                            // Dropping a subsource does not remove any `TEXT COLUMNS`
                            // values that refer to the table it ingests, which we'll
                            // handle below.
                            SqlServerConfigOptionName::TextColumns
                            | SqlServerConfigOptionName::ExcludeColumns => {}
                            // Drop details, which does not roundtrip.
                            SqlServerConfigOptionName::Details => return false,
                        };

                        match &mut o.value {
                            Some(WithOptionValue::Sequence(seq_unresolved_item_names)) => {
                                seq_unresolved_item_names.retain(|v| match v {
                                    WithOptionValue::UnresolvedItemName(n) => {
                                        let mut name = n.clone();
                                        // Remove column reference.
                                        name.0.truncate(2);
                                        adjusted_references.contains(&name)
                                    }
                                    _ => unreachable!(
                                        "TEXT COLUMNS + EXCLUDE COLUMNS must be sequence of unresolved item names"
                                    ),
                                });
                                !seq_unresolved_item_names.is_empty()
                            }
                            _ => unreachable!(
                                "TEXT COLUMNS + EXCLUDE COLUMNS must be sequence of unresolved item names"
                            ),
                        }
                    });
                }
                CreateSourceConnection::MySql { options, .. } => {
                    options.retain_mut(|o| {
                        match o.name {
                            // Dropping a subsource does not remove any `TEXT
                            // COLUMNS` values that refer to the table it
                            // ingests, which we'll handle below.
                            MySqlConfigOptionName::TextColumns
                            | MySqlConfigOptionName::ExcludeColumns => {}
                            // Drop details, which does not roundtrip.
                            MySqlConfigOptionName::Details => return false,
                        };

                        match &mut o.value {
                            Some(WithOptionValue::Sequence(seq_unresolved_item_names)) => {
                                seq_unresolved_item_names.retain(|v| match v {
                                    WithOptionValue::UnresolvedItemName(n) => {
                                        let mut name = n.clone();
                                        // Remove column reference.
                                        name.0.truncate(2);
                                        curr_references.contains_key(&name)
                                    }
                                    _ => unreachable!(
                                        "TEXT COLUMNS + EXCLUDE COLUMNS must be sequence of unresolved item names"
                                    ),
                                });
                                !seq_unresolved_item_names.is_empty()
                            }
                            _ => unreachable!(
                                "TEXT COLUMNS + EXCLUDE COLUMNS must be sequence of unresolved item names"
                            ),
                        }
                    });
                }
                CreateSourceConnection::LoadGenerator { .. } if !curr_references.is_empty() => {
                    // Load generator sources with any references only support
                    // `FOR ALL TABLES`. However, this would change if database-issues#7911
                    // landed.
                    curr_references.clear();
                    stmt.external_references = Some(ExternalReferences::All);
                }
                CreateSourceConnection::Kafka { .. }
                | CreateSourceConnection::LoadGenerator { .. } => {}
            }

            // If this source has any references, reconstruct them.
            if !curr_references.is_empty() {
                let mut subsources: Vec<_> = curr_references
                    .into_iter()
                    .flat_map(|(reference, names)| {
                        names.into_iter().map(move |name| ExternalReferenceExport {
                            reference: reference.clone(),
                            alias: Some(name),
                        })
                    })
                    .collect();
                subsources.sort();
                stmt.external_references = Some(ExternalReferences::SubsetTables(subsources));
            }
        }
        Statement::CreateSubsource(stmt) => {
            stmt.with_options.retain_mut(|o| {
                match o.name {
                    CreateSubsourceOptionName::TextColumns => true,
                    CreateSubsourceOptionName::RetainHistory => true,
                    CreateSubsourceOptionName::ExcludeColumns => true,
                    // Drop details, which does not roundtrip.
                    CreateSubsourceOptionName::Details => false,
                    CreateSubsourceOptionName::ExternalReference => true,
                    CreateSubsourceOptionName::Progress => true,
                }
            });
        }

        _ => (),
    }

    Ok(mz_sql_pretty::to_pretty(
        &resolved,
        PrettyConfig {
            width: mz_sql_pretty::DEFAULT_WIDTH,
            format_mode: if redacted {
                FormatMode::SimpleRedacted
            } else {
                FormatMode::Simple
            },
        },
    ))
}
