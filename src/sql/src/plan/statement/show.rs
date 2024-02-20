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

use std::fmt::Write;

use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ObjectType, ShowCreateConnectionStatement, ShowCreateMaterializedViewStatement, ShowObjectType,
    SystemObjectType,
};
use query::QueryContext;

use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    SelectStatement, ShowColumnsStatement, ShowCreateIndexStatement, ShowCreateSinkStatement,
    ShowCreateSourceStatement, ShowCreateTableStatement, ShowCreateViewStatement,
    ShowObjectsStatement, ShowStatementFilter, Statement, Value,
};
use crate::catalog::{CatalogItemType, SessionCatalog};
use crate::names::{
    self, Aug, NameSimplifier, ResolvedClusterName, ResolvedDatabaseName, ResolvedIds,
    ResolvedItemName, ResolvedRoleName, ResolvedSchemaName,
};
use crate::parse;
use crate::plan::scope::Scope;
use crate::plan::statement::{dml, StatementContext, StatementDesc};
use crate::plan::{
    query, transform_ast, HirRelationExpr, Params, Plan, PlanError, ShowColumnsPlan, ShowCreatePlan,
};

pub fn describe_show_create_view(
    _: &StatementContext,
    _: ShowCreateViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_view(
    scx: &StatementContext,
    ShowCreateViewStatement { view_name }: ShowCreateViewStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &view_name, CatalogItemType::View)
}

pub fn describe_show_create_materialized_view(
    _: &StatementContext,
    _: ShowCreateMaterializedViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_materialized_view(
    scx: &StatementContext,
    ShowCreateMaterializedViewStatement {
        materialized_view_name,
    }: ShowCreateMaterializedViewStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(
        scx,
        &materialized_view_name,
        CatalogItemType::MaterializedView,
    )
}

pub fn describe_show_create_table(
    _: &StatementContext,
    _: ShowCreateTableStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

fn plan_show_create(
    scx: &StatementContext,
    name: &ResolvedItemName,
    expect_type: CatalogItemType,
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
    let create_sql = humanize_sql(scx.catalog, item.create_sql())?;
    Ok(ShowCreatePlan {
        id: item.id(),
        row: Row::pack_slice(&[Datum::String(&name), Datum::String(&create_sql)]),
    })
}

pub fn plan_show_create_table(
    scx: &StatementContext,
    ShowCreateTableStatement { table_name }: ShowCreateTableStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &table_name, CatalogItemType::Table)
}

pub fn describe_show_create_source(
    _: &StatementContext,
    _: ShowCreateSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_source(
    scx: &StatementContext,
    ShowCreateSourceStatement { source_name }: ShowCreateSourceStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &source_name, CatalogItemType::Source)
}

pub fn describe_show_create_sink(
    _: &StatementContext,
    _: ShowCreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_sink(
    scx: &StatementContext,
    ShowCreateSinkStatement { sink_name }: ShowCreateSinkStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &sink_name, CatalogItemType::Sink)
}

pub fn describe_show_create_index(
    _: &StatementContext,
    _: ShowCreateIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_index(
    scx: &StatementContext,
    ShowCreateIndexStatement { index_name }: ShowCreateIndexStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &index_name, CatalogItemType::Index)
}

pub fn describe_show_create_connection(
    _: &StatementContext,
    _: ShowCreateConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("create_sql", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_connection(
    scx: &StatementContext,
    ShowCreateConnectionStatement { connection_name }: ShowCreateConnectionStatement<Aug>,
) -> Result<ShowCreatePlan, PlanError> {
    plan_show_create(scx, &connection_name, CatalogItemType::Connection)
}

pub fn show_databases<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name FROM mz_catalog.mz_databases".to_string();
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
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
        "SELECT name
        FROM mz_catalog.mz_schemas
        WHERE database_id IS NULL OR database_id = '{database_id}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
}

pub fn show_roles<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name FROM mz_catalog.mz_roles WHERE id NOT LIKE 's%'".to_string();
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
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
        ShowObjectType::Table => show_tables(scx, from, filter),
        ShowObjectType::Source { in_cluster } => show_sources(scx, from, in_cluster, filter),
        ShowObjectType::Subsource { on_source } => show_subsources(scx, from, on_source, filter),
        ShowObjectType::View => show_views(scx, from, filter),
        ShowObjectType::Sink { in_cluster } => show_sinks(scx, from, in_cluster, filter),
        ShowObjectType::Type => show_types(scx, from, filter),
        ShowObjectType::Object => show_all_objects(scx, from, filter),
        ShowObjectType::Role => {
            assert!(from.is_none(), "parser should reject from");
            show_roles(scx, filter)
        }
        ShowObjectType::Cluster => {
            assert!(from.is_none(), "parser should reject from");
            show_clusters(scx, filter)
        }
        ShowObjectType::ClusterReplica => {
            assert!(from.is_none(), "parser should reject from");
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
            assert!(from.is_none(), "parser should reject from");
            show_databases(scx, filter)
        }
        ShowObjectType::Schema { from: db_from } => {
            assert!(from.is_none(), "parser should reject from");
            show_schemas(scx, db_from, filter)
        }
        ShowObjectType::Privileges { object_type, role } => {
            assert!(from.is_none(), "parser should reject from");
            show_privileges(scx, object_type, role, filter)
        }
        ShowObjectType::DefaultPrivileges { object_type, role } => {
            assert!(from.is_none(), "parser should reject from");
            show_default_privileges(scx, object_type, role, filter)
        }
        ShowObjectType::RoleMembership { role } => {
            assert!(from.is_none(), "parser should reject from");
            show_role_membership(scx, role, filter)
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
        "SELECT name, type
        FROM mz_catalog.mz_connections
        WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "type"]))
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name
        FROM mz_catalog.mz_tables
        WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
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
        "SELECT name, type, size, cluster
        FROM mz_internal.mz_show_sources
        WHERE {where_clause}"
    );
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "type", "size", "cluster"]),
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

    let query = format!(
        "SELECT
            subsources.name AS name,
            subsources.type AS type
        FROM
            mz_sources AS subsources
            JOIN mz_internal.mz_object_dependencies deps ON subsources.id = deps.referenced_object_id
            JOIN mz_sources AS sources ON sources.id = deps.object_id
        WHERE {}",
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
        "SELECT name
        FROM mz_catalog.mz_views
        WHERE schema_id = '{schema_spec}'"
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
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
        "SELECT name, cluster
         FROM mz_internal.mz_show_materialized_views
         WHERE {where_clause}"
    );

    ShowSelect::new(scx, query, filter, None, Some(&["name", "cluster"]))
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
        "SELECT name, type, size, cluster
        FROM mz_internal.mz_show_sinks
        WHERE {where_clause}"
    );
    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "type", "size", "cluster"]),
    )
}

fn show_types<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name
        FROM mz_catalog.mz_types
        WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
}

fn show_all_objects<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, type
        FROM mz_catalog.mz_objects
        WHERE schema_id = '{schema_spec}'",
    );
    ShowSelect::new(scx, query, filter, None, Some(&["name", "type"]))
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
        "SELECT name, on, cluster, key
        FROM mz_internal.mz_show_indexes
        WHERE {}",
        itertools::join(query_filter.iter(), " AND ")
    );

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["name", "on", "cluster", "key"]),
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
        | CatalogItemType::MaterializedView => (),
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
        "SELECT
            mz_columns.name,
            mz_columns.nullable,
            mz_columns.type,
            mz_columns.position
         FROM mz_catalog.mz_columns
         WHERE mz_columns.id = '{}'",
        entry.id(),
    );
    let (show_select, new_resolved_ids) = ShowSelect::new_with_resolved_ids(
        scx,
        query,
        filter,
        Some("position"),
        Some(&["name", "nullable", "type"]),
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
    let query = "
SELECT
    mc.name,
    pg_catalog.string_agg(mcr.name || ' (' || mcr.size || ')', ', ' ORDER BY mcr.name)
        AS replicas
FROM
    mz_catalog.mz_clusters mc
        LEFT JOIN mz_catalog.mz_cluster_replicas mcr ON mc.id = mcr.cluster_id
GROUP BY mc.name"
        .to_string();
    ShowSelect::new(scx, query, filter, None, Some(&["name", "replicas"]))
}

pub fn show_cluster_replicas<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT cluster, replica, size, ready FROM mz_internal.mz_show_cluster_replicas"
        .to_string();

    ShowSelect::new(
        scx,
        query,
        filter,
        None,
        Some(&["cluster", "replica", "size", "ready"]),
    )
}

pub fn show_secrets<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let query = format!(
        "SELECT name
        FROM mz_catalog.mz_secrets
        WHERE schema_id = '{schema_spec}'",
    );

    ShowSelect::new(scx, query, filter, None, Some(&["name"]))
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
        query_filter.push(format!(
            "(pg_has_role('{name}', member, 'USAGE') OR role = '{name}')"
        ));
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

/// An intermediate result when planning a `SHOW` query.
///
/// Can be interrogated for its columns, or converted into a proper [`Plan`].
pub struct ShowSelect<'a> {
    scx: &'a StatementContext<'a>,
    stmt: SelectStatement<Aug>,
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
    id: GlobalId,
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

/// Convert a SQL statement into a form suitable for human consumption.
fn humanize_sql(catalog: &dyn SessionCatalog, sql: &str) -> Result<String, PlanError> {
    let parsed = parse::parse(sql)?.into_element().ast;
    let (mut resolved, _) = names::resolve(catalog, parsed)?;

    // Simplify names.
    let mut simplifier = NameSimplifier { catalog };
    simplifier.visit_statement_mut(&mut resolved);

    // Strip internal `AS OF` syntax.
    match &mut resolved {
        Statement::CreateMaterializedView(stmt) => stmt.as_of = None,
        _ => (),
    }

    Ok(resolved.to_ast_string_stable())
}
