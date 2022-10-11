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
use mz_repr::{Datum, RelationDesc, Row, ScalarType};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{ShowCreateConnectionStatement, ShowCreateMaterializedViewStatement};
use query::QueryContext;

use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    ObjectType, SelectStatement, ShowColumnsStatement, ShowCreateIndexStatement,
    ShowCreateSinkStatement, ShowCreateSourceStatement, ShowCreateTableStatement,
    ShowCreateViewStatement, ShowDatabasesStatement, ShowIndexesStatement, ShowObjectsStatement,
    ShowSchemasStatement, ShowStatementFilter, Statement, Value,
};
use crate::catalog::{CatalogItemType, SessionCatalog};
use crate::names::{
    self, Aug, NameSimplifier, ResolvedClusterName, ResolvedDatabaseName, ResolvedSchemaName,
};
use crate::parse;
use crate::plan::scope::Scope;
use crate::plan::statement::{dml, StatementContext, StatementDesc};
use crate::plan::{query, HirRelationExpr, Params, Plan, PlanError, SendRowsPlan};

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
) -> Result<SendRowsPlan, PlanError> {
    let view = scx.get_item_by_resolved_name(&view_name)?;
    match view.item_type() {
        CatalogItemType::View => {
            let name = view_name.full_name_str();
            let create_sql = simplify_names(scx.catalog, view.create_sql())?;
            Ok(SendRowsPlan {
                rows: vec![Row::pack_slice(&[
                    Datum::String(&name),
                    Datum::String(&create_sql),
                ])],
            })
        }
        CatalogItemType::MaterializedView => Err(PlanError::ShowCreateViewOnMaterializedView(
            view_name.full_name_str(),
        )),
        _ => sql_bail!("{} is not a view", view_name.full_name_str()),
    }
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
    stmt: ShowCreateMaterializedViewStatement<Aug>,
) -> Result<SendRowsPlan, PlanError> {
    let name = stmt.materialized_view_name;
    let mview = scx.get_item_by_resolved_name(&name)?;
    if let CatalogItemType::MaterializedView = mview.item_type() {
        let full_name = name.full_name_str();
        let create_sql = simplify_names(scx.catalog, mview.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&full_name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("{} is not a materialized view", name.full_name_str());
    }
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

pub fn plan_show_create_table(
    scx: &StatementContext,
    ShowCreateTableStatement { table_name }: ShowCreateTableStatement<Aug>,
) -> Result<SendRowsPlan, PlanError> {
    let table = scx.get_item_by_resolved_name(&table_name)?;
    if let CatalogItemType::Table = table.item_type() {
        let name = table_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, table.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("{} is not a table", table_name.full_name_str());
    }
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
) -> Result<SendRowsPlan, PlanError> {
    let source = scx.get_item_by_resolved_name(&source_name)?;
    if let CatalogItemType::Source = source.item_type() {
        let name = source_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, source.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("{} is not a source", source_name.full_name_str());
    }
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
) -> Result<SendRowsPlan, PlanError> {
    let sink = scx.get_item_by_resolved_name(&sink_name)?;
    if let CatalogItemType::Sink = sink.item_type() {
        let name = sink_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, sink.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("'{}' is not a sink", sink_name.full_name_str());
    }
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
) -> Result<SendRowsPlan, PlanError> {
    let index = scx.get_item_by_resolved_name(&index_name)?;
    if let CatalogItemType::Index = index.item_type() {
        let name = index_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, index.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("'{}' is not an index", index_name.full_name_str());
    }
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
) -> Result<SendRowsPlan, PlanError> {
    let connection = scx.get_item_by_resolved_name(&connection_name)?;
    if let CatalogItemType::Connection = connection.item_type() {
        let name = connection_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, connection.create_sql())?;
        Ok(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        })
    } else {
        sql_bail!("'{}' is not a connection", connection_name.full_name_str());
    }
}

pub fn show_databases<'a>(
    scx: &'a StatementContext<'a>,
    ShowDatabasesStatement { filter }: ShowDatabasesStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT name FROM mz_catalog.mz_databases".to_string();
    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_schemas<'a>(
    scx: &'a StatementContext<'a>,
    ShowSchemasStatement { from, filter }: ShowSchemasStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    let database_id = match from {
        Some(ResolvedDatabaseName::Database { id, .. }) => id.0,
        None => match scx.active_database() {
            Some(id) => id.0,
            None => sql_bail!("no database specified and no active database"),
        },
        Some(ResolvedDatabaseName::Error) => {
            unreachable!("should have been handled in name resolution")
        }
    };
    let query = format!(
        "SELECT name
        FROM mz_catalog.mz_schemas
        WHERE database_id IS NULL OR database_id = {database_id}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_objects<'a>(
    scx: &'a StatementContext<'a>,
    ShowObjectsStatement {
        object_type,
        from,
        in_cluster,
        filter,
    }: ShowObjectsStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    match object_type {
        ObjectType::Table => show_tables(scx, from, filter),
        ObjectType::Source => show_sources(scx, from, filter),
        ObjectType::View => show_views(scx, from, filter),
        ObjectType::MaterializedView => show_materialized_views(scx, from, in_cluster, filter),
        ObjectType::Sink => show_sinks(scx, from, filter),
        ObjectType::Type => show_types(scx, from, filter),
        ObjectType::Object => show_all_objects(scx, from, filter),
        ObjectType::Role => bail_unsupported!("SHOW ROLES"),
        ObjectType::Cluster => show_clusters(scx, filter),
        ObjectType::ClusterReplica => show_cluster_replicas(scx, filter),
        ObjectType::Secret => show_secrets(scx, from, filter),
        ObjectType::Index => unreachable!("SHOW INDEX handled separately"),
        ObjectType::Connection => show_connections(scx, from, filter),
    }
}

fn show_connections<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT t.name, t.type
        FROM mz_catalog.mz_connections t
        JOIN mz_catalog.mz_schemas s on t.schema_id = s.id
        WHERE schema_id = {schema_spec}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT t.name
        FROM mz_catalog.mz_tables t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE schema_id = {schema_spec}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_sources<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT name, type, size
        FROM mz_catalog.mz_sources
        WHERE schema_id = {schema_spec}"
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
        WHERE schema_id = {schema_spec}"
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_materialized_views<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut where_clause = format!("schema_id = {schema_spec}");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = '{}'", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = format!(
        "SELECT mviews.name, clusters.name AS cluster
         FROM mz_materialized_views AS mviews
         JOIN mz_clusters AS clusters
            ON clusters.id = mviews.cluster_id
         WHERE {where_clause}"
    );

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_sinks<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filters = vec![];

    if let Some(ResolvedSchemaName::Schema { schema_spec, .. }) = from {
        query_filters.push(format!("schema_id = {}", schema_spec));
    } else {
        query_filters.push(format!("schema_id = {}", scx.resolve_active_schema()?));
    };

    let query_filters = itertools::join(query_filters.iter(), " AND ");

    let query = format!(
        "SELECT sinks.name, sinks.type, sinks.size
        FROM mz_catalog.mz_sinks AS sinks
        WHERE {query_filters}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_types<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT t.name
        FROM mz_catalog.mz_types t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE t.schema_id = {schema_spec}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_all_objects<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let query = format!(
        "SELECT o.name, o.type
        FROM mz_catalog.mz_objects o
        JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
        WHERE o.schema_id = {schema_spec}",
    );
    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_indexes<'a>(
    scx: &'a StatementContext<'a>,
    ShowIndexesStatement {
        in_cluster,
        on_object,
        from_schema,
        filter,
    }: ShowIndexesStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filter = vec!["idxs.on_id NOT LIKE 's%'".into()];

    let schema_spec = scx.resolve_optional_schema(&from_schema)?;
    query_filter.push(format!("objs.schema_id = {}", schema_spec));

    if let Some(on_object) = on_object {
        let on_item = scx.get_item_by_resolved_name(&on_object)?;
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
        query_filter.push(format!("objs.id = '{}'", on_item.id()));
    }

    if let Some(cluster) = in_cluster {
        query_filter.push(format!("clusters.id = '{}'", cluster.id))
    };

    let query = format!(
        "SELECT
            idxs.name AS name,
            objs.name AS on,
            clusters.name AS cluster,
            ARRAY_AGG(
                CASE
                    WHEN idx_cols.on_expression IS NULL THEN obj_cols.name
                    ELSE idx_cols.on_expression
                END
                ORDER BY idx_cols.index_position ASC
            ) AS key
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_index_columns AS idx_cols ON idxs.id = idx_cols.index_id
            JOIN mz_catalog.mz_objects AS objs ON idxs.on_id = objs.id
            JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = idxs.cluster_id
            LEFT JOIN mz_catalog.mz_columns AS obj_cols
                ON idxs.on_id = obj_cols.id AND idx_cols.on_position = obj_cols.position
        WHERE
            {}
        GROUP BY idxs.name, objs.name, clusters.name",
        itertools::join(query_filter.iter(), " AND ")
    );

    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_columns<'a>(
    scx: &'a StatementContext<'a>,
    ShowColumnsStatement { table_name, filter }: ShowColumnsStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
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
         FROM mz_catalog.mz_columns AS mz_columns
         WHERE mz_columns.id = '{}'",
        entry.id(),
    );
    ShowSelect::new(
        scx,
        query,
        filter,
        Some("position"),
        Some(&["name", "nullable", "type"]),
    )
}

pub fn show_clusters<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = "SELECT mz_clusters.name FROM mz_catalog.mz_clusters".to_string();

    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_cluster_replicas<'a>(
    scx: &'a StatementContext<'a>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let query = r#"
    SELECT
        mz_catalog.mz_clusters.name AS cluster,
        mz_catalog.mz_cluster_replicas.name AS replica
    FROM
        mz_catalog.mz_cluster_replicas
        JOIN mz_catalog.mz_clusters ON
                mz_catalog.mz_cluster_replicas.cluster_id = mz_catalog.mz_clusters.id
    ORDER BY
        1, 2"#
        .to_string();

    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_secrets<'a>(
    scx: &'a StatementContext<'a>,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let query = format!(
        "SELECT sec.name FROM mz_catalog.mz_secrets sec
        JOIN mz_catalog.mz_schemas s ON sec.schema_id = s.id
        WHERE schema_id = {schema_spec}",
    );

    ShowSelect::new(scx, query, filter, None, None)
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
        let stmt = match stmts.into_element() {
            Statement::Select(select) => select,
            _ => panic!("ShowSelect::new called with non-SELECT statement"),
        };
        let (stmt, _) = names::resolve(scx.catalog, stmt)?;
        Ok(ShowSelect { scx, stmt })
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

fn simplify_names(catalog: &dyn SessionCatalog, sql: &str) -> Result<String, PlanError> {
    let parsed = parse::parse(sql)?.into_element();
    let (mut resolved, _) = names::resolve(catalog, parsed)?;
    let mut simplifier = NameSimplifier { catalog };
    simplifier.visit_statement_mut(&mut resolved);
    Ok(resolved.to_ast_string_stable())
}
