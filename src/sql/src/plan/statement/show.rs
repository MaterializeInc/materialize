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
use mz_sql_parser::ast::{ShowCreateConnectionStatement, ShowCreateRecordedViewStatement};

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
use crate::plan::statement::{dml, StatementContext, StatementDesc};
use crate::plan::{Params, Plan, PlanError, SendRowsPlan};

pub fn describe_show_create_view(
    _: &StatementContext,
    _: ShowCreateViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("View", ScalarType::String.nullable(false))
            .with_column("Create View", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_view(
    scx: &mut StatementContext,
    ShowCreateViewStatement { view_name }: ShowCreateViewStatement<Aug>,
) -> Result<Plan, PlanError> {
    let view = scx.get_item_by_resolved_name(&view_name)?;
    match view.item_type() {
        CatalogItemType::View => {
            let name = view_name.full_name_str();
            let create_sql = simplify_names(scx.catalog, view.create_sql())?;
            Ok(Plan::SendRows(SendRowsPlan {
                rows: vec![Row::pack_slice(&[
                    Datum::String(&name),
                    Datum::String(&create_sql),
                ])],
            }))
        }
        CatalogItemType::RecordedView => Err(PlanError::ShowCreateViewOnRecordedView(
            view_name.full_name_str(),
        )),
        _ => sql_bail!("{} is not a view", view_name.full_name_str()),
    }
}

pub fn describe_show_create_recorded_view(
    _: &StatementContext,
    _: ShowCreateRecordedViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Recorded View", ScalarType::String.nullable(false))
            .with_column("Create Recorded View", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_recorded_view(
    scx: &mut StatementContext,
    stmt: ShowCreateRecordedViewStatement<Aug>,
) -> Result<Plan, PlanError> {
    let name = stmt.recorded_view_name;
    let rview = scx.get_item_by_resolved_name(&name)?;
    if let CatalogItemType::RecordedView = rview.item_type() {
        let full_name = name.full_name_str();
        let create_sql = simplify_names(scx.catalog, rview.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&full_name),
                Datum::String(&create_sql),
            ])],
        }))
    } else {
        sql_bail!("{} is not a recorded view", name.full_name_str());
    }
}

pub fn describe_show_create_table(
    _: &StatementContext,
    _: ShowCreateTableStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Table", ScalarType::String.nullable(false))
            .with_column("Create Table", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_table(
    scx: &mut StatementContext,
    ShowCreateTableStatement { table_name }: ShowCreateTableStatement<Aug>,
) -> Result<Plan, PlanError> {
    let table = scx.get_item_by_resolved_name(&table_name)?;
    if let CatalogItemType::Table = table.item_type() {
        let name = table_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, table.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        }))
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
            .with_column("Source", ScalarType::String.nullable(false))
            .with_column("Create Source", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_source(
    scx: &StatementContext,
    ShowCreateSourceStatement { source_name }: ShowCreateSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let source = scx.get_item_by_resolved_name(&source_name)?;
    if let CatalogItemType::Source = source.item_type() {
        let name = source_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, source.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        }))
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
            .with_column("Sink", ScalarType::String.nullable(false))
            .with_column("Create Sink", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_sink(
    scx: &StatementContext,
    ShowCreateSinkStatement { sink_name }: ShowCreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let sink = scx.get_item_by_resolved_name(&sink_name)?;
    if let CatalogItemType::Sink = sink.item_type() {
        let name = sink_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, sink.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        }))
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
            .with_column("Index", ScalarType::String.nullable(false))
            .with_column("Create Index", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_index(
    scx: &StatementContext,
    ShowCreateIndexStatement { index_name }: ShowCreateIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let index = scx.get_item_by_resolved_name(&index_name)?;
    if let CatalogItemType::Index = index.item_type() {
        let name = index_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, index.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        }))
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
            .with_column("Connection", ScalarType::String.nullable(false))
            .with_column("Create Connection", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_connection(
    scx: &StatementContext,
    ShowCreateConnectionStatement { connection_name }: ShowCreateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let connection = scx.get_item_by_resolved_name(&connection_name)?;
    if let CatalogItemType::Connection = connection.item_type() {
        let name = connection_name.full_name_str();
        let create_sql = simplify_names(scx.catalog, connection.create_sql())?;
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&name),
                Datum::String(&create_sql),
            ])],
        }))
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
    ShowSchemasStatement {
        from,
        extended,
        full,
        filter,
    }: ShowSchemasStatement<Aug>,
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
    let query = if !full & !extended {
        format!(
            "SELECT name FROM mz_catalog.mz_schemas WHERE database_id = {}",
            &database_id,
        )
    } else if full & !extended {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {}",
            &database_id,
        )
    } else if !full & extended {
        format!(
            "SELECT name
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            &database_id,
        )
    } else {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            &database_id,
        )
    };
    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_objects<'a>(
    scx: &'a StatementContext<'a>,
    ShowObjectsStatement {
        extended,
        full,
        object_type,
        from,
        in_cluster,
        filter,
    }: ShowObjectsStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    match object_type {
        ObjectType::Table => show_tables(scx, extended, full, from, filter),
        ObjectType::Source => show_sources(scx, full, from, filter),
        ObjectType::View => show_views(scx, full, from, filter),
        ObjectType::RecordedView => show_recorded_views(scx, full, from, in_cluster, filter),
        ObjectType::Sink => show_sinks(scx, full, from, in_cluster, filter),
        ObjectType::Type => show_types(scx, extended, full, from, filter),
        ObjectType::Object => show_all_objects(scx, extended, full, from, filter),
        ObjectType::Role => bail_unsupported!("SHOW ROLES"),
        ObjectType::Cluster => show_clusters(scx, filter),
        ObjectType::ClusterReplica => show_cluster_replicas(scx, filter),
        ObjectType::Secret => show_secrets(scx, from, filter),
        ObjectType::Index => unreachable!("SHOW INDEX handled separately"),
        ObjectType::Connection => show_connections(scx, extended, full, from, filter),
    }
}

fn show_connections<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut query = format!(
        "SELECT t.name, mz_internal.mz_classify_object_id(t.id) AS type
        FROM mz_catalog.mz_connections t
        JOIN mz_catalog.mz_schemas s on t.schema_id = s.id
        WHERE schema_id = {}",
        schema_spec,
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let mut query = format!(
        "SELECT t.name, mz_internal.mz_classify_object_id(t.id) AS type
        FROM mz_catalog.mz_tables t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE schema_id = {}",
        schema_spec,
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_sources<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let query = if full {
        format!(
            "SELECT
                 name,
                 mz_internal.mz_classify_object_id(id) AS type,
                 type
             FROM mz_catalog.mz_sources
             WHERE schema_id = {schema_spec}"
        )
    } else {
        format!(
            "SELECT name
             FROM mz_catalog.mz_sources
             WHERE schema_id = {schema_spec}"
        )
    };

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_views<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let query = if full {
        format!(
            "SELECT
                name,
                mz_internal.mz_classify_object_id(id) AS type
             FROM mz_catalog.mz_views
             WHERE schema_id = {schema_spec}"
        )
    } else {
        format!(
            "SELECT name
            FROM mz_catalog.mz_views
            WHERE schema_id = {schema_spec}"
        )
    };

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_recorded_views<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;
    let mut where_clause = format!("schema_id = {schema_spec}");

    if let Some(cluster) = in_cluster {
        write!(where_clause, " AND cluster_id = {}", cluster.id)
            .expect("write on string cannot fail");
    }

    let query = if full {
        format!(
            "SELECT
                clusters.name AS cluster,
                rviews.name,
                mz_internal.mz_classify_object_id(rviews.id) AS type
             FROM mz_recorded_views AS rviews
             JOIN mz_clusters AS clusters
                ON clusters.id = rviews.cluster_id
             WHERE {where_clause}"
        )
    } else {
        format!(
            "SELECT name
             FROM mz_catalog.mz_recorded_views
             WHERE {where_clause}"
        )
    };

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_sinks<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ResolvedSchemaName>,
    in_cluster: Option<ResolvedClusterName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let mut query_filters = vec![];

    if let Some(ResolvedSchemaName::Schema { schema_spec, .. }) = from {
        query_filters.push(format!("schema_id = {}", schema_spec));
    } else if in_cluster.is_none() {
        query_filters.push(format!("schema_id = {}", scx.resolve_active_schema()?));
    };

    if let Some(cluster) = in_cluster {
        query_filters.push(format!("clusters.id = {}", cluster.id));
    }

    let query_filters = itertools::join(query_filters.iter(), " AND ");

    let query = if full {
        format!(
            "SELECT
            clusters.name AS cluster,
            sinks.name,
            mz_internal.mz_classify_object_id(sinks.id) AS type
        FROM
            mz_catalog.mz_sinks AS sinks
            JOIN mz_catalog.mz_clusters AS clusters ON
                    clusters.id = sinks.cluster_id
        WHERE {}",
            query_filters
        )
    } else {
        format!(
            "SELECT
            sinks.name
        FROM
            mz_catalog.mz_sinks AS sinks
            JOIN mz_catalog.mz_clusters AS clusters ON
                    clusters.id = sinks.cluster_id
        WHERE {}",
            query_filters
        )
    };
    ShowSelect::new(scx, query, filter, None, None)
}

fn show_types<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let mut query = format!(
        "SELECT t.name, mz_internal.mz_classify_object_id(t.id) AS type
        FROM mz_catalog.mz_types t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE t.schema_id = {}",
        schema_spec,
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    ShowSelect::new(scx, query, filter, None, None)
}

fn show_all_objects<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedSchemaName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, PlanError> {
    let schema_spec = scx.resolve_optional_schema(&from)?;

    let mut query = format!(
        "SELECT o.name, mz_internal.mz_classify_object_id(o.id) AS type
        FROM mz_catalog.mz_objects o
        JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
        WHERE o.schema_id = {}",
        schema_spec,
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_indexes<'a>(
    scx: &'a StatementContext<'a>,
    ShowIndexesStatement {
        extended,
        in_cluster,
        table_name,
        filter,
    }: ShowIndexesStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    // Exclude system indexes unless `EXTENDED` is requested.
    let mut query_filter = if extended {
        vec!["TRUE".into()]
    } else {
        vec!["idxs.on_id NOT LIKE 's%'".into()]
    };

    if let Some(table_name) = table_name {
        let from = scx.get_item_by_resolved_name(&table_name)?;
        if from.item_type() != CatalogItemType::View
            && from.item_type() != CatalogItemType::RecordedView
            && from.item_type() != CatalogItemType::Source
            && from.item_type() != CatalogItemType::Table
        {
            sql_bail!(
                "cannot show indexes on {} because it is a {}",
                table_name.full_name_str(),
                from.item_type(),
            );
        }
        query_filter.push(format!("objs.id = '{}'", from.id()));
    }

    if let Some(cluster) = in_cluster {
        query_filter.push(format!("clusters.id = {}", cluster.id))
    };

    let query = format!(
        "SELECT
            clusters.name AS cluster,
            objs.name AS on_name,
            idxs.name AS key_name,
            idx_cols.index_position AS seq_in_index,
            obj_cols.name AS column_name,
            idx_cols.on_expression AS expression,
            idx_cols.nullable AS nullable
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_index_columns AS idx_cols ON idxs.id = idx_cols.index_id
            JOIN mz_catalog.mz_objects AS objs ON idxs.on_id = objs.id
            JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = idxs.cluster_id
            LEFT JOIN mz_catalog.mz_columns AS obj_cols
                ON idxs.on_id = obj_cols.id AND idx_cols.on_position = obj_cols.position
        WHERE
            {}",
        itertools::join(query_filter.iter(), " AND ")
    );

    ShowSelect::new(scx, query, filter, None, None)
}

pub fn show_columns<'a>(
    scx: &'a StatementContext<'a>,
    ShowColumnsStatement {
        extended,
        full,
        table_name,
        filter,
    }: ShowColumnsStatement<Aug>,
) -> Result<ShowSelect<'a>, PlanError> {
    if extended {
        bail_unsupported!("SHOW EXTENDED COLUMNS");
    }
    if full {
        bail_unsupported!("SHOW FULL COLUMNS");
    }

    let entry = scx.get_item_by_resolved_name(&table_name)?;

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
        WHERE schema_id = {}",
        schema_spec,
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
}

fn simplify_names(catalog: &dyn SessionCatalog, sql: &str) -> Result<String, PlanError> {
    let parsed = parse::parse(sql)?.into_element();
    let (mut resolved, _) = names::resolve(catalog, parsed)?;
    let mut simplifier = NameSimplifier { catalog };
    simplifier.visit_statement_mut(&mut resolved);
    Ok(resolved.to_ast_string_stable())
}
