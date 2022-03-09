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

use anyhow::bail;

use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, RelationDesc, Row, ScalarType};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::Ident;

use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    ObjectType, Raw, SelectStatement, ShowColumnsStatement, ShowCreateIndexStatement,
    ShowCreateSinkStatement, ShowCreateSourceStatement, ShowCreateTableStatement,
    ShowCreateViewStatement, ShowDatabasesStatement, ShowIndexesStatement, ShowObjectsStatement,
    ShowStatementFilter, Statement, Value,
};
use crate::catalog::CatalogItemType;
use crate::names::{resolve_names_stmt, Aug, BoundStatement, NameSimplifier, ResolvedObjectName};
use crate::parse;
use crate::plan::statement::{dml, StatementContext, StatementDesc};
use crate::plan::{Params, Plan, SendRowsPlan};

pub fn describe_show_create_view(
    _: &StatementContext,
    _: ShowCreateViewStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("View", ScalarType::String.nullable(false))
            .with_column("Create View", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_view(
    scx: &StatementContext,
    ShowCreateViewStatement { view_name }: ShowCreateViewStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let view = scx.get_item_by_name(&view_name)?;
    let view_sql = view.create_sql();

    let parsed = parse::parse(view_sql)?;
    let parsed = parsed[0].clone();
    let mut resolved = resolve_names_stmt(scx.catalog, parsed)?;
    let mut s = NameSimplifier {
        catalog: scx.catalog,
    };
    s.visit_statement_mut(&mut resolved.statement);

    if let CatalogItemType::View = view.item_type() {
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&view.name().to_string()),
                Datum::String(&resolved.statement.to_ast_string_stable()),
            ])],
        }))
    } else {
        bail!("{} is not a view", view.name());
    }
}

pub fn describe_show_create_table(
    _: &StatementContext,
    _: ShowCreateTableStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Table", ScalarType::String.nullable(false))
            .with_column("Create Table", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_table(
    scx: &StatementContext,
    ShowCreateTableStatement { table_name }: ShowCreateTableStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let table = scx.get_item_by_name(&table_name)?;
    if let CatalogItemType::Table = table.item_type() {
        let parsed = parse::parse(table.create_sql())?.into_element();
        let mut resolved = resolve_names_stmt(scx.catalog, parsed)?;
        let mut s = NameSimplifier {
            catalog: scx.catalog,
        };
        s.visit_statement_mut(&mut resolved.statement);
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&table.name().to_string()),
                Datum::String(&resolved.statement.to_ast_string_stable()),
            ])],
        }))
    } else {
        bail!("{} is not a table", table.name());
    }
}

pub fn describe_show_create_source(
    _: &StatementContext,
    _: ShowCreateSourceStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Source", ScalarType::String.nullable(false))
            .with_column("Create Source", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_source(
    scx: &StatementContext,
    ShowCreateSourceStatement { source_name }: ShowCreateSourceStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let source = scx.get_item_by_name(&source_name)?;
    if let CatalogItemType::Source = source.item_type() {
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&source.name().to_string()),
                Datum::String(source.create_sql()),
            ])],
        }))
    } else {
        bail!("{} is not a source", source.name());
    }
}

pub fn describe_show_create_sink(
    _: &StatementContext,
    _: ShowCreateSinkStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Sink", ScalarType::String.nullable(false))
            .with_column("Create Sink", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_sink(
    scx: &StatementContext,
    ShowCreateSinkStatement { sink_name }: ShowCreateSinkStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let sink = scx.get_item_by_name(&sink_name)?;
    if let CatalogItemType::Sink = sink.item_type() {
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&sink.name().to_string()),
                Datum::String(sink.create_sql()),
            ])],
        }))
    } else {
        bail!("'{}' is not a sink", sink.name());
    }
}

pub fn describe_show_create_index(
    _: &StatementContext,
    _: ShowCreateIndexStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(
        RelationDesc::empty()
            .with_column("Index", ScalarType::String.nullable(false))
            .with_column("Create Index", ScalarType::String.nullable(false)),
    )))
}

pub fn plan_show_create_index(
    scx: &StatementContext,
    ShowCreateIndexStatement { index_name }: ShowCreateIndexStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let index = scx.get_item_by_name(&index_name)?;
    if let CatalogItemType::Index = index.item_type() {
        Ok(Plan::SendRows(SendRowsPlan {
            rows: vec![Row::pack_slice(&[
                Datum::String(&index.name().to_string()),
                Datum::String(index.create_sql()),
            ])],
        }))
    } else {
        bail!("'{}' is not an index", index.name());
    }
}

pub fn show_databases<'a>(
    scx: &'a StatementContext<'a>,
    ShowDatabasesStatement { filter }: ShowDatabasesStatement<Aug>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let query = "SELECT name FROM mz_catalog.mz_databases".to_string();
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

pub fn show_objects<'a>(
    scx: &'a StatementContext<'a>,
    ShowObjectsStatement {
        extended,
        full,
        materialized,
        object_type,
        from,
        filter,
    }: ShowObjectsStatement<Aug>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    match object_type {
        ObjectType::Schema => show_schemas(scx, extended, full, from, filter),
        ObjectType::Table => show_tables(scx, extended, full, from, filter),
        ObjectType::Source => show_sources(scx, full, materialized, from, filter),
        ObjectType::View => show_views(scx, full, materialized, from, filter),
        ObjectType::Sink => show_sinks(scx, full, from, filter),
        ObjectType::Type => show_types(scx, extended, full, from, filter),
        ObjectType::Object => show_all_objects(scx, extended, full, from, filter),
        ObjectType::Role => bail_unsupported!("SHOW ROLES"),
        ObjectType::Cluster => bail_unsupported!("SHOW CLUSTERS"),
        ObjectType::Secret => bail_unsupported!("SHOW SECRETS"),
        ObjectType::Index => unreachable!("SHOW INDEX handled separately"),
    }
}

fn show_schemas<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let database = if let Some(from) = from {
        // TODO(jkosh44) Resolve by ID
        scx.resolve_database_ident(Ident::from(from.raw_name.to_string()))?
    } else {
        scx.resolve_active_database()?
    };
    let query = if !full & !extended {
        format!(
            "SELECT name FROM mz_catalog.mz_schemas WHERE database_id = {}",
            database.id(),
        )
    } else if full & !extended {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {}",
            database.id(),
        )
    } else if !full & extended {
        format!(
            "SELECT name
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            database.id(),
        )
    } else {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            database.id(),
        )
    };
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let mut query = format!(
        "SELECT t.name, mz_internal.mz_classify_object_id(t.id) AS type
        FROM mz_catalog.mz_tables t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE schema_id = {}",
        schema.id(),
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_sources<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    materialized: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let query = match (full, materialized) {
        (false, false) => format!(
            "SELECT
                 name
             FROM
                 mz_catalog.mz_sources
             WHERE
                 schema_id = {}",
            schema.id()
        ),
        (false, true) => format!(
            "SELECT
                 name
             FROM mz_catalog.mz_sources
             WHERE
                 mz_internal.mz_is_materialized(id) AND
                 schema_id = {}",
            schema.id()
        ),
        (true, false) => format!(
            "SELECT
                 name,
                 mz_internal.mz_classify_object_id(id) AS type,
                 mz_internal.mz_is_materialized(id) AS materialized,
                 volatility,
                 connector_type
             FROM
                 mz_catalog.mz_sources
             WHERE
                 schema_id = {}",
            schema.id()
        ),
        (true, true) => format!(
            "SELECT
                 name,
                 mz_internal.mz_classify_object_id(id) AS type,
                 volatility,
                 connector_type
             FROM
                 mz_catalog.mz_sources
             WHERE
                  mz_internal.mz_is_materialized(id) AND
                  schema_id = {}",
            schema.id()
        ),
    };
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_views<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    materialized: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let query = if !full & !materialized {
        format!(
            "SELECT name FROM mz_catalog.mz_views WHERE schema_id = {}",
            schema.id(),
        )
    } else if full & !materialized {
        format!(
            "SELECT
                name,
                mz_internal.mz_classify_object_id(id) AS type,
                mz_internal.mz_is_materialized(id) AS materialized,
                volatility
             FROM mz_catalog.mz_views
             WHERE schema_id = {}",
            schema.id(),
        )
    } else if !full & materialized {
        format!(
            "SELECT name
             FROM mz_catalog.mz_views
             WHERE schema_id = {} AND mz_internal.mz_is_materialized(id)",
            schema.id(),
        )
    } else {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(id) AS type, volatility
             FROM mz_catalog.mz_views
             WHERE schema_id = {} AND mz_internal.mz_is_materialized(id)",
            schema.id(),
        )
    };
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_sinks<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let query = if full {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(id) AS type, volatility
            FROM mz_catalog.mz_sinks
            WHERE schema_id = {}",
            schema.id(),
        )
    } else {
        format!(
            "SELECT name FROM mz_catalog.mz_sinks WHERE schema_id = {}",
            schema.id(),
        )
    };
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_types<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let mut query = format!(
        "SELECT t.name, mz_internal.mz_classify_object_id(t.id) AS type
        FROM mz_catalog.mz_types t
        JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
        WHERE t.schema_id = {}",
        schema.id(),
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    Ok(ShowSelect::new(scx, query, filter, None, None))
}

fn show_all_objects<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ResolvedObjectName>,
    filter: Option<ShowStatementFilter<Aug>>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema = if let Some(from) = from {
        // TODO(jkosh44) Resolve from id
        scx.resolve_schema_ident(
            from.raw_name.database.map(Ident::from),
            Ident::from(from.raw_name.schema.unwrap()),
        )?
    } else {
        scx.resolve_active_schema()?
    };

    let mut query = format!(
        "SELECT o.name, mz_internal.mz_classify_object_id(o.id) AS type
        FROM mz_catalog.mz_objects o
        JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
        WHERE o.schema_id = {}",
        schema.id(),
    );
    if extended {
        query += " OR s.database_id IS NULL";
    }
    if !full {
        query = format!("SELECT name FROM ({})", query);
    }

    Ok(ShowSelect::new(scx, query, filter, None, None))
}

pub fn show_indexes<'a>(
    scx: &'a StatementContext<'a>,
    ShowIndexesStatement {
        extended,
        table_name,
        filter,
    }: ShowIndexesStatement<Aug>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    if extended {
        bail_unsupported!("SHOW EXTENDED INDEXES")
    }
    let from = scx.get_item_by_name(&table_name)?;
    if from.item_type() != CatalogItemType::View
        && from.item_type() != CatalogItemType::Source
        && from.item_type() != CatalogItemType::Table
    {
        bail!(
            "cannot show indexes on {} because it is a {}",
            from.name(),
            from.item_type(),
        );
    }

    let query = format!(
        "SELECT
            objs.name AS on_name,
            idxs.name AS key_name,
            idx_cols.index_position AS seq_in_index,
            obj_cols.name AS column_name,
            idx_cols.on_expression AS expression,
            idx_cols.nullable AS nullable,
            idxs.enabled AS enabled
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_index_columns AS idx_cols ON idxs.id = idx_cols.index_id
            JOIN mz_catalog.mz_objects AS objs ON idxs.on_id = objs.id
            LEFT JOIN mz_catalog.mz_columns AS obj_cols
                ON idxs.on_id = obj_cols.id AND idx_cols.on_position = obj_cols.position
        WHERE
            objs.id = '{}'",
        from.id(),
    );
    Ok(ShowSelect::new(scx, query, filter, None, None))
}

pub fn show_columns<'a>(
    scx: &'a StatementContext<'a>,
    ShowColumnsStatement {
        extended,
        full,
        table_name,
        filter,
    }: ShowColumnsStatement<Aug>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    if extended {
        bail_unsupported!("SHOW EXTENDED COLUMNS");
    }
    if full {
        bail_unsupported!("SHOW FULL COLUMNS");
    }

    let entry = scx.get_item_by_name(&table_name)?;

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
    Ok(ShowSelect::new(
        scx,
        query,
        filter,
        Some("position"),
        Some(&["name", "nullable", "type"]),
    ))
}

/// An intermediate result when planning a `SHOW` query.
///
/// Can be interrogated for its columns, or converted into a proper [`Plan`].
pub struct ShowSelect<'a> {
    scx: &'a StatementContext<'a>,
    stmt: SelectStatement<Raw>,
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
    ) -> ShowSelect<'a> {
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
        ShowSelect { scx, stmt }
    }

    /// Computes the shape of this `ShowSelect`.
    pub fn describe(self) -> Result<StatementDesc, anyhow::Error> {
        dml::describe_select(self.scx, self.stmt)
    }

    /// Converts this `ShowSelect` into a [`Plan`].
    pub fn plan(self) -> Result<Plan, anyhow::Error> {
        // TODO(jkosh44) This pattern is super ugly and keeps popping up. FIX IT!
        let BoundStatement { statement, .. } =
            resolve_names_stmt(self.scx.catalog, Statement::Select(self.stmt))?;
        if let Statement::Select(stmt) = statement {
            return dml::plan_select(self.scx, stmt, &Params::empty(), None);
        }
        unreachable!()
    }
}
