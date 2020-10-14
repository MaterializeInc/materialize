// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handlers for `SHOW ...` queries.

use anyhow::bail;

use ore::collections::CollectionExt;
use repr::{Datum, RelationDesc, Row, ScalarType};
use sql_parser::ast::{
    ObjectName, ObjectType, SelectStatement, ShowColumnsStatement, ShowCreateIndexStatement,
    ShowCreateSinkStatement, ShowCreateSourceStatement, ShowCreateTableStatement,
    ShowCreateViewStatement, ShowDatabasesStatement, ShowIndexesStatement, ShowObjectsStatement,
    ShowStatementFilter, Statement, Value,
};

use crate::catalog::CatalogItemType;
use crate::parse;
use crate::plan::statement::StatementContext;
use crate::plan::{Params, Plan};

pub fn handle_show_create_view(
    scx: &StatementContext,
    ShowCreateViewStatement { view_name }: ShowCreateViewStatement,
) -> Result<Plan, anyhow::Error> {
    let name = scx.resolve_item(view_name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::View = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("{} is not a view", name);
    }
}

pub fn handle_show_create_table(
    scx: &StatementContext,
    ShowCreateTableStatement { table_name }: ShowCreateTableStatement,
) -> Result<Plan, anyhow::Error> {
    let name = scx.resolve_item(table_name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Table = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("{} is not a table", name);
    }
}

pub fn handle_show_create_source(
    scx: &StatementContext,
    ShowCreateSourceStatement { source_name }: ShowCreateSourceStatement,
) -> Result<Plan, anyhow::Error> {
    let name = scx.resolve_item(source_name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Source = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("{} is not a source", name);
    }
}

pub fn handle_show_create_sink(
    scx: &StatementContext,
    ShowCreateSinkStatement { sink_name }: ShowCreateSinkStatement,
) -> Result<Plan, anyhow::Error> {
    let name = scx.resolve_item(sink_name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Sink = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("'{}' is not a sink", name);
    }
}

pub fn handle_show_create_index(
    scx: &StatementContext,
    ShowCreateIndexStatement { index_name }: ShowCreateIndexStatement,
) -> Result<Plan, anyhow::Error> {
    let name = scx.resolve_item(index_name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Index = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("'{}' is not an index", name);
    }
}

pub fn show_databases<'a>(
    scx: &'a StatementContext<'a>,
    ShowDatabasesStatement { filter }: ShowDatabasesStatement,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let query = "SELECT name FROM mz_catalog.mz_databases".to_string();
    Ok(ShowSelect::new(scx, query, filter))
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
    }: ShowObjectsStatement,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    match object_type {
        ObjectType::Schema => show_schemas(scx, extended, full, from, filter),
        ObjectType::Table => show_tables(scx, extended, full, from, filter),
        ObjectType::Source => show_sources(scx, full, materialized, from, filter),
        ObjectType::View => show_views(scx, full, materialized, from, filter),
        ObjectType::Sink => show_sinks(scx, full, from, filter),
        ObjectType::Index => unreachable!("SHOW INDEX handled separately"),
    }
}

fn show_schemas<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let (_database_name, database_id) = if let Some(from) = from {
        scx.resolve_database(from)?
    } else {
        scx.resolve_default_database()?
    };
    let query = if !full & !extended {
        format!(
            "SELECT name FROM mz_catalog.mz_schemas WHERE database_id = {}",
            database_id,
        )
    } else if full & !extended {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {}",
            database_id,
        )
    } else if !full & extended {
        format!(
            "SELECT name
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            database_id,
        )
    } else {
        format!(
            "SELECT name, CASE WHEN database_id IS NULL THEN 'system' ELSE 'user' END AS type
            FROM mz_catalog.mz_schemas
            WHERE database_id = {} OR database_id IS NULL",
            database_id,
        )
    };
    Ok(ShowSelect::new(scx, query, filter))
}

fn show_tables<'a>(
    scx: &'a StatementContext<'a>,
    extended: bool,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    if extended {
        unsupported!("SHOW EXTENDED TABLES");
    }

    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };

    let query = if full {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(global_id) AS type
            FROM mz_catalog.mz_tables
            WHERE schema_id = {}",
            schema_spec.id,
        )
    } else {
        format!(
            "SELECT name FROM mz_catalog.mz_tables WHERE schema_id = {}",
            schema_spec.id
        )
    };
    Ok(ShowSelect::new(scx, query, filter))
}

fn show_sources<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    materialized: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };

    let query = if !full & !materialized {
        format!(
            "SELECT name FROM mz_catalog.mz_sources WHERE schema_id = {}",
            schema_spec.id,
        )
    } else if full & !materialized {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(global_id) AS type,
                CASE WHEN count > 0 then true ELSE false END materialized
            FROM mz_catalog.mz_sources
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {}",
            schema_spec.id,
        )
    } else if !full & materialized {
        format!(
            "SELECT name
            FROM mz_catalog.mz_sources
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {} AND mz_indexes_count.count > 0",
            schema_spec.id,
        )
    } else {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(global_id) AS type,
            FROM mz_catalog.mz_sources
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {} AND mz_indexes_count.count > 0",
            schema_spec.id,
        )
    };
    Ok(ShowSelect::new(scx, query, filter))
}

fn show_views<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    materialized: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };

    let query = if !full & !materialized {
        format!(
            "SELECT name
             FROM mz_catalog.mz_views
             WHERE mz_catalog.mz_views.schema_id = {}",
            schema_spec.id,
        )
    } else if full & !materialized {
        format!(
            "SELECT
                name,
                mz_internal.mz_classify_object_id(global_id) AS type,
                count > 0 as materialized
             FROM mz_catalog.mz_views as mz_views
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {}",
            schema_spec.id,
        )
    } else if !full & materialized {
        format!(
            "SELECT name
             FROM mz_catalog.mz_views
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {}
                AND mz_indexes_count.count > 0",
            schema_spec.id,
        )
    } else {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(global_id) AS type
             FROM mz_catalog.mz_views
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {}
                AND mz_indexes_count.count > 0",
            schema_spec.id,
        )
    };
    Ok(ShowSelect::new(scx, query, filter))
}

fn show_sinks<'a>(
    scx: &'a StatementContext<'a>,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };

    let query = if full {
        format!(
            "SELECT name, mz_internal.mz_classify_object_id(global_id) AS type
            FROM mz_catalog.mz_sinks
            WHERE schema_id = {}",
            schema_spec.id,
        )
    } else {
        format!(
            "SELECT name FROM mz_catalog.mz_sinks WHERE schema_id = {}",
            schema_spec.id,
        )
    };
    Ok(ShowSelect::new(scx, query, filter))
}

pub fn show_indexes<'a>(
    scx: &'a StatementContext<'a>,
    ShowIndexesStatement {
        extended,
        table_name,
        filter,
    }: ShowIndexesStatement,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    if extended {
        unsupported!("SHOW EXTENDED INDEXES")
    }
    let from_name = scx.resolve_item(table_name)?;
    let from_entry = scx.catalog.get_item(&from_name);
    if from_entry.item_type() != CatalogItemType::View
        && from_entry.item_type() != CatalogItemType::Source
        && from_entry.item_type() != CatalogItemType::Table
    {
        bail!(
            "cannot show indexes on {} because it is a {}",
            from_name,
            from_entry.item_type(),
        );
    }

    let query = format!(
        "SELECT
            objs.name AS on_name,
            idxs.name AS key_name,
            idxs.seq_in_index AS seq_in_index,
            cols.name AS column_name,
            idxs.expression AS expression,
            idxs.nullable AS nullable
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_objects AS objs ON idxs.on_global_id = objs.global_id
            LEFT JOIN mz_catalog.mz_columns AS cols
                ON idxs.on_global_id = cols.global_id AND idxs.field_number = cols.field_number
        WHERE
            objs.global_id = '{}'",
        from_entry.id(),
    );
    Ok(ShowSelect::new(scx, query, filter))
}

pub fn show_columns<'a>(
    scx: &'a StatementContext<'a>,
    ShowColumnsStatement {
        extended,
        full,
        table_name,
        filter,
    }: ShowColumnsStatement,
) -> Result<ShowSelect<'a>, anyhow::Error> {
    if extended {
        unsupported!("SHOW EXTENDED COLUMNS");
    }
    if full {
        unsupported!("SHOW FULL COLUMNS");
    }

    let name = scx.resolve_item(table_name)?;
    let entry = scx.catalog.get_item(&name);

    let query = format!(
        "SELECT
            mz_columns.name,
            mz_columns.nullable,
            mz_columns.type
         FROM mz_catalog.mz_columns AS mz_columns
         WHERE mz_columns.global_id = '{}'",
        entry.id(),
    );
    Ok(ShowSelect::new(scx, query, filter))
}

/// An intermediate result when planning a `SHOW` query.
///
/// Can be interrogated for its columns, or converted into a proper [`Plan`].
pub struct ShowSelect<'a> {
    scx: &'a StatementContext<'a>,
    stmt: SelectStatement,
}

impl<'a> ShowSelect<'a> {
    /// Constructs a new [`ShowSelect`] from a query that provides the base
    /// data and an optional user-supplied filter on that data.
    ///
    /// Note that the query must return a column named `name`, as the filter
    /// may implicitly reference this column. Any `ORDER BY` in the query is
    /// ignored. `ShowSelects`s are always ordered in ascending order by all
    /// columns from left to right.
    fn new(
        scx: &'a StatementContext,
        query: String,
        filter: Option<ShowStatementFilter>,
    ) -> ShowSelect<'a> {
        let filter = match filter {
            Some(ShowStatementFilter::Like(like)) => format!("name LIKE {}", Value::String(like)),
            Some(ShowStatementFilter::Where(expr)) => expr.to_string(),
            None => "true".to_string(),
        };
        let query = format!("SELECT * FROM ({}) q WHERE {} ORDER BY q.*", query, filter);
        let stmts = parse::parse(query).expect("ShowSelect::new called with invalid SQL");
        let stmt = match stmts.into_element() {
            Statement::Select(select) => select,
            _ => panic!("ShowSelect::new called with non-SELECT statement"),
        };
        ShowSelect { scx, stmt }
    }

    /// Computes the shape of this `ShowSelect`.
    pub fn describe(self) -> Result<(Option<RelationDesc>, Vec<ScalarType>), anyhow::Error> {
        super::describe_statement(self.scx.catalog, Statement::Select(self.stmt), &[])
    }

    /// Converts this `ShowSelect` into a [`Plan`].
    pub fn handle(self) -> Result<Plan, anyhow::Error> {
        super::handle_select(self.scx, self.stmt, &Params::empty())
    }
}
