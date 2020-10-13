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
use repr::{Datum, Row};
use sql_parser::ast::{
    ObjectName, ObjectType, ShowColumnsStatement, ShowCreateIndexStatement,
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

pub fn handle_show_databases(
    scx: &StatementContext,
    ShowDatabasesStatement { filter }: ShowDatabasesStatement,
) -> Result<Plan, anyhow::Error> {
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("database LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => expr.to_string(),
        None => "true".to_owned(),
    };
    handle_generated_select(
        scx,
        format!(
            "SELECT database FROM mz_catalog.mz_databases WHERE {}",
            filter
        ),
    )
}

pub fn handle_show_objects(
    scx: &StatementContext,
    ShowObjectsStatement {
        extended,
        full,
        materialized,
        object_type,
        from,
        filter,
    }: ShowObjectsStatement,
) -> Result<Plan, anyhow::Error> {
    match object_type {
        ObjectType::Schema => handle_show_schemas(scx, extended, full, from, filter),
        ObjectType::Table => handle_show_tables(scx, extended, full, from, filter),
        ObjectType::Source => handle_show_sources(scx, full, materialized, from, filter),
        ObjectType::View => handle_show_views(scx, full, materialized, from, filter),
        ObjectType::Sink => handle_show_sinks(scx, full, from, filter),
        ObjectType::Index => unreachable!("SHOW INDEX handled separately"),
    }
}

fn handle_show_schemas(
    scx: &StatementContext,
    extended: bool,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, anyhow::Error> {
    let database_name = if let Some(from) = from {
        scx.resolve_database(from)?
    } else {
        scx.resolve_default_database()?.to_string()
    };
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("AND schema LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };

    let query = if !full & !extended {
        format!(
            "SELECT schema
            FROM mz_catalog.mz_schemas
            JOIN mz_catalog.mz_databases ON mz_catalog.mz_schemas.database_id = mz_catalog.mz_databases.id
            WHERE mz_catalog.mz_databases.database = '{}' {}",
            database_name, filter
        )
    } else if full & !extended {
        format!(
            "SELECT schema, type
            FROM mz_catalog.mz_schemas
            JOIN mz_catalog.mz_databases ON mz_catalog.mz_schemas.database_id = mz_catalog.mz_databases.id
            WHERE mz_catalog.mz_databases.database = '{}' {}",
            database_name, filter
        )
    } else if !full & extended {
        format!(
            "SELECT schema
            FROM mz_catalog.mz_schemas
            LEFT JOIN mz_catalog.mz_databases ON mz_catalog.mz_schemas.database_id = mz_catalog.mz_databases.id
            WHERE mz_catalog.mz_databases.database = '{}' OR mz_catalog.mz_databases.database IS NULL {}",
            database_name, filter
        )
    } else {
        format!(
            "SELECT schema, type
            FROM mz_catalog.mz_schemas
            LEFT JOIN mz_catalog.mz_databases ON mz_catalog.mz_schemas.database_id = mz_catalog.mz_databases.id
            WHERE mz_catalog.mz_databases.database = '{}' OR mz_catalog.mz_databases.database IS NULL {}",
            database_name, filter
        )
    };
    handle_generated_select(scx, query)
}

fn handle_show_tables(
    scx: &StatementContext,
    extended: bool,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, anyhow::Error> {
    if extended {
        unsupported!("SHOW EXTENDED TABLES");
    }

    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("AND tables LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };

    let query = if full {
        format!(
            "SELECT tables, type
            FROM mz_catalog.mz_tables
            JOIN mz_catalog.mz_schemas ON mz_catalog.mz_tables.schema_id = mz_catalog.mz_schemas.schema_id
            WHERE schema_id = {} {}
            ORDER BY tables, type",
            schema_spec.id, filter
        )
    } else {
        format!(
            "SELECT tables FROM mz_catalog.mz_tables WHERE schema_id = {} {} ORDER BY tables",
            schema_spec.id, filter
        )
    };
    handle_generated_select(scx, query)
}

fn handle_show_sources(
    scx: &StatementContext,
    full: bool,
    materialized: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => {
            format!("AND sources LIKE {}", Value::String(like))
        }
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };

    let query = if !full & !materialized {
        format!(
            "SELECT sources FROM mz_catalog.mz_sources WHERE schema_id = {} {} ORDER BY sources",
            schema_spec.id, filter
        )
    } else if full & !materialized {
        format!(
            "SELECT sources, type, CASE WHEN count > 0 then true ELSE false END materialized
            FROM mz_catalog.mz_sources
            JOIN mz_catalog.mz_schemas ON mz_catalog.mz_sources.schema_id = mz_catalog.mz_schemas.schema_id
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {} {}
            ORDER BY sources, type",
            schema_spec.id, filter
        )
    } else if !full & materialized {
        format!(
            "SELECT sources
            FROM mz_catalog.mz_sources
            JOIN mz_catalog.mz_schemas ON mz_catalog.mz_sources.schema_id = mz_catalog.mz_schemas.schema_id
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {} {} AND mz_indexes_count.count > 0
            ORDER BY sources, type",
            schema_spec.id, filter
        )
    } else {
        format!(
            "SELECT sources, type
            FROM mz_catalog.mz_sources
            JOIN mz_catalog.mz_schemas ON mz_catalog.mz_sources.schema_id = mz_catalog.mz_schemas.schema_id
            JOIN (SELECT mz_catalog.mz_sources.global_id as global_id, count(mz_catalog.mz_indexes.on_global_id) AS count
                  FROM mz_catalog.mz_sources
                  LEFT JOIN mz_catalog.mz_indexes on mz_catalog.mz_sources.global_id = mz_catalog.mz_indexes.on_global_id
                  GROUP BY mz_catalog.mz_sources.global_id) as mz_indexes_count
                ON mz_catalog.mz_sources.global_id = mz_indexes_count.global_id
            WHERE schema_id = {} {} AND mz_indexes_count.count > 0
            ORDER BY sources, type",
            schema_spec.id, filter
        )
    };
    handle_generated_select(scx, query)
}

fn handle_show_views(
    scx: &StatementContext,
    full: bool,
    materialized: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("AND views LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };

    let query = if !full & !materialized {
        format!(
            "SELECT views
             FROM mz_catalog.mz_views
             WHERE mz_catalog.mz_views.schema_id = {} {}
             ORDER BY views ASC",
            schema_spec.id, filter
        )
    } else if full & !materialized {
        format!(
            "SELECT
                views,
                type,
                count > 0 as materialized
             FROM mz_catalog.mz_views as mz_views
             JOIN mz_catalog.mz_schemas ON mz_catalog.mz_views.schema_id = mz_catalog.mz_schemas.schema_id
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {} {}
             ORDER BY views ASC",
            schema_spec.id, filter
        )
    } else if !full & materialized {
        format!(
            "SELECT views
             FROM mz_catalog.mz_views
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {}
                AND mz_indexes_count.count > 0 {}
             ORDER BY views ASC",
            schema_spec.id, filter
        )
    } else {
        format!(
            "SELECT views, type
             FROM mz_catalog.mz_views
             JOIN mz_catalog.mz_schemas ON mz_catalog.mz_views.schema_id = mz_catalog.mz_schemas.schema_id
             JOIN (SELECT mz_views.global_id as global_id, count(mz_indexes.on_global_id) AS count
                   FROM mz_views
                   LEFT JOIN mz_indexes on mz_views.global_id = mz_indexes.on_global_id
                   GROUP BY mz_views.global_id) as mz_indexes_count
                ON mz_views.global_id = mz_indexes_count.global_id
             WHERE mz_catalog.mz_views.schema_id = {}
                AND mz_indexes_count.count > 0 {}
             ORDER BY views ASC",
            schema_spec.id, filter
        )
    };
    handle_generated_select(scx, query)
}

fn handle_show_sinks(
    scx: &StatementContext,
    full: bool,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, anyhow::Error> {
    let schema_spec = if let Some(from) = from {
        scx.resolve_schema(from)?.1
    } else {
        scx.resolve_default_schema()?
    };
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("AND sinks LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };

    let query = if full {
        format!(
            "SELECT sinks, type
            FROM mz_catalog.mz_sinks
            JOIN mz_catalog.mz_schemas ON mz_catalog.mz_sinks.schema_id = mz_catalog.mz_schemas.schema_id
            WHERE schema_id = {} {}
            ORDER BY sinks, type",
            schema_spec.id, filter
        )
    } else {
        format!(
            "SELECT sinks FROM mz_catalog.mz_sinks WHERE schema_id = {} {} ORDER BY sinks",
            schema_spec.id, filter
        )
    };
    handle_generated_select(scx, query)
}

pub fn handle_show_indexes(
    scx: &StatementContext,
    ShowIndexesStatement {
        extended,
        table_name,
        filter,
    }: ShowIndexesStatement,
) -> Result<Plan, anyhow::Error> {
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

    let base_query = format!(
        "SELECT
            objs.name AS on_name,
            idxs.indexes AS key_name,
            cols.field AS column_name,
            idxs.expression AS expression,
            idxs.nullable AS nullable,
            idxs.seq_in_index AS seq_in_index
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_objects AS objs ON idxs.on_global_id = objs.global_id
            LEFT JOIN mz_catalog.mz_columns AS cols
                ON idxs.on_global_id = cols.global_id AND idxs.field_number = cols.field_number
        WHERE
            objs.global_id = '{}'
        ORDER BY
            key_name ASC,
            seq_in_index ASC",
        from_entry.id(),
    );

    let query = if let Some(filter) = filter {
        let filter = match filter {
            ShowStatementFilter::Like(like) => format!("key_name LIKE {}", Value::String(like)),
            ShowStatementFilter::Where(expr) => expr.to_string(),
        };
        format!(
            "SELECT on_name, key_name, column_name, expression, nullable, seq_in_index
             FROM ({})
             WHERE {}",
            base_query, filter,
        )
    } else {
        base_query
    };
    handle_generated_select(scx, query)
}

pub fn handle_show_columns(
    scx: &StatementContext,
    ShowColumnsStatement {
        extended,
        full,
        table_name,
        filter,
    }: ShowColumnsStatement,
) -> Result<Plan, anyhow::Error> {
    if extended {
        unsupported!("SHOW EXTENDED COLUMNS");
    }
    if full {
        unsupported!("SHOW FULL COLUMNS");
    }

    let name = scx.resolve_item(table_name)?;
    let filter = match filter {
        Some(ShowStatementFilter::Like(like)) => format!("AND field LIKE {}", Value::String(like)),
        Some(ShowStatementFilter::Where(expr)) => format!("AND {}", expr.to_string()),
        None => "".to_owned(),
    };
    let query = format!(
        "SELECT
            mz_columns.field,
            CASE WHEN mz_columns.nullable THEN 'YES' ELSE 'NO' END nullable,
            mz_columns.type
         FROM mz_catalog.mz_columns AS mz_columns
         JOIN mz_catalog.mz_catalog_names AS mz_catalog_names ON mz_columns.global_id = mz_catalog_names.global_id
         WHERE mz_catalog_names.name = '{}' {}
         ORDER BY mz_columns.field_number ASC",
        name, filter
    );
    handle_generated_select(scx, query)
}

fn handle_generated_select(scx: &StatementContext, query: String) -> Result<Plan, anyhow::Error> {
    match parse::parse(query)?.into_element() {
        Statement::Select(select) => super::handle_select(
            scx,
            select,
            &Params {
                datums: Row::pack(&[]),
                types: vec![],
            },
        ),
        _ => unreachable!("known to be select statement"),
    }
}
