// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_pgrepr::Type;
use mz_sql_parser::ast::{Ident, UnresolvedItemName};
use postgres_protocol::escape;
use tokio_postgres::Client;

use crate::destination::{config, ColumnMetadata, FIVETRAN_SYSTEM_COLUMN_DELETE};
use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::{
    AlterTableRequest, Column, CreateTableRequest, DataType, DescribeTableRequest, Table,
};
use crate::utils;

/// HACK(parkmycar): An ugly hack to track whether or not a column is a primary key.
const PRIMARY_KEY_MAGIC_STRING: &str = "mz_is_primary_key";

pub async fn handle_describe_table(
    request: DescribeTableRequest,
) -> Result<Option<Table>, OpError> {
    let (dbname, client) = config::connect(request.configuration).await?;
    describe_table(&client, &dbname, &request.schema_name, &request.table_name).await
}

pub async fn describe_table(
    client: &Client,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Table>, OpError> {
    let table_id = {
        let rows = client
            .query(
                r#"SELECT t.id
                   FROM mz_tables t
                   JOIN mz_schemas s ON s.id = t.schema_id
                   JOIN mz_databases d ON d.id = s.database_id
                   WHERE d.name = $1 AND s.name = $2 AND t.name = $3
                "#,
                &[&database, &schema, &table],
            )
            .await
            .context("fetching table ID")?;

        match &*rows {
            [] => return Ok(None),
            [row] => row.get::<_, String>("id"),
            _ => {
                let err = OpErrorKind::InvariantViolated(
                    "describe table query returned multiple results".to_string(),
                );
                return Err(err.into());
            }
        }
    };

    let columns = {
        let stmt = r#"SELECT
                   name,
                   type_oid,
                   type_mod,
                   COALESCE(coms.comment, '') = $1 AS primary_key
               FROM mz_columns AS cols
               LEFT JOIN mz_internal.mz_comments AS coms
               ON cols.id = coms.id AND cols.position = coms.object_sub_id
               WHERE cols.id = $2
               ORDER BY cols.position ASC"#;

        let rows = client
            .query(stmt, &[&PRIMARY_KEY_MAGIC_STRING, &table_id])
            .await
            .context("fetching table columns")?;

        let mut columns = vec![];
        for row in rows {
            let name = row.get::<_, String>("name");
            let primary_key = row.get::<_, bool>("primary_key");
            let ty_oid = row.get::<_, u32>("type_oid");
            let ty_mod = row.get::<_, i32>("type_mod");
            let ty = Type::from_oid_and_typmod(ty_oid, ty_mod).with_context(|| {
                format!("looking up type with OID {ty_oid} and modifier {ty_mod}")
            })?;
            let (ty, decimal) = utils::to_fivetran_type(ty)?;

            columns.push(Column {
                name,
                r#type: ty.into(),
                primary_key,
                decimal,
            })
        }
        columns
    };

    Ok(Some(Table {
        name: table.to_string(),
        columns,
    }))
}

pub async fn handle_create_table(request: CreateTableRequest) -> Result<(), OpError> {
    let table = request.table.ok_or(OpErrorKind::FieldMissing("table"))?;

    let schema = Ident::new(&request.schema_name)?;
    let qualified_table_name =
        UnresolvedItemName::qualified(&[schema.clone(), Ident::new(&table.name)?]);

    let mut total_columns = table.columns;
    // We want to make sure the deleted system column is always provided.
    //
    // Note: Instead of creating a map we check existence in the Vec, to retain original order
    // of the columns from the request.
    //
    // TODO(parkmycar): Use an IndexMap here.
    let contains_delete = total_columns
        .iter()
        .any(|col| col.name == FIVETRAN_SYSTEM_COLUMN_DELETE);
    if !contains_delete {
        let delete_column = Column {
            name: FIVETRAN_SYSTEM_COLUMN_DELETE.to_string(),
            r#type: DataType::Boolean.into(),
            primary_key: false,
            decimal: None,
        };
        total_columns.push(delete_column);
    }

    let columns = total_columns
        .iter()
        .map(ColumnMetadata::try_from)
        .collect::<Result<Vec<_>, OpError>>()?;

    let defs = columns.iter().map(|col| col.to_column_def()).join(",");
    let sql = format!(
        r#"BEGIN; CREATE SCHEMA IF NOT EXISTS {schema}; COMMIT;
        BEGIN; CREATE TABLE {qualified_table_name} ({defs}); COMMIT;"#,
    );

    let (_dbname, client) = config::connect(request.configuration).await?;
    client.batch_execute(&sql).await?;

    // TODO(parkmycar): This is an ugly hack!
    //
    // If Fivetran creates a table with primary keys, it expects a DescribeTableRequest to report
    // those columns as primary keys. But Materialize doesn't support primary keys, so we need to
    // store this metadata somewhere else. For now we do it in a COMMENT.
    for column in columns.iter().filter(|col| col.is_primary) {
        let stmt = format!(
            "COMMENT ON COLUMN {qualified_table_name}.{column_name} IS {magic_comment}",
            column_name = column.escaped_name,
            magic_comment = escape::escape_literal(PRIMARY_KEY_MAGIC_STRING),
        );
        client
            .execute(&stmt, &[])
            .await
            .context("setting magic primary key comment")?;
    }

    Ok(())
}

#[allow(clippy::unused_async)]
pub async fn handle_alter_table(request: AlterTableRequest) -> Result<(), OpError> {
    // Bail early if there isn't a table to alter.
    let Some(request_table) = request.table else {
        return Ok(());
    };

    // All other kinds of alter table requests are unsupported.
    let error = format!("alter_table, request: {request_table:?}");
    Err(OpErrorKind::Unsupported(error).into())
}
