// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_pgrepr::Type;
use postgres_protocol::escape;

use crate::destination::{config, FIVETRAN_SYSTEM_COLUMN_ID};
use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::{
    AlterTableRequest, Column, CreateTableRequest, DescribeTableRequest, Table,
};
use crate::utils;

pub async fn handle_describe_table(
    request: DescribeTableRequest,
) -> Result<Option<Table>, OpError> {
    let (dbname, client) = config::connect(request.configuration).await?;

    let table_id = {
        let rows = client
            .query(
                r#"SELECT t.id
                   FROM mz_tables t
                   JOIN mz_schemas s ON s.id = t.schema_id
                   JOIN mz_databases d ON d.id = s.database_id
                   WHERE d.name = $1 AND s.name = $2 AND t.name = $3
                "#,
                &[&dbname, &request.schema_name, &request.table_name],
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
        let rows = client
            .query(
                r#"SELECT name, type_oid, type_mod
                   FROM mz_columns c
                   WHERE c.id = $1
                "#,
                &[&table_id],
            )
            .await
            .context("fetching table columns")?;

        let mut columns = vec![];
        for row in rows {
            let name = row.get::<_, String>("name");
            let ty_oid = row.get::<_, u32>("type_oid");
            let ty_mod = row.get::<_, i32>("type_mod");
            let ty = Type::from_oid_and_typmod(ty_oid, ty_mod).with_context(|| {
                format!("looking up type with OID {ty_oid} and modifier {ty_mod}")
            })?;
            let (ty, decimal) = utils::to_fivetran_type(ty)?;

            // TODO(benesch): support primary keys in Materialize.
            let primary_key = name == FIVETRAN_SYSTEM_COLUMN_ID;
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
        name: request.table_name,
        columns,
    }))
}

pub async fn handle_create_table(request: CreateTableRequest) -> Result<(), OpError> {
    let table = request.table.ok_or(OpErrorKind::FieldMissing("table"))?;

    // TODO(parkmycar): Make sure table name is <= 255 characters.

    let mut defs = vec![];
    let mut primary_key_columns = vec![];
    for column in table.columns {
        let name = escape::escape_identifier(&column.name);
        let mut ty = utils::to_materialize_type(column.r#type())?.to_string();
        if let Some(d) = column.decimal {
            ty += &format!("({}, {})", d.precision, d.scale);
        }

        defs.push(format!("{name} {ty}"));

        if column.primary_key {
            primary_key_columns.push(name.clone());
        }
    }

    // TODO(benesch): support primary keys.
    #[allow(clippy::overly_complex_bool_expr)]
    if !primary_key_columns.is_empty() && false {
        defs.push(format!("PRIMARY KEY ({})", primary_key_columns.join(",")));
    }

    let sql = format!(
        r#"BEGIN; CREATE SCHEMA IF NOT EXISTS {schema}; COMMIT;
           BEGIN; CREATE TABLE {schema}.{table} ({defs}); COMMIT;"#,
        schema = escape::escape_identifier(&request.schema_name),
        table = escape::escape_identifier(&table.name),
        defs = defs.join(","),
    );

    let (_dbname, client) = config::connect(request.configuration).await?;
    client.batch_execute(&sql).await?;

    Ok(())
}

#[allow(clippy::unused_async)]
pub async fn handle_alter_table(_: AlterTableRequest) -> Result<(), OpError> {
    Err(OpErrorKind::Unsupported("alter_table".to_string()).into())
}
