// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use mz_ore::error::ErrorExt;
use mz_pgrepr::Type;
use postgres_protocol::escape;

use crate::destination::config;
use crate::fivetran_sdk::{
    AlterTableRequest, AlterTableResponse, Column, CreateTableRequest, CreateTableResponse,
    DescribeTableRequest, DescribeTableResponse, Table,
};
use crate::utils;

pub async fn handle_describe_table_request(
    request: DescribeTableRequest,
) -> Result<DescribeTableResponse, anyhow::Error> {
    use crate::fivetran_sdk::describe_table_response::Response;

    let response = match describe_table(request).await {
        Ok(None) => Response::NotFound(true),
        Ok(Some(table)) => Response::Table(table),
        Err(e) => Response::Failure(e.display_with_causes().to_string()),
    };
    Ok(DescribeTableResponse {
        response: Some(response),
    })
}

pub async fn handle_create_table_request(
    request: CreateTableRequest,
) -> Result<CreateTableResponse, anyhow::Error> {
    use crate::fivetran_sdk::create_table_response::Response;

    let response = match create_table(request).await {
        Ok(()) => Response::Success(true),
        Err(e) => Response::Failure(e.display_with_causes().to_string()),
    };
    Ok(CreateTableResponse {
        response: Some(response),
    })
}

#[allow(clippy::unused_async)]
pub async fn handle_alter_table_request(
    _: AlterTableRequest,
) -> Result<AlterTableResponse, anyhow::Error> {
    use crate::fivetran_sdk::alter_table_response::Response;

    Ok(AlterTableResponse {
        response: Some(Response::Failure("ALTER TABLE is not supported".into())),
    })
}

async fn describe_table(request: DescribeTableRequest) -> Result<Option<Table>, anyhow::Error> {
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
            _ => bail!("internal error: describe table query returned multiple results"),
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
            // TODO(benesch): should we be stripping these out?
            if name == "_fivetran_deleted" || name == "_fivetran_synced" {
                continue;
            }
            let primary_key = name.starts_with('k'); // TODO(benesch): support primary keys
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
        name: request.table_name,
        columns,
    }))
}

async fn create_table(request: CreateTableRequest) -> Result<(), anyhow::Error> {
    let Some(table) = request.table else {
        bail!("internal error: CreateTableRequest missing \"table\" field");
    };
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
