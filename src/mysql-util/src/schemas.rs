// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::desc::{MySqlColumnDesc, MySqlDataType, MySqlTableDesc};
use crate::MySqlError;

/// Retrieve the tables and column descriptions for tables in the given schemas.
pub async fn schema_info(
    conn: &mut Conn,
    schemas: Vec<&str>,
) -> Result<Vec<MySqlTableDesc>, MySqlError> {
    // Get all tables of type 'Base Table' in schema
    let table_q = "SELECT table_name, table_schema
                   FROM information_schema.tables
                   WHERE table_type = 'BASE TABLE'
                   AND table_schema IN (?)";
    let table_rows: Vec<(String, String)> = conn.exec(table_q, schemas).await?;

    let mut tables = vec![];
    for (table_name, schema_name) in table_rows {
        let column_q = "SELECT column_name, data_type, is_nullable, ordinal_position
                        FROM information_schema.columns
                        WHERE table_name = ? AND table_schema = ?";
        let column_rows = conn
            .exec::<(String, String, String, u16), _, _>(column_q, (&table_name, &schema_name))
            .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for (name, data_type, is_nullable, ordinal_position) in column_rows {
            let data_type = match data_type.as_str() {
                "int" => MySqlDataType::Int,
                "varchar" => MySqlDataType::Varchar(usize::MAX),
                "char" => MySqlDataType::Varchar(usize::MAX),
                _ => return Err(MySqlError::UnsupportedDataType(data_type)),
            };
            columns.push((
                ordinal_position,
                MySqlColumnDesc {
                    name,
                    data_type,
                    nullable: is_nullable == "YES",
                },
            ));
        }
        // Sort columns in-place by their ordinal_position
        columns.sort_by(|a, b| a.0.cmp(&b.0));

        tables.push(MySqlTableDesc {
            schema_name,
            name: table_name,
            columns: columns.into_iter().map(|(_, c)| c).collect(),
        });
    }
    Ok(tables)
}
