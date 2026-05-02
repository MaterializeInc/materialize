// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, anyhow, bail};
use duckdb::types::ValueRef;

use crate::action::duckdb::get_or_create_connection;
use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_query(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let sort_rows = cmd.args.opt_bool("sort-rows")?.unwrap_or(false);
    cmd.args.done()?;

    // First line is the query, remaining lines are expected output
    let mut lines = cmd.input.into_iter();
    let query = lines
        .next()
        .ok_or_else(|| anyhow!("duckdb-query requires a query as the first input line"))?;
    let mut expected_rows: Vec<String> = lines.collect();

    let conn = get_or_create_connection(state, name).await?;

    let mut actual_rows = mz_ore::task::spawn_blocking(
        || "duckdb_query".to_string(),
        move || {
            let conn = conn.lock().map_err(|e| anyhow!("lock poisoned: {}", e))?;
            println!(">> {}", query);
            let mut stmt = conn.prepare(&query).context("preparing DuckDB query")?;
            let mut rows = stmt.query([]).context("executing DuckDB query")?;

            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                // Get column count from the row's statement
                let column_count = row.as_ref().column_count();
                let mut row_values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let val = row.get_ref(i)?;
                    let formatted = format_value(&val);
                    row_values.push(formatted);
                }
                result.push(row_values.join(" "));
            }
            Ok::<_, anyhow::Error>(result)
        },
    )
    .await?;

    if sort_rows {
        expected_rows.sort();
        actual_rows.sort();
    }

    if actual_rows != expected_rows {
        bail!(
            "DuckDB query result mismatch\nexpected ({} rows):\n{}\n\nactual ({} rows):\n{}",
            expected_rows.len(),
            expected_rows.join("\n"),
            actual_rows.len(),
            actual_rows.join("\n")
        );
    }

    Ok(ControlFlow::Continue)
}

fn format_value(val: &ValueRef) -> String {
    match val {
        ValueRef::Null => "<null>".to_string(),
        ValueRef::Boolean(b) => b.to_string(),
        ValueRef::TinyInt(i) => i.to_string(),
        ValueRef::SmallInt(i) => i.to_string(),
        ValueRef::Int(i) => i.to_string(),
        ValueRef::BigInt(i) => i.to_string(),
        ValueRef::HugeInt(i) => i.to_string(),
        ValueRef::UTinyInt(i) => i.to_string(),
        ValueRef::USmallInt(i) => i.to_string(),
        ValueRef::UInt(i) => i.to_string(),
        ValueRef::UBigInt(i) => i.to_string(),
        ValueRef::Float(f) => f.to_string(),
        ValueRef::Double(f) => f.to_string(),
        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).to_string(),
        ValueRef::Blob(bytes) => format!("{:?}", bytes),
        _ => format!("{:?}", val),
    }
}
