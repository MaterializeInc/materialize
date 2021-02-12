// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::bail;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::Serialize;
use serde_json::{Number, Value};
use url::form_urlencoded;

use crate::http::util;
use coord::ExecuteResponse;
use dataflow_types::PeekResponse;
use ore::collections::CollectionExt;
use repr::{Datum, ScalarType};
use sql_parser::parser::parse_statements;

pub async fn handle_sql(
    req: Request<Body>,
    coord_client: coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    let res = async {
        let body = hyper::body::to_bytes(req).await?;
        let body: HashMap<_, _> = form_urlencoded::parse(&body).collect();
        let sql = match body.get("sql") {
            Some(sql) => sql,
            None => bail!("expected `sql` parameter"),
        };
        let res = query_sql(coord_client, sql.to_string()).await?;
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&res)?))
            .unwrap())
    }
    .await;
    match res {
        Ok(res) => Ok(res),
        Err(e) => Ok(util::error_response(StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// Executes a single SQL statement as the specified user.
async fn query_sql(
    mut coord_client: coord::SessionClient,
    sql: String,
) -> anyhow::Result<SqlResult> {
    let stmts = parse_statements(&sql)?;
    if stmts.len() != 1 {
        bail!("expected exactly 1 statement");
    }
    let stmt = stmts.into_element();

    coord_client.session().start_transaction();

    const EMPTY_PORTAL: &str = "";
    let params = vec![];
    coord_client
        .declare(EMPTY_PORTAL.into(), stmt, params)
        .await?;
    let desc = coord_client
        .session()
        .get_portal(EMPTY_PORTAL)
        .map(|portal| portal.desc.clone())
        .expect("unnamed portal should be present");
    if !desc.param_types.is_empty() {
        bail!("parameters are not supported");
    }

    let res = coord_client.execute(EMPTY_PORTAL.into()).await?;

    let rows = match res {
        ExecuteResponse::SendingRows(rows) => {
            let response = rows.await;
            response
        }
        _ => bail!("unsupported statement type"),
    };
    let rows = match rows {
        PeekResponse::Rows(rows) => rows,
        PeekResponse::Error(e) => bail!("{}", e),
        PeekResponse::Canceled => bail!("execution canceled"),
    };
    let mut sql_rows: Vec<Vec<Value>> = vec![];
    let (col_names, col_types) = match desc.relation_desc {
        Some(desc) => (
            desc.iter_names()
                .map(|name| name.map(|name| name.to_string()))
                .collect(),
            desc.typ().column_types.clone(),
        ),
        None => (vec![], vec![]),
    };
    for row in rows {
        let datums = row.unpack();
        sql_rows.push(
            datums
                .iter()
                .enumerate()
                .map(|(idx, datum)| match datum {
                    // Convert some common things to a native JSON value. This doesn't need to be
                    // too exhaustive because the SQL-over-HTTP interface is currently not hooked
                    // up to arbitrary external user queries.
                    Datum::Null | Datum::JsonNull => Value::Null,
                    Datum::False => Value::Bool(false),
                    Datum::True => Value::Bool(true),
                    Datum::Int32(n) => Value::Number(Number::from(*n)),
                    Datum::Int64(n) => Value::Number(Number::from(*n)),
                    Datum::Float32(n) => float_to_json(n.into_inner() as f64),
                    Datum::Float64(n) => float_to_json(n.into_inner()),
                    Datum::String(s) => Value::String(s.to_string()),
                    Datum::Decimal(d) => Value::String(if col_types.len() > idx {
                        match col_types[idx].scalar_type {
                            ScalarType::Decimal(_precision, scale) => {
                                d.with_scale(scale).to_string()
                            }
                            _ => datum.to_string(),
                        }
                    } else {
                        datum.to_string()
                    }),
                    _ => Value::String(datum.to_string()),
                })
                .collect(),
        );
    }
    Ok(SqlResult {
        rows: sql_rows,
        col_names,
    })
}

#[derive(Serialize)]
struct SqlResult {
    rows: Vec<Vec<Value>>,
    col_names: Vec<Option<String>>,
}

// Convert most floats to a JSON Number. JSON Numbers don't support NaN or
// Infinity, so those will still be rendered as strings.
fn float_to_json(f: f64) -> Value {
    match Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None => Value::String(f.to_string()),
    }
}
