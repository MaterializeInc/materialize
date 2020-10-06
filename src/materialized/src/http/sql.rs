// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::future::Future;

use anyhow::bail;
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::Serialize;
use serde_json::{Number, Value};
use url::form_urlencoded;

use crate::http::{util, Server};
use coord::ExecuteResponse;
use dataflow_types::PeekResponse;
use repr::Datum;
use sql::plan::Params;
use sql_parser::parser::parse_statements;

impl Server {
    pub fn handle_sql(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let cmdq_tx = self.cmdq_tx.clone();
        async move {
            let res = async {
                let body = hyper::body::to_bytes(req).await?;
                let body: HashMap<_, _> = form_urlencoded::parse(&body).collect();
                let sql = match body.get("sql") {
                    Some(sql) => sql,
                    None => bail!("expected `sql` parameter"),
                };
                let params = Params {
                    datums: repr::Row::new(vec![]),
                    types: vec![],
                };
                let res = query_sql_as_system(cmdq_tx, sql.to_string(), params).await?;
                Ok(Response::builder()
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&res)?))
                    .unwrap())
            }
            .await;
            match res {
                Ok(res) => Ok(res),
                Err(e) => return Ok(util::error_response(StatusCode::BAD_REQUEST, e.to_string())),
            }
        }
    }
}

async fn query_sql_as_system(
    mut cmdq_tx: UnboundedSender<coord::Command>,
    sql: String,
    params: Params,
) -> anyhow::Result<SqlResult> {
    let mut stmts = parse_statements(sql)?;
    if stmts.len() != 1 {
        bail!("expected exactly 1 statement");
    }
    let stmt = stmts.remove(0);
    let (tx, rx) = futures::channel::oneshot::channel();
    cmdq_tx
        .send(coord::Command::NoSessionExecute { stmt, params, tx })
        .await?;
    let (desc, result) = rx.await??;
    let response = match result {
        ExecuteResponse::SendingRows(rows) => {
            let response = rows.await?;
            response
        }
        _ => bail!("unexpected ExecuteResponse type"),
    };
    let rows = match response {
        PeekResponse::Rows(rows) => rows,
        PeekResponse::Error(e) => bail!("{}", e),
        _ => bail!("unexpected PeekResponse type"),
    };
    let mut res: Vec<Vec<Value>> = vec![];
    for row in rows {
        let datums = row.unpack();
        res.push(
            datums
                .iter()
                .map(|datum| match datum {
                    Datum::Null | Datum::JsonNull => Value::Null,
                    Datum::False => Value::Bool(false),
                    Datum::True => Value::Bool(true),
                    Datum::Int32(n) => Value::Number(Number::from(*n)),
                    Datum::Int64(n) => Value::Number(Number::from(*n)),
                    Datum::Float32(n) => float_to_json(n.into_inner() as f64),
                    Datum::Float64(n) => float_to_json(n.into_inner()),
                    Datum::String(s) => Value::String(s.to_string()),
                    _ => Value::String(datum.to_string()),
                })
                .collect(),
        );
    }
    let col_names = match desc {
        Some(desc) => desc
            .iter_names()
            .map(|name| name.map(|name| name.as_str().to_string()))
            .collect(),
        None => vec![],
    };
    Ok(SqlResult {
        rows: res,
        col_names,
    })
}

#[derive(Serialize)]
struct SqlResult {
    rows: Vec<Vec<Value>>,
    col_names: Vec<Option<String>>,
}

fn float_to_json(f: f64) -> Value {
    match Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None => Value::String(f.to_string()),
    }
}
