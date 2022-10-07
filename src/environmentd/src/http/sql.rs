// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use axum::response::IntoResponse;
use axum::Json;
use http::StatusCode;
use itertools::izip;
use serde::{Deserialize, Serialize};

use mz_adapter::session::{EndTransactionAction, TransactionStatus};
use mz_adapter::{
    ExecuteResponse, ExecuteResponseKind, ExecuteResponsePartialError, PeekResponseUnary,
    SessionClient,
};
use mz_repr::{Datum, RowArena};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{Raw, Statement};
use mz_sql::plan::Plan;

use crate::http::AuthedClient;

pub async fn handle_sql(
    mut client: AuthedClient,
    Json(request): Json<SqlRequest>,
) -> impl IntoResponse {
    match execute_request(&mut client.0, request).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}

/// A request to execute SQL over HTTP.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SqlRequest {
    /// A simple query request.
    Simple {
        /// A query string containing zero or more queries delimited by
        /// semicolons.
        // TODO: we can remove this alias once platforms issues these requests
        // using `query`.
        #[serde(alias = "sql")]
        query: String,
    },
    /// An extended query request.
    Extended {
        /// Queries to execute using the extended protocol.
        queries: Vec<ExtendedRequest>,
    },
}

/// An request to execute a SQL query using the extended protocol.
#[derive(Serialize, Deserialize, Debug)]
pub struct ExtendedRequest {
    // TODO: we can remove this alias once platforms issues these requests
    // using `query`.
    #[serde(alias = "sql")]
    /// A query string containing zero or one queries.
    query: String,
    /// Optional parameters for the query.
    #[serde(default)]
    params: Vec<Option<String>>,
}

/// The response to a [`SqlRequest`].
#[derive(Debug, Serialize)]
struct SqlResponse {
    /// The results for each query in the request.
    results: Vec<SqlResult>,
}

/// The result of a single query in a [`SqlResponse`].
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum SqlResult {
    /// The query returned rows.
    Rows {
        /// The result rows.
        rows: Vec<Vec<serde_json::Value>>,
        /// The name of the columns in the row.
        col_names: Vec<String>,
    },
    /// The query executed successfully but did not return rows.
    Ok {
        ok: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partial_err: Option<ExecuteResponsePartialError>,
    },
    /// The query returned an error.
    Err { error: String },
}

impl SqlResult {
    fn err(msg: impl std::fmt::Display) -> SqlResult {
        SqlResult::Err {
            error: msg.to_string(),
        }
    }

    /// Generates a `SimpleResult::Ok` based on an `ExecuteResponse`.
    ///
    /// # Panics
    /// - If `ExecuteResponse::partial_err(&res)` panics.
    fn ok(res: ExecuteResponse) -> SqlResult {
        let ok = res.tag();
        let partial_err = res.partial_err();
        SqlResult::Ok { ok, partial_err }
    }
}

/// Executes an entire [`SqlRequest`].
///
/// See the user-facing documentation about the HTTP API for a description of
/// the semantics of this function.
async fn execute_request(
    client: &mut SessionClient,
    request: SqlRequest,
) -> Result<SqlResponse, anyhow::Error> {
    // This API prohibits executing statements with responses whose
    // semantics are at odds with an HTTP response.
    fn check_prohibited_stmts(stmt: &Statement<Raw>) -> Result<(), anyhow::Error> {
        let execute_responses = Plan::generated_from(stmt.into())
            .into_iter()
            .map(ExecuteResponse::generated_from)
            .flatten()
            .collect::<Vec<_>>();

        if execute_responses.iter().any(|execute_response| {
            matches!(
                execute_response,
                ExecuteResponseKind::Fetch
                    | ExecuteResponseKind::Subscribing
                    | ExecuteResponseKind::CopyTo
                    | ExecuteResponseKind::CopyFrom
                    | ExecuteResponseKind::Raise
                    | ExecuteResponseKind::DeclaredCursor
                    | ExecuteResponseKind::ClosedCursor
            )
        }) && !matches!(
            stmt,
            // Both `SelectStatement` and `CopyStatement` generate
            // `PeekPlan`, but `SELECT` should be permitted and `COPY` not.
            Statement::Select(mz_sql::ast::SelectStatement { query: _, as_of: _ })
        ) {
            anyhow::bail!("unsupported via this API: {}", stmt.to_ast_string());
        }

        Ok(())
    }

    let mut stmt_groups = vec![];
    let mut results = vec![];

    match request {
        SqlRequest::Simple { query } => {
            let stmts = mz_sql::parse::parse(&query).map_err(|e| anyhow!(e))?;
            let mut stmt_group = Vec::with_capacity(stmts.len());
            for stmt in stmts {
                check_prohibited_stmts(&stmt)?;
                stmt_group.push((stmt, vec![]));
            }
            stmt_groups.push(stmt_group);
        }
        SqlRequest::Extended { queries } => {
            for ExtendedRequest { query, params } in queries {
                let mut stmts = mz_sql::parse::parse(&query).map_err(|e| anyhow!(e))?;
                if stmts.len() != 1 {
                    anyhow::bail!(
                        "each query must contain exactly 1 statement, but \"{}\" contains {}",
                        query,
                        stmts.len()
                    );
                }

                let stmt = stmts.pop().unwrap();
                check_prohibited_stmts(&stmt)?;

                stmt_groups.push(vec![(stmt, params)]);
            }
        }
    }

    for stmt_group in stmt_groups {
        let num_stmts = stmt_group.len();
        for (stmt, params) in stmt_group {
            assert!(num_stmts <= 1 || params.is_empty(),
                "statement groups contain more than 1 statement iff Simple request, which does not support parameters"
            );

            if matches!(client.session().transaction(), TransactionStatus::Failed(_)) {
                break;
            }
            // Mirror the behavior of the PostgreSQL simple query protocol.
            // See the pgwire::protocol::StateMachine::query method for details.
            if let Err(e) = client.start_transaction(Some(num_stmts)).await {
                results.push(SqlResult::err(e));
                break;
            }
            let res = execute_stmt(client, stmt, params).await;
            if matches!(res, SqlResult::Err { .. }) {
                client.fail_transaction();
            }
            results.push(res);
        }
    }

    if client.session().transaction().is_implicit() {
        client.end_transaction(EndTransactionAction::Commit).await?;
    }
    Ok(SqlResponse { results })
}

/// Executes a single statement in a [`SqlRequest`].
async fn execute_stmt(
    client: &mut SessionClient,
    stmt: Statement<Raw>,
    raw_params: Vec<Option<String>>,
) -> SqlResult {
    const EMPTY_PORTAL: &str = "";
    if let Err(e) = client
        .describe(EMPTY_PORTAL.into(), Some(stmt.clone()), vec![])
        .await
    {
        return SqlResult::err(e);
    }

    let prep_stmt = match client.get_prepared_statement(EMPTY_PORTAL).await {
        Ok(stmt) => stmt,
        Err(err) => {
            return SqlResult::err(err);
        }
    };

    let param_types = &prep_stmt.desc().param_types;
    if param_types.len() != raw_params.len() {
        let message = format!(
            "request supplied {actual} parameters, \
                        but {statement} requires {expected}",
            statement = stmt.to_ast_string(),
            actual = raw_params.len(),
            expected = param_types.len()
        );
        return SqlResult::err(message);
    }

    let buf = RowArena::new();
    let mut params = vec![];
    for (raw_param, mz_typ) in izip!(raw_params, param_types) {
        let pg_typ = mz_pgrepr::Type::from(mz_typ);
        let datum = match raw_param {
            None => Datum::Null,
            Some(raw_param) => {
                match mz_pgrepr::Value::decode(
                    mz_pgrepr::Format::Text,
                    &pg_typ,
                    raw_param.as_bytes(),
                ) {
                    Ok(param) => param.into_datum(&buf, &pg_typ),
                    Err(err) => {
                        let msg = format!("unable to decode parameter: {}", err);
                        return SqlResult::err(msg);
                    }
                }
            }
        };
        params.push((datum, mz_typ.clone()))
    }

    let result_formats = vec![
        mz_pgrepr::Format::Text;
        prep_stmt
            .desc()
            .relation_desc
            .clone()
            .map(|desc| desc.typ().column_types.len())
            .unwrap_or(0)
    ];

    let desc = prep_stmt.desc().clone();
    let revision = prep_stmt.catalog_revision;
    let stmt = prep_stmt.sql().cloned();
    if let Err(err) = client.session().set_portal(
        EMPTY_PORTAL.into(),
        desc,
        stmt,
        params,
        result_formats,
        revision,
    ) {
        return SqlResult::err(err.to_string());
    }

    let desc = client
        .session()
        // We do not need to verify here because `client.execute` verifies below.
        .get_portal_unverified(EMPTY_PORTAL)
        .map(|portal| portal.desc.clone())
        .expect("unnamed portal should be present");

    let res = match client.execute(EMPTY_PORTAL.into()).await {
        Ok(res) => res,
        Err(e) => {
            return SqlResult::err(e);
        }
    };

    match res {
        ExecuteResponse::Canceled => {
            SqlResult::err("statement canceled due to user request")
        }
        res @ (ExecuteResponse::CreatedConnection { .. }
        | ExecuteResponse::CreatedDatabase { .. }
        | ExecuteResponse::CreatedSchema { .. }
        | ExecuteResponse::CreatedRole
        | ExecuteResponse::CreatedComputeInstance { .. }
        | ExecuteResponse::CreatedComputeReplica { .. }
        | ExecuteResponse::CreatedTable { .. }
        | ExecuteResponse::CreatedIndex { .. }
        | ExecuteResponse::CreatedSecret { .. }
        | ExecuteResponse::CreatedSource { .. }
        | ExecuteResponse::CreatedSources
        | ExecuteResponse::CreatedSink { .. }
        | ExecuteResponse::CreatedView { .. }
        | ExecuteResponse::CreatedViews { .. }
        | ExecuteResponse::CreatedMaterializedView { .. }
        | ExecuteResponse::CreatedType
        | ExecuteResponse::Deleted(_)
        | ExecuteResponse::DiscardedTemp
        | ExecuteResponse::DiscardedAll
        | ExecuteResponse::DroppedDatabase
        | ExecuteResponse::DroppedSchema
        | ExecuteResponse::DroppedRole
        | ExecuteResponse::DroppedComputeInstance
        | ExecuteResponse::DroppedComputeReplica
        | ExecuteResponse::DroppedSource
        | ExecuteResponse::DroppedIndex
        | ExecuteResponse::DroppedSink
        | ExecuteResponse::DroppedTable
        | ExecuteResponse::DroppedView
        | ExecuteResponse::DroppedMaterializedView
        | ExecuteResponse::DroppedType
        | ExecuteResponse::DroppedSecret
        | ExecuteResponse::DroppedConnection
        | ExecuteResponse::EmptyQuery
        | ExecuteResponse::Inserted(_)
        | ExecuteResponse::SetVariable { .. }
        | ExecuteResponse::StartedTransaction { .. }
        | ExecuteResponse::TransactionExited {
            ..
        }
        | ExecuteResponse::Updated(_)
        | ExecuteResponse::AlteredObject(_)
        | ExecuteResponse::AlteredIndexLogicalCompaction
        | ExecuteResponse::AlteredSystemConfiguraion
        | ExecuteResponse::Deallocate { .. }
        | ExecuteResponse::Prepare) => SqlResult::ok(res),
        ExecuteResponse::SendingRows {
            future: rows,
            span: _,
        } => {
            let rows = match rows.await {
                PeekResponseUnary::Rows(rows) => rows,
                PeekResponseUnary::Error(e) => {
                    return SqlResult::err(e);
                }
                PeekResponseUnary::Canceled => {
                    return SqlResult::err("statement canceled due to user request");
                }
            };
            let mut sql_rows: Vec<Vec<serde_json::Value>> = vec![];
            let col_names = match desc.relation_desc {
                Some(desc) => desc.iter_names().map(|name| name.to_string()).collect(),
                None => vec![],
            };
            let mut datum_vec = mz_repr::DatumVec::new();
            for row in rows {
                let datums = datum_vec.borrow_with(&row);
                sql_rows.push(datums.iter().map(From::from).collect());
            }
            SqlResult::Rows {
                rows: sql_rows,
                col_names,
            }
        }
        res @ (ExecuteResponse::Fetch { .. }
        | ExecuteResponse::Subscribing { .. }
        | ExecuteResponse::CopyTo { .. }
        | ExecuteResponse::CopyFrom { .. }
        | ExecuteResponse::Raise { .. }
        | ExecuteResponse::DeclaredCursor
        | ExecuteResponse::ClosedCursor) => {
            SqlResult::err(
                format!("internal error: encountered prohibited ExecuteResponse {:?}.\n\n
This is a bug. Can you please file an issue letting us know?\n
https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=C-bug%2CC-triage&template=01-bug.yml",
            ExecuteResponseKind::from(res)))
        }
    }
}
