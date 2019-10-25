// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use dataflow::{WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use failure::bail;
use futures::sync::mpsc::UnboundedReceiver;
use futures::Stream;
use ore::collections::CollectionExt;
use repr::{RelationDesc, ScalarType};
use sql::store::{Catalog, CatalogItem};
use sql::PreparedStatement;
use sql::{Plan, Session};
use std::thread;
use std::thread::JoinHandle;

use super::{coordinator::Coordinator, Command, QueryExecuteResponse, Response};

enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
}

pub fn serve<C>(
    switchboard: comm::Switchboard<C>,
    num_timely_workers: usize,
    logging_config: Option<&LoggingConfig>,
    startup_sql: String,
    cmd_rx: UnboundedReceiver<Command>,
) -> Result<JoinHandle<()>, failure::Error>
where
    C: comm::Connection,
{
    let mut coord = Coordinator::new(switchboard.clone(), num_timely_workers, logging_config);
    let feedback_rx = coord.enable_feedback();

    let mut catalog = sql::store::Catalog::new(logging_config);

    {
        // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
        // the first connection ID used is 1. As long as that remains the case,
        // 0 is safe to use here.
        let conn_id = 0;
        let mut session = sql::Session::default();
        for stmt in sql::parse(startup_sql)? {
            handle_statement(&mut coord, &mut catalog, &mut session, stmt, conn_id)?;
        }
    }

    let messages = cmd_rx
        .map(Message::Command)
        .map_err(|()| unreachable!())
        .select(feedback_rx.map(Message::Worker));

    Ok(thread::spawn(move || {
        for msg in messages.wait() {
            match msg.unwrap() {
                Message::Command(Command::Query {
                    sql,
                    mut session,
                    conn_id,
                    tx,
                }) => {
                    let result = handle_query(&mut coord, &mut catalog, &mut session, sql, conn_id);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::Execute {
                    portal_name,
                    mut session,
                    conn_id,
                    tx,
                }) => {
                    let result = handle_execute(
                        &mut coord,
                        &mut catalog,
                        &mut session,
                        portal_name,
                        conn_id,
                    );
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::Parse {
                    name,
                    sql,
                    mut session,
                    tx,
                }) => {
                    let result = handle_parse(&catalog, &mut session, name, sql);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::CancelRequest { conn_id }) => {
                    coord.sequence_cancel(conn_id);
                }

                Message::Command(Command::Shutdown) => break,

                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id,
                    message: WorkerFeedback::FrontierUppers(updates),
                }) => {
                    // Only take information from worker 0 for now. We'll want
                    // to do something smarter soon. Ask Frank for details.
                    if worker_id == 0 {
                        for (name, frontier) in updates {
                            coord.update_upper(&name, &frontier);
                        }
                        coord.maintenance();
                    }
                }
            }
        }
    }))
}

// TODO(benesch): temporarily public until SLT stops depending on this
// crate's internal API.
pub fn apply_plan(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &Plan,
) -> Result<(), failure::Error> {
    match plan {
        Plan::CreateView(view) => catalog.insert(CatalogItem::View(view.clone()))?,
        Plan::CreateSource(source) => catalog.insert(CatalogItem::Source(source.clone()))?,
        Plan::CreateSources(sources) => {
            for source in sources {
                catalog.insert(CatalogItem::Source(source.clone()))?
            }
        }
        Plan::CreateSink(sink) => catalog.insert(CatalogItem::Sink(sink.clone()))?,
        Plan::DropItems((names, _is_source)) => {
            for name in names {
                catalog.remove(name);
            }
        }
        Plan::SetVariable { name, value } => session.set(name, value)?,
        _ => (),
    }
    Ok(())
}

fn handle_statement<C>(
    coord: &mut Coordinator<C>,
    catalog: &mut Catalog,
    session: &mut Session,
    stmt: sql::Statement,
    conn_id: u32,
) -> Result<QueryExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    sql::plan(catalog, session, stmt)
        .and_then(|plan| {
            apply_plan(catalog, session, &plan)?;
            Ok(plan)
        })
        .map(|plan| coord.sequence_plan(plan, conn_id, None /* ts_override */))
}

fn handle_query<C>(
    coord: &mut Coordinator<C>,
    catalog: &mut Catalog,
    session: &mut Session,
    sql: String,
    conn_id: u32,
) -> Result<QueryExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    let stmts = sql::parse(sql)?;
    match stmts.len() {
        0 => Ok(QueryExecuteResponse::EmptyQuery),
        1 => handle_statement(coord, catalog, session, stmts.into_element(), conn_id),
        n => bail!("expected no more than one query, got {}", n),
    }
}

fn handle_execute<C>(
    coord: &mut Coordinator<C>,
    catalog: &mut Catalog,
    session: &mut Session,
    portal_name: String,
    conn_id: u32,
) -> Result<QueryExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    let portal = session
        .get_portal(&portal_name)
        .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))?;
    let prepared = session
        .get_prepared_statement(&portal.statement_name)
        .ok_or_else(|| {
            failure::format_err!(
                "statement for portal does not exist portal={:?} statement={:?}",
                portal_name,
                portal.statement_name
            )
        })?;
    match prepared.sql() {
        Some(stmt) => {
            let stmt = stmt.clone();
            handle_statement(coord, catalog, session, stmt, conn_id)
        }
        None => Ok(QueryExecuteResponse::EmptyQuery),
    }
}

fn handle_parse(
    catalog: &Catalog,
    session: &mut Session,
    name: String,
    sql: String,
) -> Result<(), failure::Error> {
    let stmts = sql::parse(sql)?;
    let (stmt, desc) = match stmts.len() {
        0 => (None, None),
        1 => {
            let stmt = stmts.into_element();
            let desc = match sql::plan(catalog, session, stmt.clone())? {
                Plan::Peek { desc, .. }
                | Plan::SendRows { desc, .. }
                | Plan::ExplainPlan { desc, .. } => Some(desc),
                Plan::CreateSources { .. } => {
                    Some(RelationDesc::empty().add_column("Topic", ScalarType::String))
                }
                _ => None,
            };
            (Some(stmt), desc)
        }
        n => bail!("expected no more than one query, got {}", n),
    };
    session.set_prepared_statement(name, PreparedStatement::new(stmt, desc));
    Ok(())
}
