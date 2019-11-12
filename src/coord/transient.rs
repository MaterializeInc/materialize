// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use dataflow::{WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{Source, SourceConnector};
use failure::bail;
use futures::sync::mpsc::UnboundedReceiver;
use futures::{stream, Stream};
use ore::collections::CollectionExt;
use sql::store::{Catalog, CatalogItem};
use sql::PreparedStatement;
use sql::{Plan, Session};
use std::thread;
use std::thread::JoinHandle;
use symbiosis::Postgres;

use super::{coordinator::Coordinator, Command, ExecuteResponse, Response};

enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    Shutdown,
}

pub struct Config<'a, C>
where
    C: comm::Connection,
{
    pub switchboard: comm::Switchboard<C>,
    pub num_timely_workers: usize,
    pub symbiosis_url: Option<&'a str>,
    pub logging: Option<&'a LoggingConfig>,
    pub bootstrap_sql: String,
    pub cmd_rx: UnboundedReceiver<Command>,
}

pub fn serve<'a, C>(config: Config<'a, C>) -> Result<JoinHandle<()>, failure::Error>
where
    C: comm::Connection,
{
    let mut coord = Coordinator::new(
        config.switchboard.clone(),
        config.num_timely_workers,
        config.logging,
        config.symbiosis_url.is_some(),
    );

    let mut catalog = sql::store::Catalog::new(config.logging);

    let mut postgres = if let Some(symbiosis_url) = config.symbiosis_url {
        Some(symbiosis::Postgres::open_and_erase(symbiosis_url)?)
    } else {
        None
    };

    {
        // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
        // the first connection ID used is 1. As long as that remains the case,
        // 0 is safe to use here.
        let conn_id = 0;
        let mut session = sql::Session::default();
        for stmt in sql::parse(config.bootstrap_sql)? {
            handle_statement(
                &mut coord,
                postgres.as_mut(),
                &mut catalog,
                &mut session,
                stmt,
                None,
                conn_id,
            )?;
        }
    }

    let feedback_rx = coord.enable_feedback();
    // NOTE: returning an error from this point on (i.e., after calling
    // `coord.enable_feedback()`) will be fatal, as dropping `feedback_rx` will
    // cause the worker threads to panic.
    let messages = config
        .cmd_rx
        .map(Message::Command)
        .map_err(|()| unreachable!())
        .chain(stream::once(Ok(Message::Shutdown)))
        .select(feedback_rx.map(Message::Worker));

    Ok(thread::spawn(move || {
        let mut messages = messages.wait();
        for msg in messages.by_ref() {
            match msg.expect("coordinator message receiver failed") {
                Message::Command(Command::Execute {
                    portal_name,
                    mut session,
                    conn_id,
                    tx,
                }) => {
                    let result = handle_execute(
                        &mut coord,
                        postgres.as_mut(),
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
                    let result = handle_parse(postgres.as_mut(), &catalog, &mut session, name, sql);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::CancelRequest { conn_id }) => {
                    coord.sequence_cancel(conn_id);
                }

                Message::Shutdown => {
                    coord.shutdown();
                    break;
                }

                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id: _,
                    message: WorkerFeedback::FrontierUppers(updates),
                }) => {
                    for (name, changes) in updates {
                        coord.update_upper(&name, changes);
                    }
                    coord.maintenance();
                }
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        for msg in messages {
            match msg.expect("coordinator message receiver failed") {
                Message::Command(_) | Message::Shutdown => unreachable!(),
                Message::Worker(_) => (),
            }
        }
    }))
}

fn apply_plan(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &Plan,
) -> Result<(), failure::Error> {
    match plan {
        Plan::CreateView(view) => catalog.insert(CatalogItem::View(view.clone()))?,
        Plan::CreateTable { name, desc } => catalog.insert(CatalogItem::Source(Source {
            name: name.clone(),
            connector: SourceConnector::Local,
            desc: desc.clone(),
        }))?,
        Plan::CreateSource(source) => catalog.insert(CatalogItem::Source(source.clone()))?,
        Plan::CreateSources(sources) => {
            for source in sources {
                catalog.insert(CatalogItem::Source(source.clone()))?
            }
        }
        Plan::CreateSink(sink) => catalog.insert(CatalogItem::Sink(sink.clone()))?,
        Plan::DropItems(names, _item_type) => {
            for name in names {
                catalog.remove(name);
            }
        }
        Plan::SetVariable { name, value } => session.set(name, value)?,
        Plan::StartTransaction => session.start_transaction(),
        Plan::Commit | Plan::Rollback => session.end_transaction(),
        _ => (),
    }
    Ok(())
}

fn handle_statement<C>(
    coord: &mut Coordinator<C>,
    mut postgres: Option<&mut Postgres>,
    catalog: &mut Catalog,
    session: &mut Session,
    stmt: sql::Statement,
    portal_name: Option<String>,
    conn_id: u32,
) -> Result<ExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    sql::plan(catalog, session, stmt.clone(), portal_name)
        .or_else(|err| {
            // Executing the query failed. If we're running in symbiosis with
            // Postgres, see if Postgres can handle it.
            match postgres {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => postgres.execute(&stmt),
                _ => Err(err),
            }
        })
        .and_then(|plan| {
            apply_plan(catalog, session, &plan)?;
            Ok(plan)
        })
        .map(|plan| coord.sequence_plan(plan, conn_id))
}

fn handle_execute<C>(
    coord: &mut Coordinator<C>,
    postgres: Option<&mut Postgres>,
    catalog: &mut Catalog,
    session: &mut Session,
    portal_name: String,
    conn_id: u32,
) -> Result<ExecuteResponse, failure::Error>
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
            handle_statement(
                coord,
                postgres,
                catalog,
                session,
                stmt,
                Some(portal_name),
                conn_id,
            )
        }
        None => Ok(ExecuteResponse::EmptyQuery),
    }
}

fn handle_parse(
    postgres: Option<&mut Postgres>,
    catalog: &Catalog,
    session: &mut Session,
    name: String,
    sql: String,
) -> Result<(), failure::Error> {
    let stmts = sql::parse(sql)?;
    let (stmt, desc, param_types) = match stmts.len() {
        0 => (None, None, vec![]),
        1 => {
            let stmt = stmts.into_element();
            let (desc, param_types) = match sql::describe(catalog, stmt.clone()) {
                Ok((desc, param_types)) => (desc, param_types),
                // Describing the query failed. If we're running in symbiosis with
                // Postgres, see if Postgres can handle it. Note that Postgres
                // only handles commands that do not return rows, so the
                // `RelationDesc` is always `None`.
                Err(err) => match postgres {
                    Some(ref postgres) if postgres.can_handle(&stmt) => (None, vec![]),
                    _ => return Err(err),
                },
            };
            (Some(stmt), desc, param_types)
        }
        n => bail!("expected no more than one query, got {}", n),
    };
    session.set_prepared_statement(name, PreparedStatement::new(stmt, desc, param_types));
    Ok(())
}
