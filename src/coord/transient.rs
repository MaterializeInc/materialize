// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use dataflow::{WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use futures::sync::mpsc::UnboundedReceiver;
use futures::Stream;
use std::thread;
use std::thread::JoinHandle;

use super::{coordinator, Command, Response};

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
    let mut coord =
        coordinator::Coordinator::new(switchboard.clone(), num_timely_workers, logging_config);
    let feedback_rx = coord.enable_feedback();

    let mut catalog = sql::store::Catalog::new(logging_config);

    // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
    // the first connection ID used is 1. As long as that remains the case, 0 is safe to use here.
    {
        let mut session = sql::Session::default();
        let plans = sql::handle_commands(&mut session, startup_sql, &mut catalog)?;
        plans.into_iter().for_each(|plan| {
            coord.sequence_plan(plan, 0, None /* ts_override */);
        });
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
                    let plan = sql::handle_command(&mut session, sql, &mut catalog);
                    let exec_result = plan.map(|plan| {
                        coord.sequence_plan(plan, conn_id, None /* ts_override */)
                    });
                    let _ = tx.send(Response {
                        result: exec_result,
                        session,
                    });
                }

                Message::Command(Command::Execute {
                    portal_name,
                    mut session,
                    conn_id,
                    tx,
                }) => {
                    let plan =
                        sql::handle_execute_command(&mut session, &portal_name, &mut catalog);
                    let exec_result = plan.map(|plan| {
                        coord.sequence_plan(plan, conn_id, None /* ts_override */)
                    });
                    let _ = tx.send(Response {
                        result: exec_result,
                        session,
                    });
                }

                Message::Command(Command::Parse {
                    name,
                    sql,
                    mut session,
                    tx,
                }) => {
                    let result = sql::handle_parse_command(&catalog, &mut session, sql, name);
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
