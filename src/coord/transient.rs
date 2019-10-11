// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use dataflow::{WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use futures::sync::mpsc::UnboundedReceiver;
use futures::Stream;

use super::{coordinator, Command, CommandKind, Response};

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
) -> Result<(), failure::Error>
where
    C: comm::Connection,
{
    let mut coord =
        coordinator::Coordinator::new(switchboard.clone(), num_timely_workers, logging_config);
    let feedback_rx = coord.enable_feedback();

    let mut planner = sql::Planner::new(logging_config);

    // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
    // the first connection ID used is 1. As long as that remains the case, 0 is safe to use here.
    let mut session = sql::Session::default();
    let plans = planner.handle_commands(&mut session, startup_sql)?;
    plans.into_iter().for_each(|plan| {
        coord.sequence_plan(plan, 0, None /* ts_override */);
    });

    let messages = cmd_rx
        .map(Message::Command)
        .map_err(|()| unreachable!())
        .select(feedback_rx.map(Message::Worker));

    std::thread::spawn(move || {
        for msg in messages.wait() {
            match msg.unwrap() {
                Message::Command(mut cmd) => {
                    let conn_id = cmd.conn_id;
                    let sql_result = match cmd.kind {
                        CommandKind::Query { sql } => planner.handle_command(&mut cmd.session, sql),
                        CommandKind::Parse { sql, name } => {
                            planner.handle_parse_command(&mut cmd.session, sql, name)
                        }
                        CommandKind::Execute { portal_name } => {
                            planner.handle_execute_command(&cmd.session, &portal_name)
                        }
                    }
                    .map(|plan| {
                        coord.sequence_plan(plan, conn_id, None /* ts_override */)
                    });

                    // The client connection may disappear at any time, so the error
                    // handling here is deliberately relaxed.
                    let _ = cmd.tx.send(Response {
                        sql_result,
                        session: cmd.session,
                    });
                }
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
    });

    Ok(())
}
