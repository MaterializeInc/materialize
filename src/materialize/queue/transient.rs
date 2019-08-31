// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use dataflow::DataflowCommand;
use dataflow_types::logging::LoggingConfig;
use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::Stream;

use super::{translate_plan, Command, Response};

pub fn serve(
    logging_config: Option<&LoggingConfig>,
    cmd_rx: UnboundedReceiver<Command>,
    dataflow_command_sender: UnboundedSender<DataflowCommand>,
    worker0_thread: std::thread::Thread,
) {
    let mut planner = sql::Planner::new(logging_config);
    std::thread::spawn(move || {
        for msg in cmd_rx.wait() {
            let mut cmd = msg.unwrap();

            let (sql_result, dataflow_command) =
                match planner.handle_command(&mut cmd.session, cmd.sql) {
                    Ok(plan) => {
                        let (sql_response, dataflow_command) = translate_plan(plan, cmd.conn_id);
                        (Ok(sql_response), dataflow_command)
                    }
                    Err(err) => (Err(err), None),
                };

            if let Some(dataflow_command) = dataflow_command {
                dataflow_command_sender
                    .unbounded_send(dataflow_command.clone())
                    // if the dataflow server has gone down, just explode
                    .unwrap();

                worker0_thread.unpark();
            }

            // The client connection may disappear at any time, so the error
            // handling here is deliberately relaxed.
            let _ = cmd.tx.send(Response {
                sql_result,
                session: cmd.session,
            });
        }
    });
}
