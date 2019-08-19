// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A trivial single-node command queue that doesn't store state at all.

use futures::Stream;

use crate::sql::{self, SqlCommand, SqlResult};
use dataflow::DataflowCommand;
use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use ore::mpmc::Mux;

pub fn serve(
    sql_command_receiver: UnboundedReceiver<SqlCommand>,
    sql_result_mux: Mux<SqlResult>,
    dataflow_command_sender: UnboundedSender<DataflowCommand>,
    worker0_thread: std::thread::Thread,
) {
    std::thread::spawn(move || {
        let mut planner = sql::Planner::default();
        for msg in sql_command_receiver.wait() {
            let mut cmd = msg.unwrap();

            let (sql_result, dataflow_command) =
                match planner.handle_command(&mut cmd.session, cmd.connection_uuid, cmd.sql) {
                    Ok((resp, cmd)) => (Ok(resp), cmd),
                    Err(err) => (Err(err), None),
                };

            if let Some(dataflow_command) = dataflow_command {
                dataflow_command_sender
                    .unbounded_send(dataflow_command.clone())
                    // if the dataflow server has gone down, just explode
                    .unwrap();

                worker0_thread.unpark();
            }

            // the response sender is allowed disappear at any time, so the error handling here is deliberately relaxed
            if let Ok(sender) = sql_result_mux.read().unwrap().sender(&cmd.connection_uuid) {
                drop(sender.unbounded_send(SqlResult {
                    result: sql_result,
                    session: cmd.session,
                }));
            }
        }
    });
}
