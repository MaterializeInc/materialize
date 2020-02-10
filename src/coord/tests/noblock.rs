// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
use coord::{Command, ExecuteResponse, Response};
use dataflow_types::PeekResponse;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::SinkExt;
use repr::{Datum, Row};
use sql::Session;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

#[test]
fn no_block() {
    let logging_config = None;
    let process_id = 0;

    let (switchboard, runtime) = comm::Switchboard::local().unwrap();
    let executor = runtime.handle().clone();

    let (mut cmd_tx, cmd_rx) = futures::channel::mpsc::unbounded();

    let mut coord = coord::Coordinator::new(coord::Config {
        switchboard: switchboard.clone(),
        num_timely_workers: 1,
        symbiosis_url: None,
        logging: logging_config.as_ref(),
        data_directory: None,
        executor: &executor,
        ts_channel: None,
    })
    .unwrap();

    let _coord_thread = thread::spawn(move || coord.serve(cmd_rx));

    // Since we don't do anything with the listener, connection attempts will hang forever.
    // This will be used to imitate a slow Schema Registry.
    let _listener = TcpListener::bind("0.0.0.0:9999").unwrap();

    let dataflow_workers = dataflow::serve(
        vec![None],
        1,
        process_id,
        switchboard,
        runtime.handle().clone(),
        false,
        logging_config,
    )
    .unwrap();

    let fut = async move {
        let session = Session::default();
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        cmd_tx
            .send(Command::Parse {
                name: "hang".into(),
                sql: "CREATE SOURCE foo \
                      FROM KAFKA BROKER 'localhost:9092' TOPIC 'foo' \
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:9999'"
                    .into(),
                session,
                tx: oneshot_tx,
            })
            .await
            .unwrap();

        let Response {
            result,
            mut session,
        } = oneshot_rx.await.unwrap();
        result.unwrap();

        let stmt = session.get_prepared_statement("hang").unwrap();
        let result_formats = vec![pgrepr::Format::Text; stmt.result_width()];
        session
            .set_portal("hang".into(), "hang".into(), vec![], result_formats)
            .unwrap();

        let (oneshot_tx, _) = oneshot::channel();
        cmd_tx
            .send(Command::Execute {
                portal_name: "hang".into(),
                session,
                conn_id: 0,
                tx: oneshot_tx,
            })
            .await
            .unwrap();

        // The coordinator should now be attempting to process a query that will hang forever.
        // Verify that it can still process other requests.
        let session = Session::default();
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        cmd_tx
            .send(Command::Parse {
                name: "math".into(),
                sql: "SELECT 2 + 2".into(),
                session,
                tx: oneshot_tx,
            })
            .await
            .unwrap();

        let Response {
            result,
            mut session,
        } = oneshot_rx.await.unwrap();
        result.unwrap();

        let stmt = session.get_prepared_statement("math").unwrap();
        let result_formats = vec![pgrepr::Format::Text; stmt.result_width()];
        session
            .set_portal("math".into(), "math".into(), vec![], result_formats)
            .unwrap();

        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        cmd_tx
            .send(Command::Execute {
                portal_name: "math".into(),
                session,
                conn_id: 0,
                tx: oneshot_tx,
            })
            .await
            .unwrap();

        let Response { result, session: _ } = oneshot_rx.await.unwrap();
        let response: ExecuteResponse = result.unwrap();
        let rows = match response {
            ExecuteResponse::SendRows(rows) => rows,
            _ => panic!(),
        }
        .await
        .unwrap();

        assert_eq!(
            PeekResponse::Rows(vec![Row::pack(&[Datum::Int32(4)])]),
            rows
        );
    };

    let res = tokio::runtime::Runtime::new()
        .unwrap()
        .enter(|| block_on(tokio::time::timeout(Duration::from_secs(10), fut)));
    std::mem::forget(dataflow_workers);
    res.unwrap();
}
