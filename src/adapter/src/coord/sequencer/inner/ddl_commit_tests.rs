// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_sql::session::vars::{SystemVars, VarInput};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::DdlCommitContext;
use crate::AdapterError;
use crate::command::{ExecuteResponse, Response};
use crate::coord::{ExecuteContext, ExecuteContextGuard, Message, StagedContext};
use crate::session::{EndTransactionAction, Session};
use crate::statement_logging::{StatementEndedExecutionReason, StatementLoggingId};
use crate::util::ClientTransmitter;

async fn complete_ddl_commit(
    result: Result<ExecuteResponse, AdapterError>,
) -> Response<ExecuteResponse> {
    let mut session = Session::dummy();
    let session_id = session.uuid();
    let system_vars = SystemVars::new();
    let vars = session.vars_mut();
    vars.set(
        &system_vars,
        "application_name",
        VarInput::Flat("before"),
        false,
    )
    .expect("set baseline");
    vars.end_transaction(EndTransactionAction::Commit);
    vars.set(
        &system_vars,
        "application_name",
        VarInput::Flat("committed"),
        false,
    )
    .expect("transactional SET");
    vars.set(
        &system_vars,
        "application_name",
        VarInput::Flat("local"),
        true,
    )
    .expect("SET LOCAL");
    assert_eq!(vars.application_name(), "local");

    // DDL completion receives the session after its transaction was extracted,
    // with transactional variable changes still pending.
    let (client_tx, mut client_rx) = oneshot::channel();
    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
    let (release, barrier) = oneshot::channel::<()>();
    let (entered_tx, entered_rx) = oneshot::channel();
    let logging_id = StatementLoggingId(Uuid::new_v4());
    let ctx = DdlCommitContext(ExecuteContext::from_parts_with_response_barriers(
        ClientTransmitter::new(client_tx, internal_tx.clone()),
        internal_tx.clone(),
        session,
        ExecuteContextGuard::new(Some(logging_id), internal_tx),
        vec![Box::pin(async move {
            entered_tx.send(()).expect("barrier observer");
            barrier.await.expect("release barrier");
        })],
    ));
    match result {
        Ok(response) => ctx.retire(Ok(response)),
        Err(error) => ctx.handle_error(error),
    }

    // Synchronize with the actual barrier wait rather than relying on a yield.
    entered_rx.await.expect("response barrier must be polled");
    assert!(matches!(
        client_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        internal_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    release.send(()).expect("response barrier retained");
    let response = client_rx.await.expect("final client response");
    assert_eq!(response.session.uuid(), session_id);
    let Message::RetireExecute { data, reason, .. } =
        internal_rx.recv().await.expect("logging retirement")
    else {
        panic!("DDL completion must retire, not replan");
    };
    assert_eq!(data.contents(), Some(logging_id));
    match (&response.result, reason) {
        (Ok(_), StatementEndedExecutionReason::Success { .. }) => (),
        (Err(expected), StatementEndedExecutionReason::Errored { error }) => {
            assert_eq!(error, expected.to_string());
        }
        (_, reason) => panic!("unexpected logging outcome: {reason:?}"),
    }
    assert!(
        internal_rx.recv().await.is_none(),
        "exactly one retirement, with no outstanding execution sender"
    );
    response
}

#[mz_ore::test(tokio::test)]
async fn successful_commit_finishes_vars_and_merges_parameter_updates() {
    let mut response = complete_ddl_commit(Ok(ExecuteResponse::TransactionCommitted {
        params: BTreeMap::from([("client_encoding", "UTF8".to_string())]),
    }))
    .await;
    let ExecuteResponse::TransactionCommitted { params } =
        response.result.expect("commit succeeds")
    else {
        panic!("expected COMMIT response");
    };
    assert_eq!(
        params,
        BTreeMap::from([
            ("application_name", "committed".to_string()),
            ("client_encoding", "UTF8".to_string()),
        ])
    );
    let vars = response.session.vars_mut();
    assert_eq!(vars.application_name(), "committed");
    assert!(
        vars.end_transaction(EndTransactionAction::Rollback)
            .is_empty()
    );
    assert_eq!(vars.application_name(), "committed");
}

#[mz_ore::test(tokio::test)]
async fn failed_or_canceled_commit_rolls_back_vars_and_preserves_error() {
    for error in [AdapterError::DDLTransactionRace, AdapterError::Canceled] {
        let expected = std::mem::discriminant(&error);
        let mut response = complete_ddl_commit(Err(error)).await;
        assert_eq!(
            std::mem::discriminant(&response.result.expect_err("commit fails")),
            expected
        );
        let vars = response.session.vars_mut();
        assert_eq!(vars.application_name(), "before");
        assert!(
            vars.end_transaction(EndTransactionAction::Commit)
                .is_empty()
        );
        assert_eq!(vars.application_name(), "before");
    }
}
