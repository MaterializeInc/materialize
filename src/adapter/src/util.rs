// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use mz_compute_client::controller::ComputeInstanceId;
use mz_repr::{RelationDesc, Row, ScalarType, Timestamp};
use mz_sql::names::FullObjectName;
use mz_sql::plan::StatementDesc;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateIndexStatement, FetchStatement, Ident, Raw, RawClusterName, RawObjectName, Statement,
};
use mz_stash::Append;

use crate::catalog::Catalog;
use crate::command::{Command, Response};
use crate::coord::Message;
use crate::error::AdapterError;
use crate::session::Session;
use crate::{ExecuteResponse, PeekResponseUnary};

/// Handles responding to clients.
pub struct ClientTransmitter<T> {
    tx: Option<oneshot::Sender<Response<T>>>,
    internal_cmd_tx: UnboundedSender<Message>,
}

impl<T> ClientTransmitter<T> {
    /// Creates a new client transmitter.
    pub fn new(
        tx: oneshot::Sender<Response<T>>,
        internal_cmd_tx: UnboundedSender<Message>,
    ) -> ClientTransmitter<T> {
        ClientTransmitter {
            tx: Some(tx),
            internal_cmd_tx,
        }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    pub fn send(mut self, result: Result<T, AdapterError>, session: Session) {
        // If we were not able to send a message, we must clean up the session
        // ourselves. Return it to the caller for disposal.
        if let Err(res) = self.tx.take().unwrap().send(Response { result, session }) {
            self.internal_cmd_tx
                .send(Message::Command(Command::Terminate {
                    session: res.session,
                }))
                .expect("coordinator unexpectedly gone");
        }
    }

    pub fn take(mut self) -> oneshot::Sender<Response<T>> {
        self.tx.take().unwrap()
    }
}

impl<T> Drop for ClientTransmitter<T> {
    fn drop(&mut self) {
        if self.tx.is_some() {
            panic!("client transmitter dropped without send")
        }
    }
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
pub(crate) fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    ExecuteResponse::SendingRows {
        future: Box::pin(async { PeekResponseUnary::Rows(rows) }),
        span: tracing::Span::none(),
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub fn index_sql(
    index_name: String,
    compute_instance: ComputeInstanceId,
    view_name: FullObjectName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: RawObjectName::Name(mz_sql::normalize::unresolve(view_name)),
        in_cluster: Some(RawClusterName::Resolved(compute_instance.to_string())),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        with_options: vec![],
        if_not_exists: false,
    }
    .to_ast_string_stable()
}

/// Converts a Duration to a Timestamp representing the number
/// of milliseconds contained in that Duration
pub(crate) fn duration_to_timestamp_millis(d: Duration) -> Timestamp {
    let millis = d.as_millis();
    if millis > Timestamp::MAX as u128 {
        Timestamp::MAX
    } else if millis < Timestamp::MIN as u128 {
        Timestamp::MIN
    } else {
        millis as Timestamp
    }
}

/// Creates a description of the statement `stmt`.
///
/// This function is identical to sql::plan::describe except this is also
/// supports describing FETCH statements which need access to bound portals
/// through the session.
pub fn describe<S: Append>(
    catalog: &Catalog<S>,
    stmt: Statement<Raw>,
    param_types: &[Option<ScalarType>],
    session: &Session,
) -> Result<StatementDesc, AdapterError> {
    match stmt {
        // FETCH's description depends on the current session, which describe_statement
        // doesn't (and shouldn't?) have access to, so intercept it here.
        Statement::Fetch(FetchStatement { ref name, .. }) => {
            // Unverified portal is ok here because Coordinator::execute will verify the
            // named portal during execution.
            match session
                .get_portal_unverified(name.as_str())
                .map(|p| p.desc.clone())
            {
                Some(desc) => Ok(desc),
                None => Err(AdapterError::UnknownCursor(name.to_string())),
            }
        }
        _ => {
            let catalog = &catalog.for_session(session);
            let (stmt, _) = mz_sql::names::resolve(catalog, stmt)?;
            Ok(mz_sql::plan::describe(
                &session.pcx(),
                catalog,
                stmt,
                param_types,
            )?)
        }
    }
}
