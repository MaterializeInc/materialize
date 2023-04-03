// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use mz_compute_client::controller::error::{
    CollectionUpdateError, DataflowCreationError, InstanceMissing, PeekError, SubscribeTargetError,
};
use mz_controller::clusters::ClusterId;
use mz_ore::halt;
use mz_ore::soft_assert;
use mz_repr::{GlobalId, RelationDesc, Row, ScalarType};
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::FullObjectName;
use mz_sql::plan::{StatementDesc, StatementTagger};
use mz_sql::session::vars::Var;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateIndexStatement, FetchStatement, Ident, Raw, RawClusterName, RawObjectName, Statement,
};
use mz_stash::StashError;
use mz_storage_client::controller::StorageError;
use mz_transform::TransformError;

use crate::catalog::{Catalog, CatalogState};
use crate::command::{Command, Response};
use crate::coord::Message;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, Session};
use crate::{ExecuteResponse, PeekResponseUnary};

/// Handles responding to clients.
#[derive(Debug)]
pub struct ClientTransmitter<T: Transmittable> {
    tx: Option<oneshot::Sender<Response<T>>>,
    internal_cmd_tx: UnboundedSender<Message>,
    /// Expresses an optional [`soft_assert`] on the set of values allowed to be
    /// sent from `self`.
    allowed: Option<Vec<T::Allowed>>,
}

impl<T: Transmittable> ClientTransmitter<T> {
    /// Creates a new client transmitter.
    pub fn new(
        tx: oneshot::Sender<Response<T>>,
        internal_cmd_tx: UnboundedSender<Message>,
    ) -> ClientTransmitter<T> {
        ClientTransmitter {
            tx: Some(tx),
            internal_cmd_tx,
            allowed: None,
        }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    ///
    /// # Panics
    /// - If in `soft_assert`, `result.is_ok()`, `self.allowed.is_some()`, and
    ///   the result value is not in the set of allowed values.
    pub fn send(mut self, result: Result<T, AdapterError>, session: Session) {
        // Guarantee that the value sent is of an allowed type.
        soft_assert!(
            match (&result, self.allowed.take()) {
                (Ok(ref t), Some(allowed)) => allowed.contains(&t.to_allowed()),
                _ => true,
            },
            "tried to send disallowed value through ClientTransmitter; \
            see ClientTransmitter::set_allowed"
        );

        // If we were not able to send a message, we must clean up the session
        // ourselves. Return it to the caller for disposal.
        if let Err(res) = self
            .tx
            .take()
            .expect("tx will always be `Some` unless `self` has been consumed")
            .send(Response { result, session })
        {
            self.internal_cmd_tx
                .send(Message::Command(Command::Terminate {
                    session: res.session,
                    tx: None,
                }))
                .expect("coordinator unexpectedly gone");
        }
    }

    pub fn take(mut self) -> oneshot::Sender<Response<T>> {
        self.tx
            .take()
            .expect("tx will always be `Some` unless `self` has been consumed")
    }

    /// Sets `self` so that the next call to [`Self::send`] will [`soft_assert`]
    /// that, if `Ok`, the value is one of `allowed`, as determined by
    /// [`Transmittable::to_allowed`].
    pub fn set_allowed(&mut self, allowed: Vec<T::Allowed>) {
        self.allowed = Some(allowed);
    }
}

/// A helper trait for [`ClientTransmitter`].
pub trait Transmittable {
    /// The type of values used to express which set of values are allowed.
    type Allowed: Eq + PartialEq + std::fmt::Debug;
    /// The conversion from the [`ClientTransmitter`]'s type to `Allowed`.
    ///
    /// The benefit of this style of trait, rather than relying on a bound on
    /// `Allowed`, are:
    /// - Not requiring a clone
    /// - The flexibility for facile implementations that do not plan to make
    ///   use of the `allowed` feature. Those types can simply implement this
    ///   trait for `bool`, and return `true`. However, it might not be
    ///   semantically appropriate to expose `From<&Self> for bool`.
    fn to_allowed(&self) -> Self::Allowed;
}

impl Transmittable for () {
    type Allowed = bool;

    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

/// `ClientTransmitter` with a response to send.
#[derive(Debug)]
pub struct CompletedClientTransmitter<T: Transmittable> {
    client_transmitter: ClientTransmitter<T>,
    response: Result<T, AdapterError>,
    session: Session,
    action: EndTransactionAction,
}

impl<T: Transmittable> CompletedClientTransmitter<T> {
    /// Creates a new completed client transmitter.
    pub fn new(
        client_transmitter: ClientTransmitter<T>,
        response: Result<T, AdapterError>,
        session: Session,
        action: EndTransactionAction,
    ) -> Self {
        CompletedClientTransmitter {
            client_transmitter,
            response,
            session,
            action,
        }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    pub fn send(mut self) {
        self.session.vars_mut().end_transaction(self.action);
        self.client_transmitter.send(self.response, self.session);
    }
}

impl<T: Transmittable> Drop for ClientTransmitter<T> {
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
    cluster_id: ClusterId,
    view_name: FullObjectName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: RawObjectName::Name(mz_sql::normalize::unresolve(view_name)),
        in_cluster: Some(RawClusterName::Resolved(cluster_id.to_string())),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier {
                        names: vec![Ident::new(n.to_string())],
                        id: (),
                    },
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        with_options: vec![],
        if_not_exists: false,
    }
    .to_ast_string_stable()
}

/// Creates a description of the statement `stmt`.
///
/// This function is identical to sql::plan::describe except this is also
/// supports describing FETCH statements which need access to bound portals
/// through the session.
pub fn describe(
    catalog: &Catalog,
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
            let mut statement_tagger =
                StatementTagger::new(catalog.system_vars().enable_disambiguate_columns());
            let (stmt, _) = mz_sql::names::resolve(catalog, &mut statement_tagger, stmt)?;
            Ok(mz_sql::plan::describe(
                session.pcx(),
                catalog,
                stmt,
                param_types,
                statement_tagger,
            )?)
        }
    }
}

/// Type identifying a sink maintained by a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComputeSinkId {
    pub cluster_id: ClusterId,
    pub global_id: GlobalId,
}

pub trait ResultExt<T> {
    // Like [`Result::expect`], but terminates the process with `halt` instead
    // of `panic` if the underlying error is a condition that should halt the
    // rather than panic the process.
    fn unwrap_or_terminate(self, context: &str) -> T;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: ShouldHalt + Debug,
{
    fn unwrap_or_terminate(self, context: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) if e.should_halt() => halt!("{context}: {e:?}"),
            Err(e) => panic!("{context}: {e:?}"),
        }
    }
}

/// A trait for errors that should halt rather than panic the process.
trait ShouldHalt {
    /// Reports whether the error should halt rather than panic the process.
    fn should_halt(&self) -> bool;
}

impl ShouldHalt for AdapterError {
    fn should_halt(&self) -> bool {
        match self {
            AdapterError::Catalog(e) => e.should_halt(),
            _ => false,
        }
    }
}

impl ShouldHalt for crate::catalog::Error {
    fn should_halt(&self) -> bool {
        match &self.kind {
            crate::catalog::ErrorKind::Stash(e) => e.should_halt(),
            _ => false,
        }
    }
}

impl ShouldHalt for StashError {
    fn should_halt(&self) -> bool {
        self.is_unrecoverable()
    }
}

impl ShouldHalt for StorageError {
    fn should_halt(&self) -> bool {
        match self {
            StorageError::UpdateBeyondUpper(_)
            | StorageError::ReadBeforeSince(_)
            | StorageError::InvalidUppers(_)
            | StorageError::InvalidUsage(_) => true,
            StorageError::SourceIdReused(_)
            | StorageError::SinkIdReused(_)
            | StorageError::IdentifierMissing(_)
            | StorageError::ClientError(_)
            | StorageError::DataflowError(_) => false,
            StorageError::IOError(e) => e.should_halt(),
        }
    }
}

impl ShouldHalt for DataflowCreationError {
    fn should_halt(&self) -> bool {
        match self {
            DataflowCreationError::SinceViolation(_) => true,
            DataflowCreationError::InstanceMissing(_)
            | DataflowCreationError::CollectionMissing(_)
            | DataflowCreationError::MissingAsOf => false,
        }
    }
}

impl ShouldHalt for CollectionUpdateError {
    fn should_halt(&self) -> bool {
        match self {
            CollectionUpdateError::InstanceMissing(_)
            | CollectionUpdateError::CollectionMissing(_) => false,
        }
    }
}

impl ShouldHalt for PeekError {
    fn should_halt(&self) -> bool {
        match self {
            PeekError::SinceViolation(_) => true,
            PeekError::InstanceMissing(_)
            | PeekError::CollectionMissing(_)
            | PeekError::ReplicaMissing(_) => false,
        }
    }
}

impl ShouldHalt for SubscribeTargetError {
    fn should_halt(&self) -> bool {
        match self {
            SubscribeTargetError::InstanceMissing(_)
            | SubscribeTargetError::SubscribeMissing(_)
            | SubscribeTargetError::ReplicaMissing(_)
            | SubscribeTargetError::SubscribeAlreadyStarted => false,
        }
    }
}

impl ShouldHalt for TransformError {
    fn should_halt(&self) -> bool {
        match self {
            TransformError::Internal(_)
            | TransformError::LetRecUnsupported
            | TransformError::IdentifierMissing(_) => false,
        }
    }
}

impl ShouldHalt for InstanceMissing {
    fn should_halt(&self) -> bool {
        false
    }
}

/// Returns the viewable session and system variables.
pub(crate) fn viewable_variables<'a>(
    catalog: &'a CatalogState,
    session: &'a Session,
) -> impl Iterator<Item = &'a dyn Var> {
    session
        .vars()
        .iter()
        .chain(catalog.system_config().iter())
        .filter(|v| !v.experimental() && v.visible(session.user()))
        .filter(|v| v.safe() || catalog.unsafe_mode())
}
