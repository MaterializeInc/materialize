// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use itertools::Itertools;
use mz_catalog::durable::{DurableCatalogError, FenceError};
use mz_compute_client::controller::error::{
    CollectionUpdateError, DataflowCreationError, InstanceMissing, PeekError, ReadPolicyError,
};
use mz_controller_types::ClusterId;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{exit, soft_assert_no_log};
use mz_repr::{RelationDesc, RowIterator, SqlScalarType};
use mz_sql::names::FullItemName;
use mz_sql::plan::StatementDesc;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::Var;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateIndexStatement, Ident, Raw, RawClusterName, RawItemName, Statement,
};
use mz_storage_types::controller::StorageError;
use mz_transform::TransformError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::catalog::{Catalog, CatalogState};
use crate::command::{Command, Response};
use crate::coord::{Message, PendingTxnResponse};
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, Session};
use crate::{ExecuteContext, ExecuteResponse};

/// Handles responding to clients.
#[derive(Debug)]
pub struct ClientTransmitter<T>
where
    T: Transmittable,
    <T as Transmittable>::Allowed: 'static,
{
    tx: Option<oneshot::Sender<Response<T>>>,
    internal_cmd_tx: UnboundedSender<Message>,
    /// Expresses an optional soft-assert on the set of values allowed to be
    /// sent from `self`.
    allowed: Option<&'static [T::Allowed]>,
}

impl<T: Transmittable + std::fmt::Debug> ClientTransmitter<T> {
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
    #[mz_ore::instrument(level = "debug")]
    pub fn send(mut self, result: Result<T, AdapterError>, session: Session) {
        // Guarantee that the value sent is of an allowed type.
        soft_assert_no_log!(
            match (&result, self.allowed.take()) {
                (Ok(t), Some(allowed)) => allowed.contains(&t.to_allowed()),
                _ => true,
            },
            "tried to send disallowed value {result:?} through ClientTransmitter; \
            see ClientTransmitter::set_allowed"
        );

        // If we were not able to send a message, we must clean up the session
        // ourselves. Return it to the caller for disposal.
        if let Err(res) = self
            .tx
            .take()
            .expect("tx will always be `Some` unless `self` has been consumed")
            .send(Response {
                result,
                session,
                otel_ctx: OpenTelemetryContext::obtain(),
            })
        {
            self.internal_cmd_tx
                .send(Message::Command(
                    OpenTelemetryContext::obtain(),
                    Command::Terminate {
                        conn_id: res.session.conn_id().clone(),
                        tx: None,
                    },
                ))
                .expect("coordinator unexpectedly gone");
        }
    }

    pub fn take(mut self) -> oneshot::Sender<Response<T>> {
        self.tx
            .take()
            .expect("tx will always be `Some` unless `self` has been consumed")
    }

    /// Sets `self` so that the next call to [`Self::send`] will soft-assert
    /// that, if `Ok`, the value is one of `allowed`, as determined by
    /// [`Transmittable::to_allowed`].
    pub fn set_allowed(&mut self, allowed: &'static [T::Allowed]) {
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
pub struct CompletedClientTransmitter {
    ctx: ExecuteContext,
    response: Result<PendingTxnResponse, AdapterError>,
    action: EndTransactionAction,
}

impl CompletedClientTransmitter {
    /// Creates a new completed client transmitter.
    pub fn new(
        ctx: ExecuteContext,
        response: Result<PendingTxnResponse, AdapterError>,
        action: EndTransactionAction,
    ) -> Self {
        CompletedClientTransmitter {
            ctx,
            response,
            action,
        }
    }

    /// Returns the execute context to be finalized, and the result to send it.
    pub fn finalize(mut self) -> (ExecuteContext, Result<ExecuteResponse, AdapterError>) {
        let changed = self
            .ctx
            .session_mut()
            .vars_mut()
            .end_transaction(self.action);

        // Append any parameters that changed to the response.
        let response = self.response.map(|mut r| {
            r.extend_params(changed);
            ExecuteResponse::from(r)
        });

        (self.ctx, response)
    }
}

impl<T: Transmittable> Drop for ClientTransmitter<T> {
    fn drop(&mut self) {
        if self.tx.is_some() {
            panic!("client transmitter dropped without send")
        }
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub fn index_sql(
    index_name: String,
    cluster_id: ClusterId,
    view_name: FullItemName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new_unchecked(index_name)),
        on_name: RawItemName::Name(mz_sql::normalize::unresolve(view_name)),
        in_cluster: Some(RawClusterName::Resolved(cluster_id.to_string())),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new_unchecked(n.to_string())]),
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
pub fn describe(
    catalog: &Catalog,
    stmt: Statement<Raw>,
    param_types: &[Option<SqlScalarType>],
    session: &Session,
) -> Result<StatementDesc, AdapterError> {
    let catalog = &catalog.for_session(session);
    let (stmt, _) = mz_sql::names::resolve(catalog, stmt)?;
    Ok(mz_sql::plan::describe(
        session.pcx(),
        catalog,
        stmt,
        param_types,
    )?)
}

pub trait ResultExt<T> {
    /// Like [`Result::expect`], but terminates the process with `halt` or
    /// exit code 0 instead of `panic` if the error indicates that it should
    /// cause a halt of graceful termination.
    fn unwrap_or_terminate(self, context: &str) -> T;

    /// Terminates the process with `halt` or exit code 0 if `self` is an
    /// error that should halt or cause graceful termination. Otherwise,
    /// does nothing.
    fn maybe_terminate(self, context: &str) -> Self;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: ShouldTerminateGracefully + Debug,
{
    fn unwrap_or_terminate(self, context: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) if e.should_terminate_gracefully() => exit!(0, "{context}: {e:?}"),
            Err(e) => panic!("{context}: {e:?}"),
        }
    }

    fn maybe_terminate(self, context: &str) -> Self {
        if let Err(e) = &self {
            if e.should_terminate_gracefully() {
                exit!(0, "{context}: {e:?}");
            }
        }

        self
    }
}

/// A trait for errors that should terminate gracefully rather than panic
/// the process.
trait ShouldTerminateGracefully {
    /// Reports whether the error should terminate the process gracefully
    /// rather than panic.
    fn should_terminate_gracefully(&self) -> bool;
}

impl ShouldTerminateGracefully for AdapterError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            AdapterError::Catalog(e) => e.should_terminate_gracefully(),
            _ => false,
        }
    }
}

impl ShouldTerminateGracefully for mz_catalog::memory::error::Error {
    fn should_terminate_gracefully(&self) -> bool {
        match &self.kind {
            mz_catalog::memory::error::ErrorKind::Durable(e) => e.should_terminate_gracefully(),
            _ => false,
        }
    }
}

impl ShouldTerminateGracefully for mz_catalog::durable::CatalogError {
    fn should_terminate_gracefully(&self) -> bool {
        match &self {
            Self::Durable(e) => e.should_terminate_gracefully(),
            _ => false,
        }
    }
}

impl ShouldTerminateGracefully for DurableCatalogError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            DurableCatalogError::Fence(err) => err.should_terminate_gracefully(),
            DurableCatalogError::IncompatibleDataVersion { .. }
            | DurableCatalogError::IncompatiblePersistVersion { .. }
            | DurableCatalogError::Proto(_)
            | DurableCatalogError::Uninitialized
            | DurableCatalogError::NotWritable(_)
            | DurableCatalogError::DuplicateKey
            | DurableCatalogError::UniquenessViolation
            | DurableCatalogError::Storage(_)
            | DurableCatalogError::Internal(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for FenceError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            FenceError::DeployGeneration { .. } => true,
            FenceError::Epoch { .. } | FenceError::MigrationUpper { .. } => false,
        }
    }
}

impl<T> ShouldTerminateGracefully for StorageError<T> {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            StorageError::ResourceExhausted(_)
            | StorageError::CollectionMetadataAlreadyExists(_)
            | StorageError::PersistShardAlreadyInUse(_)
            | StorageError::PersistSchemaEvolveRace { .. }
            | StorageError::PersistInvalidSchemaEvolve { .. }
            | StorageError::TxnWalShardAlreadyExists
            | StorageError::UpdateBeyondUpper(_)
            | StorageError::ReadBeforeSince(_)
            | StorageError::InvalidUppers(_)
            | StorageError::InvalidUsage(_)
            | StorageError::CollectionIdReused(_)
            | StorageError::SinkIdReused(_)
            | StorageError::IdentifierMissing(_)
            | StorageError::IdentifierInvalid(_)
            | StorageError::IngestionInstanceMissing { .. }
            | StorageError::ExportInstanceMissing { .. }
            | StorageError::Generic(_)
            | StorageError::ReadOnly
            | StorageError::DataflowError(_)
            | StorageError::InvalidAlter { .. }
            | StorageError::ShuttingDown(_)
            | StorageError::MissingSubsourceReference { .. }
            | StorageError::RtrTimeout(_)
            | StorageError::RtrDropFailure(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for DataflowCreationError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            DataflowCreationError::SinceViolation(_)
            | DataflowCreationError::InstanceMissing(_)
            | DataflowCreationError::CollectionMissing(_)
            | DataflowCreationError::ReplicaMissing(_)
            | DataflowCreationError::MissingAsOf
            | DataflowCreationError::EmptyAsOfForSubscribe
            | DataflowCreationError::EmptyAsOfForCopyTo => false,
        }
    }
}

impl ShouldTerminateGracefully for CollectionUpdateError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            CollectionUpdateError::InstanceMissing(_)
            | CollectionUpdateError::CollectionMissing(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for PeekError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            PeekError::SinceViolation(_)
            | PeekError::InstanceMissing(_)
            | PeekError::CollectionMissing(_)
            | PeekError::ReplicaMissing(_)
            | PeekError::ReadHoldIdMismatch(_)
            | PeekError::ReadHoldInsufficient(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for ReadPolicyError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            ReadPolicyError::InstanceMissing(_)
            | ReadPolicyError::CollectionMissing(_)
            | ReadPolicyError::WriteOnlyCollection(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for TransformError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            TransformError::Internal(_)
            | TransformError::IdentifierMissing(_)
            | TransformError::CallerShouldPanic(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for InstanceMissing {
    fn should_terminate_gracefully(&self) -> bool {
        false
    }
}

/// Returns the viewable session and system variables.
pub(crate) fn viewable_variables<'a>(
    catalog: &'a CatalogState,
    session: &'a dyn SessionMetadata,
) -> impl Iterator<Item = &'a dyn Var> {
    session
        .vars()
        .iter()
        .chain(catalog.system_config().iter())
        .filter(|v| v.visible(session.user(), catalog.system_config()).is_ok())
}

/// Verify that the rows in [`RowIterator`] match the expected [`RelationDesc`].
pub fn verify_datum_desc(
    desc: &RelationDesc,
    rows: &mut dyn RowIterator,
) -> Result<(), AdapterError> {
    // Verify the first row is of the expected type. This is often good enough to
    // find problems.
    //
    // Notably it failed to find database-issues#1946 when "FETCH 2" was used in a test, instead
    // we had to use "FETCH 1" twice.

    let Some(row) = rows.peek() else {
        return Ok(());
    };

    let datums = row.unpack();
    let col_types = &desc.typ().column_types;
    if datums.len() != col_types.len() {
        let msg = format!(
            "internal error: row descriptor has {} columns but row has {} columns",
            col_types.len(),
            datums.len(),
        );
        return Err(AdapterError::Internal(msg));
    }

    for (i, (d, t)) in datums.iter().zip_eq(col_types).enumerate() {
        if !d.is_instance_of_sql(t) {
            let msg = format!(
                "internal error: column {} is not of expected type {:?}: {:?}",
                i, t, d
            );
            return Err(AdapterError::Internal(msg));
        }
    }

    Ok(())
}
