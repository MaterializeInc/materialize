// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-connection configuration parameters and state.

#![warn(missing_docs)]

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use chrono::{DateTime, Utc};
use derivative::Derivative;
use rand::Rng;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::OwnedMutexGuard;

use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_pgrepr::Format;
use mz_repr::{Datum, Diff, GlobalId, Row, ScalarType, TimestampManipulation};
use mz_sql::ast::{Raw, Statement, TransactionAccessMode};
use mz_sql::names::RoleId;
use mz_sql::plan::{Params, PlanContext, StatementDesc};
use mz_sql::session::user::{ExternalUserMetadata, User, INTERNAL_USER_NAMES, SYSTEM_USER};
use mz_sql::session::vars::{IsolationLevel, VarInput};
use mz_sql_parser::ast::TransactionIsolationLevel;
use mz_storage_client::types::sources::Timeline;

use crate::client::ConnectionId;
use crate::coord::peek::PeekResponseUnary;
use crate::coord::timestamp_selection::TimestampContext;
use crate::error::AdapterError;
use crate::AdapterNotice;

pub use mz_sql::session::vars::{
    EndTransactionAction, SessionVars, DEFAULT_DATABASE_NAME, SERVER_MAJOR_VERSION,
    SERVER_MINOR_VERSION, SERVER_PATCH_VERSION,
};

const DUMMY_CONNECTION_ID: ConnectionId = 0;

/// A session holds per-connection state.
#[derive(Debug)]
pub struct Session<T = mz_repr::Timestamp> {
    conn_id: ConnectionId,
    prepared_statements: BTreeMap<String, PreparedStatement>,
    portals: BTreeMap<String, Portal>,
    transaction: TransactionStatus<T>,
    pcx: Option<PlanContext>,
    /// The role ID of the current user.
    ///
    /// Invariant: role_id must be `Some` after the user has
    /// successfully connected to and authenticated with Materialize.
    ///
    /// Prefer using this value over [`Self.user.name`].
    //
    // It would be better for this not to be an Option, but the
    // `Session` is initialized before the user has connected to
    // Materialize and is able to look up the `RoleId`. The `Session`
    // is also used to return an error when no role exists and
    // therefore there is no valid `RoleId`.
    role_id: Option<RoleId>,
    vars: SessionVars,
    notices_tx: mpsc::UnboundedSender<AdapterNotice>,
    notices_rx: mpsc::UnboundedReceiver<AdapterNotice>,
    next_transaction_id: TransactionId,
    secret_key: u32,
    external_metadata_tx: mpsc::UnboundedSender<ExternalUserMetadata>,
    external_metadata_rx: mpsc::UnboundedReceiver<ExternalUserMetadata>,
}

impl<T: TimestampManipulation> Session<T> {
    /// Creates a new session for the specified connection ID.
    pub(crate) fn new(
        build_info: &'static BuildInfo,
        conn_id: ConnectionId,
        user: User,
    ) -> Session<T> {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Self::new_internal(build_info, conn_id, user)
    }

    /// Creates a new dummy session.
    ///
    /// Dummy sessions are intended for use when executing queries on behalf of
    /// the system itself, rather than on behalf of a user.
    pub fn dummy() -> Session<T> {
        let mut dummy =
            Self::new_internal(&DUMMY_BUILD_INFO, DUMMY_CONNECTION_ID, SYSTEM_USER.clone());
        dummy.set_role_id(RoleId::User(0));
        dummy
    }

    fn new_internal(
        build_info: &'static BuildInfo,
        conn_id: ConnectionId,
        user: User,
    ) -> Session<T> {
        let (notices_tx, notices_rx) = mpsc::unbounded_channel();
        let (external_metadata_tx, external_metadata_rx) = mpsc::unbounded_channel();
        let default_cluster = INTERNAL_USER_NAMES
            .contains(&user.name)
            .then(|| user.name.clone());
        let mut vars = SessionVars::new(build_info, user);
        if let Some(default_cluster) = default_cluster {
            vars.set_cluster(default_cluster);
        }
        Session {
            conn_id,
            transaction: TransactionStatus::Default,
            pcx: None,
            prepared_statements: BTreeMap::new(),
            portals: BTreeMap::new(),
            role_id: None,
            vars,
            notices_tx,
            notices_rx,
            next_transaction_id: 0,
            secret_key: rand::thread_rng().gen(),
            external_metadata_tx,
            external_metadata_rx,
        }
    }

    /// Returns the connection ID associated with the session.
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    /// Returns the secret key associated with the session.
    pub fn secret_key(&self) -> ConnectionId {
        self.secret_key
    }

    /// Returns the current transaction's PlanContext. Panics if there is not a
    /// current transaction.
    pub fn pcx(&self) -> &PlanContext {
        &self
            .transaction()
            .inner()
            .expect("no active transaction")
            .pcx
    }

    /// Starts an explicit transaction, or changes an implicit to an explicit
    /// transaction.
    pub fn start_transaction(
        mut self,
        wall_time: DateTime<Utc>,
        access: Option<TransactionAccessMode>,
        isolation_level: Option<TransactionIsolationLevel>,
    ) -> (Self, Result<(), AdapterError>) {
        // Check that current transaction state is compatible with new `access`
        if let Some(txn) = self.transaction.inner() {
            // `READ WRITE` prohibited if:
            // - Currently in `READ ONLY`
            // - Already performed a query
            let read_write_prohibited = match txn.ops {
                TransactionOps::Peeks(_) | TransactionOps::Subscribe => {
                    txn.access == Some(TransactionAccessMode::ReadOnly)
                }
                TransactionOps::None | TransactionOps::Writes(_) => false,
            };

            if read_write_prohibited && access == Some(TransactionAccessMode::ReadWrite) {
                return (self, Err(AdapterError::ReadWriteUnavailable));
            }
        }

        match self.transaction {
            TransactionStatus::Default => {
                let id = self.next_transaction_id;
                self.next_transaction_id = self.next_transaction_id.wrapping_add(1);
                self.transaction = TransactionStatus::InTransaction(Transaction {
                    pcx: PlanContext::new(wall_time),
                    ops: TransactionOps::None,
                    write_lock_guard: None,
                    access,
                    id,
                });
            }
            TransactionStatus::Started(mut txn)
            | TransactionStatus::InTransactionImplicit(mut txn)
            | TransactionStatus::InTransaction(mut txn) => {
                if access.is_some() {
                    txn.access = access;
                }
                self.transaction = TransactionStatus::InTransaction(txn);
            }
            TransactionStatus::Failed(_) => unreachable!(),
        };

        if let Some(isolation_level) = isolation_level {
            self.vars
                .set("transaction_isolation", VarInput::Flat(IsolationLevel::from(isolation_level).as_str()), true)
                .expect("transaction_isolation should be a valid var and isolation level is a valid value");
        }

        (self, Ok(()))
    }

    /// Starts either a single statement or implicit transaction based on the
    /// number of statements, but only if no transaction has been started already.
    pub fn start_transaction_implicit(mut self, wall_time: DateTime<Utc>, stmts: usize) -> Self {
        if let TransactionStatus::Default = self.transaction {
            let id = self.next_transaction_id;
            self.next_transaction_id = self.next_transaction_id.wrapping_add(1);
            let txn = Transaction {
                pcx: PlanContext::new(wall_time),
                ops: TransactionOps::None,
                write_lock_guard: None,
                access: None,
                id,
            };
            match stmts {
                1 => self.transaction = TransactionStatus::Started(txn),
                n if n > 1 => self.transaction = TransactionStatus::InTransactionImplicit(txn),
                _ => {}
            }
        }
        self
    }

    /// Clears a transaction, setting its state to Default and destroying all
    /// portals. Returned are:
    /// - sinks that were started in this transaction and need to be dropped
    /// - the cleared transaction so its operations can be handled
    ///
    /// The [Postgres protocol docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY) specify:
    /// > a named portal object lasts till the end of the current transaction
    /// and
    /// > An unnamed portal is destroyed at the end of the transaction
    #[must_use]
    pub fn clear_transaction(&mut self) -> TransactionStatus<T> {
        self.portals.clear();
        self.pcx = None;
        mem::take(&mut self.transaction)
    }

    /// Marks the current transaction as failed.
    pub fn fail_transaction(mut self) -> Self {
        match self.transaction {
            TransactionStatus::Default => unreachable!(),
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransactionImplicit(txn)
            | TransactionStatus::InTransaction(txn) => {
                self.transaction = TransactionStatus::Failed(txn);
            }
            TransactionStatus::Failed(_) => {}
        };
        self
    }

    /// Returns the current transaction status.
    pub fn transaction(&self) -> &TransactionStatus<T> {
        &self.transaction
    }

    /// Returns the session's transaction code.
    pub fn transaction_code(&self) -> TransactionCode {
        self.transaction().into()
    }

    /// Adds operations to the current transaction. An error is produced if
    /// they cannot be merged (i.e., a timestamp-dependent read cannot be
    /// merged to an insert).
    pub fn add_transaction_ops(&mut self, add_ops: TransactionOps<T>) -> Result<(), AdapterError> {
        match &mut self.transaction {
            TransactionStatus::Started(Transaction { ops, access, .. })
            | TransactionStatus::InTransaction(Transaction { ops, access, .. })
            | TransactionStatus::InTransactionImplicit(Transaction { ops, access, .. }) => {
                match ops {
                    TransactionOps::None => {
                        if matches!(access, Some(TransactionAccessMode::ReadOnly))
                            && matches!(add_ops, TransactionOps::Writes(_))
                        {
                            return Err(AdapterError::ReadOnlyTransaction);
                        }
                        *ops = add_ops;
                    }
                    TransactionOps::Peeks(txn_timestamp_context) => match add_ops {
                        TransactionOps::Peeks(add_timestamp_context) => {
                            match (&txn_timestamp_context, add_timestamp_context) {
                                (
                                    TimestampContext::TimelineTimestamp(txn_timeline, txn_ts),
                                    TimestampContext::TimelineTimestamp(add_timeline, add_ts),
                                ) => {
                                    assert_eq!(*txn_timeline, add_timeline);
                                    assert_eq!(*txn_ts, add_ts);
                                }
                                (TimestampContext::NoTimestamp, add_timestamp_context) => {
                                    *txn_timestamp_context = add_timestamp_context
                                }
                                (_, TimestampContext::NoTimestamp) => {}
                            };
                        }
                        // Iff peeks thus far do not have a timestamp (i.e.
                        // they are constant), we can switch to a write
                        // transaction.
                        writes @ TransactionOps::Writes(..)
                            if !txn_timestamp_context.contains_timestamp() =>
                        {
                            *ops = writes;
                        }
                        _ => return Err(AdapterError::ReadOnlyTransaction),
                    },
                    TransactionOps::Subscribe => {
                        return Err(AdapterError::SubscribeOnlyTransaction)
                    }
                    TransactionOps::Writes(txn_writes) => match add_ops {
                        TransactionOps::Writes(mut add_writes) => {
                            // We should have already checked the access above, but make sure we don't miss
                            // it anyway.
                            assert!(!matches!(access, Some(TransactionAccessMode::ReadOnly)));
                            txn_writes.append(&mut add_writes);

                            if txn_writes
                                .iter()
                                .map(|op| op.id)
                                .collect::<BTreeSet<_>>()
                                .len()
                                > 1
                            {
                                return Err(AdapterError::MultiTableWriteTransaction);
                            }
                        }
                        // Iff peeks do not have a timestamp (i.e. they are
                        // constant), we can permit them.
                        TransactionOps::Peeks(timestamp_context)
                            if !timestamp_context.contains_timestamp() => {}
                        _ => {
                            return Err(AdapterError::WriteOnlyTransaction);
                        }
                    },
                }
            }
            TransactionStatus::Default | TransactionStatus::Failed(_) => {
                unreachable!()
            }
        }
        Ok(())
    }

    /// Returns a channel on which to send notices to the session.
    pub fn retain_notice_transmitter(&self) -> UnboundedSender<AdapterNotice> {
        self.notices_tx.clone()
    }

    /// Adds a notice to the session.
    pub fn add_notice(&mut self, notice: AdapterNotice) {
        let _ = self.notices_tx.send(notice);
    }

    /// Awaits a possible notice.
    ///
    /// This method is cancel safe.
    pub async fn recv_notice(&mut self) -> AdapterNotice {
        // This method is cancel safe because recv is cancel safe.
        loop {
            let notice = self
                .notices_rx
                .recv()
                .await
                .expect("Session also holds a sender, so recv won't ever return None");
            match self.notice_filter(notice) {
                Some(notice) => return notice,
                None => continue,
            }
        }
    }

    /// Returns a draining iterator over the notices attached to the session.
    pub fn drain_notices(&mut self) -> Vec<AdapterNotice> {
        let mut notices = Vec::new();
        while let Ok(notice) = self.notices_rx.try_recv() {
            if let Some(notice) = self.notice_filter(notice) {
                notices.push(notice);
            }
        }
        notices
    }

    /// Returns Some if the notice should be reported, otherwise None.
    fn notice_filter(&mut self, notice: AdapterNotice) -> Option<AdapterNotice> {
        // Filter out notices for other clusters.
        if let AdapterNotice::ClusterReplicaStatusChanged { cluster, .. } = &notice {
            if cluster != self.vars.cluster() {
                return None;
            }
        }
        Some(notice)
    }

    /// Sets the transaction ops to `TransactionOps::None`. Must only be used after
    /// verifying that no transaction anomalies will occur if cleared.
    pub fn clear_transaction_ops(&mut self) {
        if let Some(txn) = self.transaction.inner_mut() {
            txn.ops = TransactionOps::None;
        }
    }

    /// If the current transaction ops belong to a read, then sets the
    /// ops to `None`, returning the old read timestamp context if
    /// any existed. Must only be used after verifying that no transaction
    /// anomalies will occur if cleared.
    pub fn take_transaction_timestamp_context(&mut self) -> Option<TimestampContext<T>> {
        if let Some(Transaction { ops, .. }) = self.transaction.inner_mut() {
            if let TransactionOps::Peeks(_) = ops {
                let ops = std::mem::take(ops);
                Some(ops.timestamp_context().expect("checked above"))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Returns the transaction's read timestamp context, if set.
    ///
    /// Returns `None` if there is no active transaction, or if the active
    /// transaction is not a read transaction.
    pub fn get_transaction_timestamp_context(&self) -> Option<TimestampContext<T>> {
        match self.transaction.inner() {
            Some(Transaction {
                pcx: _,
                ops: TransactionOps::Peeks(timestamp_context),
                write_lock_guard: _,
                access: _,
                id: _,
            }) => Some(timestamp_context.clone()),
            _ => None,
        }
    }

    /// Whether this session has a timestamp for a read transaction.
    pub fn contains_read_timestamp(&self) -> bool {
        matches!(
            self.transaction.inner(),
            Some(Transaction {
                pcx: _,
                ops: TransactionOps::Peeks(TimestampContext::TimelineTimestamp(_, _)),
                write_lock_guard: _,
                access: _,
                id: _,
            })
        )
    }

    /// Registers the prepared statement under `name`.
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// Removes the prepared statement associated with `name`.
    ///
    /// Returns whether a statement previously existed.
    pub fn remove_prepared_statement(&mut self, name: &str) -> bool {
        self.prepared_statements.remove(name).is_some()
    }

    /// Removes all prepared statements.
    pub fn remove_all_prepared_statements(&mut self) {
        self.prepared_statements.clear();
    }

    /// Retrieves the prepared statement associated with `name`.
    ///
    /// This is unverified and could be incorrect if the underlying catalog has
    /// changed.
    pub fn get_prepared_statement_unverified(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
    }

    /// Retrieves the prepared statement associated with `name`.
    ///
    /// This is unverified and could be incorrect if the underlying catalog has
    /// changed.
    pub fn get_prepared_statement_mut_unverified(
        &mut self,
        name: &str,
    ) -> Option<&mut PreparedStatement> {
        self.prepared_statements.get_mut(name)
    }

    /// Returns the prepared statements for the session.
    pub fn prepared_statements(&self) -> &BTreeMap<String, PreparedStatement> {
        &self.prepared_statements
    }

    /// Binds the specified portal to the specified prepared statement.
    ///
    /// If the prepared statement contains parameters, the values and types of
    /// those parameters must be provided in `params`. It is the caller's
    /// responsibility to ensure that the correct number of parameters is
    /// provided.
    ///
    /// The `results_formats` parameter sets the desired format of the results,
    /// and is stored on the portal.
    pub fn set_portal(
        &mut self,
        portal_name: String,
        desc: StatementDesc,
        stmt: Option<Statement<Raw>>,
        params: Vec<(Datum, ScalarType)>,
        result_formats: Vec<mz_pgrepr::Format>,
        catalog_revision: u64,
    ) -> Result<(), AdapterError> {
        // The empty portal can be silently replaced.
        if !portal_name.is_empty() && self.portals.contains_key(&portal_name) {
            return Err(AdapterError::DuplicateCursor(portal_name));
        }
        self.portals.insert(
            portal_name,
            Portal {
                stmt,
                desc,
                catalog_revision,
                parameters: Params {
                    datums: Row::pack(params.iter().map(|(d, _t)| d)),
                    types: params.into_iter().map(|(_d, t)| t).collect(),
                },
                result_formats: result_formats.into_iter().map(Into::into).collect(),
                state: PortalState::NotStarted,
            },
        );
        Ok(())
    }

    /// Removes the specified portal.
    ///
    /// If there is no such portal, this method does nothing. Returns whether that portal existed.
    pub fn remove_portal(&mut self, portal_name: &str) -> bool {
        self.portals.remove(portal_name).is_some()
    }

    /// Retrieves a reference to the specified portal.
    ///
    /// If there is no such portal, returns `None`.
    pub fn get_portal_unverified(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }

    /// Retrieves a mutable reference to the specified portal.
    ///
    /// If there is no such portal, returns `None`.
    pub fn get_portal_unverified_mut(&mut self, portal_name: &str) -> Option<&mut Portal> {
        self.portals.get_mut(portal_name)
    }

    /// Creates and installs a new portal.
    pub fn create_new_portal(
        &mut self,
        stmt: Option<Statement<Raw>>,
        desc: StatementDesc,
        parameters: Params,
        result_formats: Vec<Format>,
        catalog_revision: u64,
    ) -> Result<String, AdapterError> {
        // See: https://github.com/postgres/postgres/blob/84f5c2908dad81e8622b0406beea580e40bb03ac/src/backend/utils/mmgr/portalmem.c#L234

        for i in 0usize.. {
            let name = format!("<unnamed portal {}>", i);
            match self.portals.entry(name.clone()) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(entry) => {
                    entry.insert(Portal {
                        stmt,
                        desc,
                        catalog_revision,
                        parameters,
                        result_formats,
                        state: PortalState::NotStarted,
                    });
                    return Ok(name);
                }
            }
        }

        coord_bail!("unable to create a new portal");
    }

    /// Resets the session to its initial state. Returns sinks that need to be
    /// dropped.
    pub fn reset(&mut self) {
        let _ = self.clear_transaction();
        self.prepared_statements.clear();
        self.vars = SessionVars::new(self.vars.build_info(), self.vars.user().clone());
    }

    /// Returns the user who owns this session.
    pub fn user(&self) -> &User {
        self.vars.user()
    }

    /// Returns the [application_name] that created this session.
    /// 
    /// [application_name]: (https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME)
    pub fn application_name(&self) -> &str {
        self.vars.application_name()
    }

    /// Returns a reference to the variables in this session.
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Returns a mutable reference to the variables in this session.
    pub fn vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    /// Grants the coordinator's write lock guard to this session's inner
    /// transaction.
    ///
    /// # Panics
    /// If the inner transaction is idle. See
    /// [`TransactionStatus::grant_write_lock`].
    pub fn grant_write_lock(&mut self, guard: OwnedMutexGuard<()>) {
        self.transaction.grant_write_lock(guard);
    }

    /// Returns whether or not this session currently holds the write lock.
    pub fn has_write_lock(&self) -> bool {
        match self.transaction.inner() {
            None => false,
            Some(txn) => txn.write_lock_guard.is_some(),
        }
    }

    /// Returns whether the current session is a superuser.
    pub fn is_superuser(&self) -> bool {
        self.vars.is_superuser()
    }

    /// Returns a channel on which to send external metadata to the session.
    pub fn retain_external_metadata_transmitter(
        &mut self,
    ) -> UnboundedSender<ExternalUserMetadata> {
        self.external_metadata_tx.clone()
    }

    /// Drains any external metadata updates and applies the changes from the latest update.
    pub fn apply_external_metadata_updates(&mut self) {
        while let Ok(metadata) = self.external_metadata_rx.try_recv() {
            self.vars.set_external_user_metadata(metadata);
        }
    }

    /// Initializes the session's role ID.
    pub fn set_role_id(&mut self, role_id: RoleId) {
        self.role_id = Some(role_id);
    }

    /// Returns the session's role ID.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    pub fn role_id(&self) -> &RoleId {
        self.role_id.as_ref().expect("role_id invariant violated")
    }
}

/// A prepared statement.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    sql: Option<Statement<Raw>>,
    desc: StatementDesc,
    /// The most recent catalog revision that has verified this statement.
    pub catalog_revision: u64,
}

impl PreparedStatement {
    /// Constructs a new prepared statement.
    pub fn new(
        sql: Option<Statement<Raw>>,
        desc: StatementDesc,
        catalog_revision: u64,
    ) -> PreparedStatement {
        PreparedStatement {
            sql,
            desc,
            catalog_revision,
        }
    }

    /// Returns the raw SQL string associated with this prepared statement,
    /// if the prepared statement was not the empty query.
    pub fn sql(&self) -> Option<&Statement<Raw>> {
        self.sql.as_ref()
    }

    /// Returns the description of the prepared statement.
    pub fn desc(&self) -> &StatementDesc {
        &self.desc
    }
}

/// A portal represents the execution state of a running or runnable query.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Portal {
    /// The statement that is bound to this portal.
    pub stmt: Option<Statement<Raw>>,
    /// The statement description.
    pub desc: StatementDesc,
    /// The most recent catalog revision that has verified this statement.
    pub catalog_revision: u64,
    /// The bound values for the parameters in the prepared statement, if any.
    pub parameters: Params,
    /// The desired output format for each column in the result set.
    pub result_formats: Vec<mz_pgrepr::Format>,
    /// The execution state of the portal.
    #[derivative(Debug = "ignore")]
    pub state: PortalState,
}

/// Execution states of a portal.
pub enum PortalState {
    /// Portal not yet started.
    NotStarted,
    /// Portal is a rows-returning statement in progress with 0 or more rows
    /// remaining.
    InProgress(Option<InProgressRows>),
    /// Portal has completed and should not be re-executed. If the optional string
    /// is present, it is returned as a CommandComplete tag, otherwise an error
    /// is sent.
    Completed(Option<String>),
}

/// State of an in-progress, rows-returning portal.
pub struct InProgressRows {
    /// The current batch of rows.
    pub current: Option<Vec<Row>>,
    /// A stream from which to fetch more row batches.
    pub remaining: RowBatchStream,
}

impl InProgressRows {
    /// Creates a new InProgressRows from a batch stream.
    pub fn new(remaining: RowBatchStream) -> Self {
        Self {
            current: None,
            remaining,
        }
    }
}

/// A channel of batched rows.
pub type RowBatchStream = UnboundedReceiver<PeekResponseUnary>;

/// The transaction status of a session.
///
/// PostgreSQL's transaction states are in backend/access/transam/xact.c.
#[derive(Debug)]
pub enum TransactionStatus<T> {
    /// Idle. Matches `TBLOCK_DEFAULT`.
    Default,
    /// Running a single-query transaction. Matches
    /// `TBLOCK_STARTED`. In PostgreSQL, when using the extended query protocol, this
    /// may be upgraded into multi-statement implicit query (see [`Self::InTransactionImplicit`]).
    /// Additionally, some statements may trigger an eager commit of the implicit transaction,
    /// see: <https://git.postgresql.org/gitweb/?p=postgresql.git&a=commitdiff&h=f92944137>. In
    /// Materialize however, we eagerly commit all statements outside of an explicit transaction
    /// when using the extended query protocol. Therefore, we can guarantee that this state will
    /// always be a single-query transaction and never be upgraded into a multi-statement implicit
    /// query.
    Started(Transaction<T>),
    /// Currently in a transaction issued from a `BEGIN`. Matches `TBLOCK_INPROGRESS`.
    InTransaction(Transaction<T>),
    /// Currently in an implicit transaction started from a multi-statement query
    /// with more than 1 statements. Matches `TBLOCK_IMPLICIT_INPROGRESS`.
    InTransactionImplicit(Transaction<T>),
    /// In a failed transaction. Matches `TBLOCK_ABORT`.
    Failed(Transaction<T>),
}

impl<T> TransactionStatus<T> {
    /// Extracts the inner transaction ops and write lock guard if not failed.
    pub fn into_ops_and_lock_guard(
        self,
    ) -> (Option<TransactionOps<T>>, Option<OwnedMutexGuard<()>>) {
        match self {
            TransactionStatus::Default | TransactionStatus::Failed(_) => (None, None),
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransaction(txn)
            | TransactionStatus::InTransactionImplicit(txn) => {
                (Some(txn.ops), txn.write_lock_guard)
            }
        }
    }

    /// Exposes the inner transaction.
    pub fn inner(&self) -> Option<&Transaction<T>> {
        match self {
            TransactionStatus::Default => None,
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransaction(txn)
            | TransactionStatus::InTransactionImplicit(txn)
            | TransactionStatus::Failed(txn) => Some(txn),
        }
    }

    /// Exposes the inner transaction.
    pub fn inner_mut(&mut self) -> Option<&mut Transaction<T>> {
        match self {
            TransactionStatus::Default => None,
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransaction(txn)
            | TransactionStatus::InTransactionImplicit(txn)
            | TransactionStatus::Failed(txn) => Some(txn),
        }
    }

    /// Expresses whether or not the transaction was implicitly started.
    /// However, its negation does not imply explicitly started.
    pub fn is_implicit(&self) -> bool {
        match self {
            TransactionStatus::Started(_) | TransactionStatus::InTransactionImplicit(_) => true,
            TransactionStatus::Default
            | TransactionStatus::InTransaction(_)
            | TransactionStatus::Failed(_) => false,
        }
    }

    /// Whether the transaction may contain multiple statements.
    pub fn is_in_multi_statement_transaction(&self) -> bool {
        match self {
            TransactionStatus::InTransaction(_) | TransactionStatus::InTransactionImplicit(_) => {
                true
            }
            TransactionStatus::Default
            | TransactionStatus::Started(_)
            | TransactionStatus::Failed(_) => false,
        }
    }

    /// Grants the write lock to the inner transaction.
    ///
    /// # Panics
    /// If `self` is `TransactionStatus::Default`, which indicates that the
    /// transaction is idle, which is not appropriate to assign the
    /// coordinator's write lock to.
    pub fn grant_write_lock(&mut self, guard: OwnedMutexGuard<()>) {
        match self {
            TransactionStatus::Default => panic!("cannot grant write lock to txn not yet started"),
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransaction(txn)
            | TransactionStatus::InTransactionImplicit(txn)
            | TransactionStatus::Failed(txn) => txn.grant_write_lock(guard),
        }
    }

    /// The timeline of the transaction, if one exists.
    pub fn timeline(&self) -> Option<Timeline> {
        match self {
            TransactionStatus::Default => None,
            TransactionStatus::Started(txn)
            | TransactionStatus::InTransaction(txn)
            | TransactionStatus::InTransactionImplicit(txn)
            | TransactionStatus::Failed(txn) => txn.timeline(),
        }
    }
}

/// An abstraction allowing us to identify different transactions.
pub type TransactionId = u64;

impl<T> Default for TransactionStatus<T> {
    fn default() -> Self {
        TransactionStatus::Default
    }
}

/// State data for transactions.
#[derive(Debug)]
pub struct Transaction<T> {
    /// Plan context.
    pub pcx: PlanContext,
    /// Transaction operations.
    pub ops: TransactionOps<T>,
    /// Uniquely identifies the transaction on a per connection basis.
    /// Two transactions started from separate connections may share the
    /// same ID.
    /// If all IDs have been exhausted, this will wrap around back to 0.
    pub id: TransactionId,
    /// Holds the coordinator's write lock.
    write_lock_guard: Option<OwnedMutexGuard<()>>,
    /// Access mode (read only, read write).
    access: Option<TransactionAccessMode>,
}

impl<T> Transaction<T> {
    /// Grants the write lock to this transaction for the remainder of its lifetime.
    fn grant_write_lock(&mut self, guard: OwnedMutexGuard<()>) {
        self.write_lock_guard = Some(guard);
    }

    /// The timeline of the transaction, if one exists.
    fn timeline(&self) -> Option<Timeline> {
        match &self.ops {
            TransactionOps::Peeks(TimestampContext::TimelineTimestamp(timeline, _)) => {
                Some(timeline.clone())
            }
            TransactionOps::Peeks(_)
            | TransactionOps::None
            | TransactionOps::Subscribe
            | TransactionOps::Writes(_) => None,
        }
    }
}

/// A transaction's status code.
#[derive(Debug, Clone, Copy)]
pub enum TransactionCode {
    /// Not currently in a transaction
    Idle,
    /// Currently in a transaction
    InTransaction,
    /// Currently in a transaction block which is failed
    Failed,
}

impl From<TransactionCode> for u8 {
    fn from(code: TransactionCode) -> Self {
        match code {
            TransactionCode::Idle => b'I',
            TransactionCode::InTransaction => b'T',
            TransactionCode::Failed => b'E',
        }
    }
}

impl From<TransactionCode> for String {
    fn from(code: TransactionCode) -> Self {
        char::from(u8::from(code)).to_string()
    }
}

impl<T> From<&TransactionStatus<T>> for TransactionCode {
    /// Convert from the Session's version
    fn from(status: &TransactionStatus<T>) -> TransactionCode {
        match status {
            TransactionStatus::Default => TransactionCode::Idle,
            TransactionStatus::Started(_) => TransactionCode::InTransaction,
            TransactionStatus::InTransaction(_) => TransactionCode::InTransaction,
            TransactionStatus::InTransactionImplicit(_) => TransactionCode::InTransaction,
            TransactionStatus::Failed(_) => TransactionCode::Failed,
        }
    }
}

/// The type of operation being performed by the transaction.
///
/// This is needed because we currently do not allow mixing reads and writes in
/// a transaction. Use this to record what we have done, and what may need to
/// happen at commit.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionOps<T> {
    /// The transaction has been initiated, but no statement has yet been executed
    /// in it.
    None,
    /// This transaction has had a peek (`SELECT`, `SUBSCRIBE`). If the inner value
    /// is has a timestamp, it must only do other peeks. However, if it doesn't
    /// have a timestamp (i.e. the values are constants), the transaction can still
    /// perform writes.
    Peeks(TimestampContext<T>),
    /// This transaction has done a `SUBSCRIBE` and must do nothing else.
    Subscribe,
    /// This transaction has had a write (`INSERT`, `UPDATE`, `DELETE`) and must
    /// only do other writes, or reads whose timestamp is None (i.e. constants).
    Writes(Vec<WriteOp>),
}

impl<T> TransactionOps<T> {
    fn timestamp_context(self) -> Option<TimestampContext<T>> {
        match self {
            TransactionOps::Peeks(timestamp_context) => Some(timestamp_context),
            TransactionOps::None | TransactionOps::Subscribe | TransactionOps::Writes(_) => None,
        }
    }
}

impl<T> Default for TransactionOps<T> {
    fn default() -> Self {
        Self::None
    }
}

/// An `INSERT` waiting to be committed.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteOp {
    /// The target table.
    pub id: GlobalId,
    /// The data rows.
    pub rows: Vec<(Row, Diff)>,
}
