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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem;

use chrono::{DateTime, Utc};
use derivative::Derivative;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::OwnedMutexGuard;

use mz_compute_client::client::ComputeInstanceId;
use mz_pgrepr::Format;
use mz_repr::{Datum, Diff, GlobalId, Row, ScalarType};
use mz_sql::ast::{Raw, Statement, TransactionAccessMode};
use mz_sql::plan::{Params, PlanContext, StatementDesc};

use crate::client::ConnectionId;
use crate::coord::{CoordTimestamp, PeekResponseUnary};
use crate::error::CoordError;

mod vars;

pub use self::vars::{
    ClientSeverity, Var, Vars, DEFAULT_DATABASE_NAME, SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION,
    SERVER_PATCH_VERSION,
};

const DUMMY_CONNECTION_ID: ConnectionId = 0;

/// A session holds per-connection state.
#[derive(Debug)]
pub struct Session<T = mz_repr::Timestamp> {
    conn_id: ConnectionId,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    transaction: TransactionStatus<T>,
    pcx: Option<PlanContext>,
    user: String,
    vars: Vars,
    drop_sinks: Vec<(ComputeInstanceId, GlobalId)>,
}

impl<T: CoordTimestamp> Session<T> {
    /// Creates a new session for the specified connection ID.
    pub fn new(conn_id: ConnectionId, user: String) -> Session<T> {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Self::new_internal(conn_id, user)
    }

    /// Creates a new dummy session.
    ///
    /// Dummy sessions are intended for use when executing queries on behalf of
    /// the system itself, rather than on behalf of a user.
    pub fn dummy() -> Session<T> {
        Self::new_internal(DUMMY_CONNECTION_ID, "mz_system".into())
    }

    fn new_internal(conn_id: ConnectionId, user: String) -> Session<T> {
        Session {
            conn_id,
            transaction: TransactionStatus::Default,
            pcx: None,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            user,
            vars: Vars::default(),
            drop_sinks: vec![],
        }
    }

    /// Returns the connection ID associated with the session.
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    /// Returns the current transaction's PlanContext. Panics if there is not a
    /// current transaction.
    pub fn pcx(&self) -> &PlanContext {
        &self.transaction().inner().unwrap().pcx
    }

    /// Starts an explicit transaction, or changes an implicit to an explicit
    /// transaction.
    pub fn start_transaction(
        mut self,
        wall_time: DateTime<Utc>,
        access: Option<TransactionAccessMode>,
    ) -> Self {
        match self.transaction {
            TransactionStatus::Default | TransactionStatus::Started(_) => {
                self.transaction = TransactionStatus::InTransaction(Transaction {
                    pcx: PlanContext::new(wall_time, self.vars.qgm_optimizations()),
                    ops: TransactionOps::None,
                    write_lock_guard: None,
                    access,
                });
            }
            TransactionStatus::InTransactionImplicit(txn) => {
                self.transaction = TransactionStatus::InTransaction(txn);
            }
            TransactionStatus::InTransaction(_) => {}
            TransactionStatus::Failed(_) => unreachable!(),
        };
        self
    }

    /// Starts either a single statement or implicit transaction based on the
    /// number of statements, but only if no transaction has been started already.
    pub fn start_transaction_implicit(mut self, wall_time: DateTime<Utc>, stmts: usize) -> Self {
        if let TransactionStatus::Default = self.transaction {
            let txn = Transaction {
                pcx: PlanContext::new(wall_time, self.vars.qgm_optimizations()),
                ops: TransactionOps::None,
                write_lock_guard: None,
                access: None,
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
    pub fn clear_transaction(
        &mut self,
    ) -> (Vec<(ComputeInstanceId, GlobalId)>, TransactionStatus<T>) {
        self.portals.clear();
        self.pcx = None;
        let drop_sinks = mem::take(&mut self.drop_sinks);
        let txn = mem::take(&mut self.transaction);
        (drop_sinks, txn)
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

    /// Adds operations to the current transaction. An error is produced if they
    /// cannot be merged (i.e., a read cannot be merged to an insert).
    pub fn add_transaction_ops(&mut self, add_ops: TransactionOps<T>) -> Result<(), CoordError> {
        match &mut self.transaction {
            TransactionStatus::Started(Transaction { ops, access, .. })
            | TransactionStatus::InTransaction(Transaction { ops, access, .. })
            | TransactionStatus::InTransactionImplicit(Transaction { ops, access, .. }) => {
                match ops {
                    TransactionOps::None => {
                        if matches!(access, Some(TransactionAccessMode::ReadOnly))
                            && matches!(add_ops, TransactionOps::Writes(_))
                        {
                            return Err(CoordError::ReadOnlyTransaction);
                        }
                        *ops = add_ops;
                    }
                    TransactionOps::Peeks(txn_ts) => match add_ops {
                        TransactionOps::Peeks(add_ts) => {
                            assert_eq!(*txn_ts, add_ts);
                        }
                        _ => return Err(CoordError::ReadOnlyTransaction),
                    },
                    TransactionOps::Tail => return Err(CoordError::TailOnlyTransaction),
                    TransactionOps::Writes(txn_writes) => match add_ops {
                        TransactionOps::Writes(mut add_writes) => {
                            // We should have already checked the access above, but make sure we don't miss
                            // it anyway.
                            assert!(!matches!(access, Some(TransactionAccessMode::ReadOnly)));
                            txn_writes.append(&mut add_writes);

                            if txn_writes
                                .iter()
                                .map(|op| op.id)
                                .collect::<HashSet<_>>()
                                .len()
                                > 1
                            {
                                return Err(CoordError::MultiTableWriteTransaction);
                            }
                        }
                        _ => {
                            return Err(CoordError::WriteOnlyTransaction);
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

    /// Adds a sink that will need to be dropped when the current transaction is
    /// cleared.
    pub fn add_drop_sink(&mut self, compute_instance: ComputeInstanceId, name: GlobalId) {
        self.drop_sinks.push((compute_instance, name));
    }

    /// Sets the transaction ops to `TransactionOps::None`. Must only be used after
    /// verifying that no transaction anomalies will occur if cleared.
    pub fn clear_transaction_ops(&mut self) {
        if let Some(txn) = self.transaction.inner_mut() {
            txn.ops = TransactionOps::None;
        }
    }

    /// Returns the transaction's read timestamp, if set.
    ///
    /// Returns `None` if there is no active transaction, or if the active
    /// transaction is not a read transaction.
    pub fn get_transaction_timestamp(&self) -> Option<T> {
        // If the transaction already has a peek timestamp, use it. Otherwise generate
        // one. We generate one even though we could check here that the transaction
        // isn't in some other conflicting state because we want all of that logic to
        // reside in add_transaction_ops.
        match self.transaction.inner() {
            Some(Transaction {
                pcx: _,
                ops: TransactionOps::Peeks(ts),
                write_lock_guard: _,
                access: _,
            }) => Some(ts.clone()),
            _ => None,
        }
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
    pub fn prepared_statements(&self) -> &HashMap<String, PreparedStatement> {
        &self.prepared_statements
    }

    /// Binds the specified portal to the specified prepared statement.
    ///
    /// If the prepared statement contains parameters, the values and types of
    /// those parameters must be provided in `params`. It is the caller's
    /// responsibility to ensure that the correct number of parameters is
    /// provided.
    ///
    // The `results_formats` parameter sets the desired format of the results,
    /// and is stored on the portal.
    pub fn set_portal(
        &mut self,
        portal_name: String,
        desc: StatementDesc,
        stmt: Option<Statement<Raw>>,
        params: Vec<(Datum, ScalarType)>,
        result_formats: Vec<mz_pgrepr::Format>,
        catalog_revision: u64,
    ) -> Result<(), CoordError> {
        // The empty portal can be silently replaced.
        if !portal_name.is_empty() && self.portals.contains_key(&portal_name) {
            return Err(CoordError::DuplicateCursor(portal_name));
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
    ) -> Result<String, CoordError> {
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
    pub fn reset(&mut self) -> Vec<(ComputeInstanceId, GlobalId)> {
        let (drop_sinks, _) = self.clear_transaction();
        self.prepared_statements.clear();
        self.vars = Vars::default();
        drop_sinks
    }

    /// Returns the name of the user who owns this session.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns a reference to the variables in this session.
    pub fn vars(&self) -> &Vars {
        &self.vars
    }

    /// Returns a mutable reference to the variables in this session.
    pub fn vars_mut(&mut self) -> &mut Vars {
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
    /// Running a single-query transaction. Matches `TBLOCK_STARTED`.
    Started(Transaction<T>),
    /// Currently in a transaction issued from a `BEGIN`. Matches `TBLOCK_INPROGRESS`.
    InTransaction(Transaction<T>),
    /// Currently in an implicit transaction started from a multi-statement query
    /// with more than 1 statements. Matches `TBLOCK_IMPLICIT_INPROGRESS`.
    InTransactionImplicit(Transaction<T>),
    /// In a failed transaction that was started explicitly (i.e., previously
    /// InTransaction). We do not use Failed for implicit transactions because
    /// those cleanup after themselves. Matches `TBLOCK_ABORT`.
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
}

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
    /// This transaction has had a peek (`SELECT`, `TAIL`) and must only do other peeks.
    Peeks(T),
    /// This transaction has done a TAIL and must do nothing else.
    Tail,
    /// This transaction has had a write (`INSERT`, `UPDATE`, `DELETE`) and must only do
    /// other writes.
    Writes(Vec<WriteOp>),
}

/// An `INSERT` waiting to be committed.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteOp {
    /// The target table.
    pub id: GlobalId,
    /// The data rows.
    pub rows: Vec<(Row, Diff)>,
}

/// The action to take during end_transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndTransactionAction {
    /// Commit the transaction.
    Commit,
    /// Rollback the transaction.
    Rollback,
}

impl EndTransactionAction {
    /// Returns the pgwire tag for this action.
    pub fn tag(&self) -> &'static str {
        match self {
            EndTransactionAction::Commit => "COMMIT",
            EndTransactionAction::Rollback => "ROLLBACK",
        }
    }
}
