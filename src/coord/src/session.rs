// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-connection configuration parameters and state.

#![forbid(missing_docs)]

use std::collections::HashMap;
use std::mem;

use derivative::Derivative;
use futures::Stream;

use expr::GlobalId;
use repr::{Datum, Row, ScalarType};
use sql::ast::{Raw, Statement};
use sql::plan::{Params, StatementDesc};

use crate::error::CoordError;

mod vars;

pub use self::vars::{Var, Vars};

const DUMMY_CONNECTION_ID: u32 = 0;

/// A session holds per-connection state.
#[derive(Debug)]
pub struct Session {
    conn_id: u32,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    transaction: TransactionStatus,
    user: String,
    vars: Vars,
    drop_sinks: Vec<GlobalId>,
}

impl Session {
    /// Creates a new session for the specified connection ID.
    pub fn new(conn_id: u32, user: String) -> Session {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Self::new_internal(conn_id, user)
    }

    /// Creates a new dummy session.
    ///
    /// Dummy sessions are intended for use when executing queries on behalf of
    /// the system itself, rather than on behalf of a user.
    pub fn dummy() -> Session {
        Self::new_internal(DUMMY_CONNECTION_ID, "mz_system".into())
    }

    fn new_internal(conn_id: u32, user: String) -> Session {
        Session {
            conn_id,
            transaction: TransactionStatus::Default,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            user,
            vars: Vars::default(),
            drop_sinks: vec![],
        }
    }

    /// Returns the connection ID associated with the session.
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }

    /// Starts a transaction.
    pub fn start_transaction(&mut self) {
        self.transaction = match &mut self.transaction {
            TransactionStatus::Default | TransactionStatus::Started(_) => {
                TransactionStatus::InTransaction(TransactionOps::None)
            }
            TransactionStatus::InTransaction(ops)
            | TransactionStatus::InTransactionImplicit(ops) => {
                TransactionStatus::InTransaction(mem::replace(ops, TransactionOps::None))
            }
            TransactionStatus::Failed => unreachable!(),
        };
    }

    /// Starts an implicit transaction.
    pub fn start_transaction_implicit(&mut self, stmts: usize) {
        if let TransactionStatus::Default = self.transaction {
            match stmts {
                1 => self.transaction = TransactionStatus::Started(TransactionOps::None),
                n if n > 1 => {
                    self.transaction =
                        TransactionStatus::InTransactionImplicit(TransactionOps::None)
                }
                _ => {}
            }
        }
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
    pub fn clear_transaction(&mut self) -> (Vec<GlobalId>, TransactionStatus) {
        self.portals.clear();
        let drop_sinks = mem::take(&mut self.drop_sinks);
        let txn = mem::take(&mut self.transaction);
        (drop_sinks, txn)
    }

    /// Marks the current transaction as failed.
    pub fn fail_transaction(&mut self) {
        self.transaction = TransactionStatus::Failed;
    }

    /// Returns the current transaction status.
    pub fn transaction(&self) -> &TransactionStatus {
        &self.transaction
    }

    /// Adds operations to the current transaction. An error is produced if they
    /// cannot be merged (i.e., a read cannot be merged to an insert).
    pub fn add_transaction_ops(&mut self, add_ops: TransactionOps) -> Result<(), CoordError> {
        match &mut self.transaction {
            TransactionStatus::Started(txn_ops)
            | TransactionStatus::InTransaction(txn_ops)
            | TransactionStatus::InTransactionImplicit(txn_ops) => match txn_ops {
                TransactionOps::None => *txn_ops = add_ops,
                TransactionOps::Reads => match add_ops {
                    TransactionOps::Reads => {}
                    _ => return Err(CoordError::ReadOnlyTransaction),
                },
                TransactionOps::Writes(txn_writes) => match add_ops {
                    TransactionOps::Writes(mut add_writes) => {
                        txn_writes.append(&mut add_writes);
                    }
                    _ => {
                        return Err(CoordError::WriteOnlyTransaction);
                    }
                },
            },
            TransactionStatus::Default | TransactionStatus::Failed => {
                unreachable!()
            }
        }
        Ok(())
    }

    /// Adds a sink that will need to be dropped when the current transaction is
    /// cleared.
    pub fn add_drop_sink(&mut self, name: GlobalId) {
        self.drop_sinks.push(name);
    }

    /// Registers the prepared statement under `name`.
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// Removes the prepared statement associated with `name`.
    ///
    /// If there is no such prepared statement, this method does nothing.
    pub fn remove_prepared_statement(&mut self, name: &str) {
        let _ = self.prepared_statements.remove(name);
    }

    /// Retrieves the prepared statement associated with `name`.
    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
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
        result_formats: Vec<pgrepr::Format>,
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
    pub fn get_portal(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }

    /// Retrieves a mutable reference to the specified portal.
    ///
    /// If there is no such portal, returns `None`.
    pub fn get_portal_mut(&mut self, portal_name: &str) -> Option<&mut Portal> {
        self.portals.get_mut(portal_name)
    }

    /// Resets the session to its initial state. Returns sinks that need to be
    /// dropped.
    pub fn reset(&mut self) -> Vec<GlobalId> {
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
}

/// A prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    sql: Option<Statement<Raw>>,
    desc: StatementDesc,
}

impl PreparedStatement {
    /// Constructs a new prepared statement.
    pub fn new(sql: Option<Statement<Raw>>, desc: StatementDesc) -> PreparedStatement {
        PreparedStatement { sql, desc }
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
    /// The bound values for the parameters in the prepared statement, if any.
    pub parameters: Params,
    /// The desired output format for each column in the result set.
    pub result_formats: Vec<pgrepr::Format>,
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
    InProgress(Option<RowBatchStream>),
    /// Portal has completed and should not be re-executed. If the optional string
    /// is present, it is returned as a CommandComplete tag, otherwise an error
    /// is sent.
    Completed(Option<String>),
}

/// A stream of batched rows.
pub type RowBatchStream = Box<dyn Stream<Item = Vec<Row>> + Send + Unpin>;

/// The transaction status of a session.
///
/// PostgreSQL's transaction states are in backend/access/transam/xact.c.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    /// Idle. Matches `TBLOCK_DEFAULT`.
    Default,
    /// Running a single-query transaction. Matches `TBLOCK_STARTED`.
    Started(TransactionOps),
    /// Currently in a transaction issued from a `BEGIN`. Matches `TBLOCK_INPROGRESS`.
    InTransaction(TransactionOps),
    /// Currently in an implicit transaction started from a multi-statement query
    /// with more than 1 statements. Matches `TBLOCK_IMPLICIT_INPROGRESS`.
    InTransactionImplicit(TransactionOps),
    /// In a failed transaction that was started explicitly (i.e., previously
    /// InTransaction). We do not use Failed for implicit transactions because
    /// those cleanup after themselves. Matches `TBLOCK_ABORT`.
    Failed,
}

impl Default for TransactionStatus {
    fn default() -> Self {
        TransactionStatus::Default
    }
}

/// The type of operation being performed by the transaction.
///
/// This is needed because we currently do not allow mixing reads and writes in
/// a transaction. Use this to record what we have done, and what may need to
/// happen at commit.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionOps {
    /// The transaction has been initiated, but no statement has yet been executed
    /// in it.
    None,
    /// This transaction has had a read (`SELECT`, `TAIL`) and must only do other reads.
    Reads,
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
    pub rows: Vec<(Row, isize)>,
}

/// The action to take during end_transaction.
#[derive(Debug)]
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
