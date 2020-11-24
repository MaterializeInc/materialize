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

use derivative::Derivative;
use futures::Stream;

use repr::{Datum, Row, ScalarType};
use sql::ast::Statement;
use sql::plan::{Params, StatementDesc};

mod vars;

pub use self::vars::Vars;

const DUMMY_CONNECTION_ID: u32 = 0;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Debug)]
pub struct Session {
    conn_id: u32,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    transaction: TransactionStatus,
    vars: Vars,
}

impl Session {
    /// Creates a new session for the specified connection ID.
    pub fn new(conn_id: u32) -> Session {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Self::new_internal(conn_id)
    }

    /// Creates a new dummy session.
    ///
    /// Dummy sessions are intended for use when executing queries on behalf of
    /// the system itself, rather than on behalf of a user.
    pub fn dummy() -> Session {
        Self::new_internal(DUMMY_CONNECTION_ID)
    }

    fn new_internal(conn_id: u32) -> Session {
        Session {
            conn_id,
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            vars: Vars::default(),
        }
    }

    /// Returns the connection ID associated with the session.
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }

    /// Starts a transaction.
    pub fn start_transaction(&mut self) {
        self.transaction = TransactionStatus::InTransaction;
    }

    /// Starts an implicit transaction.
    pub fn start_transaction_implicit(&mut self) {
        self.transaction = TransactionStatus::InTransactionImplicit;
    }

    /// Ends a transaction, setting its state to Idle and destroying all portals.
    ///
    /// The [Postgres protocol docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY) specify:
    /// > a named portal object lasts till the end of the current transaction
    /// and
    /// > An unnamed portal is destroyed at the end of the transaction
    pub fn end_transaction(&mut self) {
        self.transaction = TransactionStatus::Idle;
        self.portals.clear();
    }

    /// Marks the current transaction as failed.
    ///
    /// If the session is not in a transaction, this method does nothing.
    pub fn fail_transaction(&mut self) {
        match self.transaction {
            TransactionStatus::InTransaction => {
                self.transaction = TransactionStatus::Failed;
            }
            TransactionStatus::InTransactionImplicit => {
                self.transaction = TransactionStatus::Idle;
            }
            _ => {}
        }
    }

    /// Returns the current transaction status.
    pub fn transaction(&self) -> &TransactionStatus {
        &self.transaction
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
    ///
    /// Returns an error if `statement_name` does not specify a valid
    /// prepared statement.
    pub fn set_portal(
        &mut self,
        portal_name: String,
        desc: StatementDesc,
        stmt: Option<Statement>,
        params: Vec<(Datum, ScalarType)>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<(), anyhow::Error> {
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

    /// Resets the session to its initial state.
    pub fn reset(&mut self) {
        self.end_transaction();
        self.prepared_statements.clear();
        self.portals.clear();
        self.vars = Vars::default();
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
    sql: Option<Statement>,
    desc: StatementDesc,
}

impl PreparedStatement {
    /// Constructs a new prepared statement.
    pub fn new(sql: Option<Statement>, desc: StatementDesc) -> PreparedStatement {
        PreparedStatement { sql, desc }
    }

    /// Returns the raw SQL string associated with this prepared statement,
    /// if the prepared statement was not the empty query.
    pub fn sql(&self) -> Option<&Statement> {
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
    pub stmt: Option<Statement>,
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
    InProgress(Option<Box<RowBatchStream>>),
    /// Portal has completed and should not be re-executed. If the optional string
    /// is present, it is returned as a CommandComplete tag, otherwise an error
    /// is sent.
    Completed(Option<String>),
}

/// A stream of batched rows.
pub type RowBatchStream = Box<dyn Stream<Item = Result<Vec<Row>, comm::Error>> + Send + Unpin>;

/// The transaction status of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not currently in a transaction.
    Idle,
    /// Currently in a transaction.
    InTransaction,
    /// Currently in an implicit transaction.
    InTransactionImplicit,
    /// Currently in a failed transaction.
    Failed,
}
