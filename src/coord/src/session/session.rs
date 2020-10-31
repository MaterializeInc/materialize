// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! All the state that is associated with a specific session.
//!
//! The primary docs for this module are in the parent [`sql::session`](super)
//! module docs. This module exists to make the top level more clear in terms of
//! what is actually exported vs. required internally.
//!
//! Client connections each get a new [`Session`], which is composed of
//! [`Var`](var::Var)s and the elements of prepared statements.

use std::collections::HashMap;

use anyhow::bail;

use repr::{Datum, Row, ScalarType};
use sql::plan::Params;

use crate::session::statement::{Portal, PreparedStatement};
use crate::session::transaction::TransactionStatus;
use crate::session::vars::Vars;

const DUMMY_CONNECTION_ID: u32 = 0;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Debug)]
pub struct Session {
    conn_id: u32,
    /// The current state of the the session's transaction
    transaction: TransactionStatus,
    /// A map from statement names to SQL queries
    prepared_statements: HashMap<String, PreparedStatement>,
    /// Portals associated with the current session
    ///
    /// Portals are primarily a way to retrieve the results for a query with all
    /// parameters bound.
    portals: HashMap<String, Portal>,
    vars: Vars,
}

impl Session {
    /// Given a connection id, provides a new session with default values.
    pub fn new(conn_id: u32) -> Session {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Session {
            conn_id,
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            vars: Vars::default(),
        }
    }

    /// Returns a Session using a DUMMY_CONNECTION_ID.
    /// NOTE: Keep this in sync with ::new()
    pub fn dummy() -> Session {
        Session {
            conn_id: DUMMY_CONNECTION_ID,
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            vars: Vars::default(),
        }
    }

    /// Returns the connection id of a session
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }

    /// Put the session into a transaction
    ///
    /// This does not nest, it just keeps us in a transaction even if we were already in
    /// one.
    pub fn start_transaction(&mut self) {
        self.transaction = TransactionStatus::InTransaction;
    }

    /// Take the session out of a transaction
    ///
    /// This is fine to do even if we are not in a transaction
    pub fn end_transaction(&mut self) {
        self.transaction = TransactionStatus::Idle;
    }

    /// If the session is currenlty in a transaction, mark it failed
    ///
    /// Does nothing in other cases
    pub fn fail_transaction(&mut self) {
        if self.transaction == TransactionStatus::InTransaction {
            self.transaction = TransactionStatus::Failed;
        }
    }

    /// Get the current transaction status of the session
    pub fn transaction(&self) -> &TransactionStatus {
        &self.transaction
    }

    /// Ensure that the given prepared statement is present in this session
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// Removes the prepared statement associated with `name`. It is not an
    /// error if no such statement exists.
    pub fn remove_prepared_statement(&mut self, name: &str) {
        let _ = self.prepared_statements.remove(name);
    }

    /// Retrieve the prepared statement in this session associated with `name`
    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
    }

    /// Given a portal name, get the associated prepared statement
    pub fn get_prepared_statement_for_portal(
        &self,
        portal_name: &str,
    ) -> Option<&PreparedStatement> {
        self.portals
            .get(portal_name)
            .and_then(|portal| self.prepared_statements.get(&portal.statement_name))
    }

    /// Ensure that the given portal exists
    ///
    /// **Errors** if the statement name has not be set
    pub fn set_portal<'a>(
        &mut self,
        portal_name: String,
        statement_name: String,
        params: Vec<(Datum<'a>, ScalarType)>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<(), anyhow::Error> {
        if !self.prepared_statements.contains_key(&statement_name) {
            bail!(
                "statement does not exist for portal creation: \
                 statement={:?} portal={:?}",
                statement_name,
                portal_name
            );
        }

        self.portals.insert(
            portal_name,
            Portal {
                statement_name,
                parameters: Params {
                    datums: Row::pack(params.iter().map(|(d, _t)| d)),
                    types: params.into_iter().map(|(_d, t)| t).collect(),
                },
                result_formats: result_formats.into_iter().map(Into::into).collect(),
                remaining_rows: None,
            },
        );
        Ok(())
    }

    /// Remove the portal, doing nothing if the portal does not exist
    pub fn remove_portal(&mut self, portal_name: &str) {
        let _ = self.portals.remove(portal_name);
    }

    /// Retrieve a portal by name
    pub fn get_portal(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }

    /// Get a portal for mutation
    pub fn get_portal_mut(&mut self, portal_name: &str) -> Option<&mut Portal> {
        self.portals.get_mut(portal_name)
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
