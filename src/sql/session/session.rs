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
use std::fmt;

use failure::bail;

use catalog::names::DatabaseSpecifier;
use repr::{Datum, Row, ScalarType};

use crate::session::statement::{Portal, PreparedStatement};
use crate::session::transaction::TransactionStatus;
use crate::session::var::{ServerVar, SessionVar, Var};
use crate::Params;

const APPLICATION_NAME: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

const CLIENT_ENCODING: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

const DATABASE: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("database"),
    value: "materialize",
    description: "Sets the current database (CockroachDB).",
};

const DATE_STYLE: ServerVar<&str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

const EXTRA_FLOAT_DIGITS: ServerVar<&i32> = ServerVar {
    name: unicase::Ascii::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

const SEARCH_PATH: ServerVar<&[&str]> = ServerVar {
    name: unicase::Ascii::new("search_path"),
    value: &["mz_catalog", "pg_catalog", "public"],
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
};

const SERVER_VERSION: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    // Pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
    // be. Too new and some clients will emit a "server too new" warning. Too
    // old and some clients will fall back to legacy code paths. v9.5.0
    // empirically seems to be a good compromise.
    value: "9.5.0",
    description: "Shows the server version (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<&bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

/// A `Session` holds SQL state that is attached to a session.
pub struct Session {
    application_name: SessionVar<str>,
    client_encoding: ServerVar<&'static str>,
    database: SessionVar<str>,
    date_style: ServerVar<&'static str>,
    extra_float_digits: SessionVar<i32>,
    search_path: ServerVar<&'static [&'static str]>,
    server_version: ServerVar<&'static str>,
    sql_safe_updates: SessionVar<bool>,
    /// The current state of the the session's transaction
    transaction: TransactionStatus,
    /// A map from statement names to SQL queries
    prepared_statements: HashMap<String, PreparedStatement>,
    /// Portals associated with the current session
    ///
    /// Portals are primarily a way to retrieve the results for a query with all
    /// parameters bound.
    portals: HashMap<String, Portal>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("application_name", &self.application_name())
            .field("client_encoding", &self.client_encoding())
            .field("database", &self.database())
            .field("date_style", &self.date_style())
            .field("extra_float_digits", &self.extra_float_digits())
            .field("search_path", &self.search_path())
            .field("server_version", &self.server_version())
            .field("sql_safe_updates", &self.sql_safe_updates())
            .field("transaction", &self.transaction())
            .field("prepared_statements", &self.prepared_statements.keys())
            .field("portals", &self.portals.keys())
            .finish()
    }
}

impl Default for Session {
    /// Constructs a new `Session` with default values.
    fn default() -> Session {
        Session {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            database: SessionVar::new(&DATABASE),
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            search_path: SEARCH_PATH,
            server_version: SERVER_VERSION,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }
}

impl Session {
    /// Returns all configuration parameters and their current values for this
    /// session.
    pub fn vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.application_name,
            &self.client_encoding,
            &self.database,
            &self.date_style,
            &self.extra_float_digits,
            &self.search_path,
            &self.server_version,
            &self.sql_safe_updates,
        ]
    }

    /// Returns the configuration parameters (and their current values for this
    /// session) that are expected to be sent to the client when a new
    /// connection is established or when their value changes.
    pub fn notify_vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.application_name,
            &self.client_encoding,
            &self.date_style,
            &self.server_version,
        ]
    }

    /// Returns a [`Var`] representing the configuration parameter with the
    /// specified name.
    ///
    /// Configuration parameters are matched case insensitively. If no such
    /// configuration parameter exists, `get` returns an error.
    ///
    /// Note that if `name` is known at compile time, you should instead use the
    /// named accessor to access the variable with its true Rust type. For
    /// example, `self.get("sql_safe_updates").value()` returns the string
    /// `"true"` or `"false"`, while `self.sql_safe_updates()` returns a bool.
    pub fn get(&self, name: &str) -> Result<&dyn Var, failure::Error> {
        if name == APPLICATION_NAME.name {
            Ok(&self.application_name)
        } else if name == CLIENT_ENCODING.name {
            Ok(&self.client_encoding)
        } else if name == DATABASE.name {
            Ok(&self.database)
        } else if name == DATE_STYLE.name {
            Ok(&self.date_style)
        } else if name == EXTRA_FLOAT_DIGITS.name {
            Ok(&self.extra_float_digits)
        } else if name == SEARCH_PATH.name {
            Ok(&self.search_path)
        } else if name == SERVER_VERSION.name {
            Ok(&self.server_version)
        } else if name == SQL_SAFE_UPDATES.name {
            Ok(&self.sql_safe_updates)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`Session::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), failure::Error> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(value)
        } else if name == CLIENT_ENCODING.name {
            bail!("parameter {} is read only", CLIENT_ENCODING.name);
        } else if name == DATABASE.name {
            self.database.set(value)
        } else if name == DATE_STYLE.name {
            bail!("parameter {} is read only", DATE_STYLE.name);
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(value)
        } else if name == SEARCH_PATH.name {
            bail!("parameter {} is read only", SEARCH_PATH.name);
        } else if name == SERVER_VERSION.name {
            bail!("parameter {} is read only", SERVER_VERSION.name);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.application_name.value()
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &'static str {
        self.client_encoding.value
    }

    /// Returns the value of the `DateStyle` configuration parameter.
    pub fn date_style(&self) -> &'static str {
        self.date_style.value
    }

    /// Returns the value of the `database` configuration parameter.
    pub fn database(&self) -> DatabaseSpecifier {
        DatabaseSpecifier::Name(self.database.value().to_owned())
    }

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.extra_float_digits.value()
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &'static [&'static str] {
        self.search_path.value
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &'static str {
        self.server_version.value
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.sql_safe_updates.value()
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
    ) -> Result<(), failure::Error> {
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
}

/// A trait for a session that exposes only the parameters that should impact
/// the planning of a SQL query.
pub trait PlanSession: fmt::Debug {
    /// Returns the value of the `database` configuration parameter.
    fn database(&self) -> DatabaseSpecifier;

    /// Returns the value of the `search_path` configuration parameter.
    fn search_path(&self) -> &[&str];

    /// Constructs an owned version of this `PlanSession`.
    fn to_owned(&self) -> Box<dyn PlanSession + Send>;
}

impl PlanSession for Session {
    fn database(&self) -> DatabaseSpecifier {
        self.database()
    }

    fn search_path(&self) -> &[&str] {
        self.search_path()
    }

    fn to_owned(&self) -> Box<dyn PlanSession + Send> {
        Box::new(OwnedPlanSession {
            database: self.database(),
            search_path: self.search_path.value,
        })
    }
}

#[derive(Clone, Debug)]
pub struct OwnedPlanSession {
    database: DatabaseSpecifier,
    search_path: &'static [&'static str],
}

impl PlanSession for OwnedPlanSession {
    fn database(&self) -> DatabaseSpecifier {
        self.database.clone()
    }

    fn search_path(&self) -> &[&str] {
        self.search_path
    }

    fn to_owned(&self) -> Box<dyn PlanSession + Send> {
        Box::new(self.clone())
    }
}

/// A [`PlanSession`] that errors if any of its parameters are accessed.
/// Useful for planning internal SQL queries that should not depend on any
/// session state.
#[derive(Clone, Debug)]
pub struct InternalSession;

impl PlanSession for InternalSession {
    fn database(&self) -> DatabaseSpecifier {
        DatabaseSpecifier::Ambient
    }

    fn search_path(&self) -> &[&str] {
        &[]
    }

    fn to_owned(&self) -> Box<dyn PlanSession + Send> {
        Box::new(self.clone())
    }
}
