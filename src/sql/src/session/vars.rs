// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Run-time configuration parameters
//!
//! ## Overview
//! Materialize roughly follows the PostgreSQL configuration model, which works
//! as follows. There is a global set of named configuration parameters, like
//! `DateStyle` and `client_encoding`. These parameters can be set in several
//! places: in an on-disk configuration file (in Postgres, named
//! postgresql.conf), in command line arguments when the server is started, or
//! at runtime via the `ALTER SYSTEM` or `SET` statements. Parameters that are
//! set in a session take precedence over database defaults, which in turn take
//! precedence over command line arguments, which in turn take precedence over
//! settings in the on-disk configuration. Note that changing the value of
//! parameters obeys transaction semantics: if a transaction fails to commit,
//! any parameters that were changed in that transaction (i.e., via `SET`) will
//! be rolled back to their previous value.
//!
//! The Materialize configuration hierarchy at the moment is much simpler.
//! Global defaults are hardcoded into the binary, and a select few parameters
//! can be overridden per session. A select few parameters can be overridden on
//! disk.
//!
//! The set of variables that can be overridden per session and the set of
//! variables that can be overridden on disk are currently disjoint. The
//! infrastructure has been designed with an eye towards merging these two sets
//! and supporting additional layers to the hierarchy, however, should the need
//! arise.
//!
//! The configuration parameters that exist are driven by compatibility with
//! PostgreSQL drivers that expect them, not because they are particularly
//! important.
//!
//! ## Structure
//! Thw most meaningful exports from this module are:
//!
//! - [`SessionVars`] represent per-session parameters, which each user can
//!   access independently of one another, and are accessed via `SET`.
//!
//!   The fields of [`SessionVars`] are either;
//!     - `SessionVar`, which is preferable and simply requires full support of
//!       the `SessionVar` impl for its embedded value type.
//!     - `ServerVar` for types that do not currently support everything
//!       required by `SessionVar`, e.g. they are fixed-value parameters.
//!
//!   In the fullness of time, all fields in [`SessionVars`] should be
//!   `SessionVar`.
//!
//! - [`SystemVars`] represent system-wide configuration settings and are
//!   accessed via `ALTER SYSTEM SET`.
//!
//!   All elements of [`SystemVars`] are `SystemVar`.
//!
//! Some [`VarDefinition`] are also marked as a [`FeatureFlag`]; this is just a
//! wrapper to make working with a set of [`VarDefinition`] easier, primarily from
//! within SQL planning, where we might want to check if a feature is enabled
//! before planning it.

use std::borrow::Cow;
use std::clone::Clone;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use im::OrdMap;
use mz_build_info::BuildInfo;
use mz_dyncfg::{ConfigSet, ConfigType, ConfigUpdates as PersistConfigUpdates, ConfigVal};
use mz_ore::cast::CastFrom;
use mz_persist_client::cfg::{CRDB_CONNECT_TIMEOUT, CRDB_TCP_USER_TIMEOUT};
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::bytes::ByteSize;
use mz_repr::user::ExternalUserMetadata;
use mz_storage_types::controller::PersistTxnTablesImpl;
use mz_tracing::{CloneableEnvFilter, SerializableDirective};
use once_cell::sync::Lazy;
use serde::Serialize;
use uncased::UncasedStr;

use crate::ast::Ident;
use crate::session::user::User;

pub(crate) mod constraints;
pub(crate) mod definitions;
pub(crate) mod errors;
pub(crate) mod polyfill;
pub(crate) mod value;

pub use definitions::*;
pub use errors::*;
pub use value::*;

/// The action to take during end_transaction.
///
/// This enum lives here because of convenience: it's more of an adapter
/// concept but [`SessionVars::end_transaction`] takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndTransactionAction {
    /// Commit the transaction.
    Commit,
    /// Rollback the transaction.
    Rollback,
}

/// Represents the input to a variable.
///
/// Each variable has different rules for how it handles each style of input.
/// This type allows us to defer interpretation of the input until the
/// variable-specific interpretation can be applied.
#[derive(Debug, Clone, Copy)]
pub enum VarInput<'a> {
    /// The input has been flattened into a single string.
    Flat(&'a str),
    /// The input comes from a SQL `SET` statement and is jumbled across
    /// multiple components.
    SqlSet(&'a [String]),
}

impl<'a> VarInput<'a> {
    /// Converts the variable input to an owned vector of strings.
    fn to_vec(&self) -> Vec<String> {
        match self {
            VarInput::Flat(v) => vec![v.to_string()],
            VarInput::SqlSet(values) => values.into_iter().map(|v| v.to_string()).collect(),
        }
    }
}

/// An owned version of [`VarInput`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum OwnedVarInput {
    /// See [`VarInput::Flat`].
    Flat(String),
    /// See [`VarInput::SqlSet`].
    SqlSet(Vec<String>),
}

impl OwnedVarInput {
    /// Converts this owned variable input as a [`VarInput`].
    pub fn borrow(&self) -> VarInput {
        match self {
            OwnedVarInput::Flat(v) => VarInput::Flat(v),
            OwnedVarInput::SqlSet(v) => VarInput::SqlSet(v),
        }
    }
}

/// A `Var` represents a configuration parameter of an arbitrary type.
pub trait Var: Debug {
    /// Returns the name of the configuration parameter.
    fn name(&self) -> &'static str;

    /// Constructs a flattened string representation of the current value of the
    /// configuration parameter.
    ///
    /// The resulting string is guaranteed to be parsable if provided to
    /// `Value::parse` as a [`VarInput::Flat`].
    fn value(&self) -> String;

    /// Returns a short sentence describing the purpose of the configuration
    /// parameter.
    fn description(&self) -> &'static str;

    /// Returns the name of the type of this variable.
    fn type_name(&self) -> Cow<'static, str>;

    /// Indicates wither the [`Var`] is visible as a function of the `user` and `system_vars`.
    /// "Invisible" parameters return `VarErrors`.
    ///
    /// Variables marked as `internal` are only visible for the system user.
    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError>;

    /// Upcast `self` to a `dyn Var`, useful when working with multiple different implementors of
    /// [`Var`].
    fn as_var(&self) -> &dyn Var
    where
        Self: Sized,
    {
        self
    }
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
///
/// Note: even though all of the different `*_value` fields are `Box<dyn Value>` they are enforced
/// to be the same type because we use the `definition`s `parse(...)` method. This is guaranteed to
/// return the same type as the compiled in default.
#[derive(Debug)]
pub struct SessionVar {
    definition: VarDefinition,
    /// System or Role default value.
    default_value: Option<Box<dyn Value>>,
    /// Value `LOCAL` to a transaction, will be unset at the completion of the transaction.
    local_value: Option<Box<dyn Value>>,
    /// Value set during a transaction, will be set if the transaction is committed.
    staged_value: Option<Box<dyn Value>>,
    /// Value that overrides the default.
    session_value: Option<Box<dyn Value>>,
}

impl Clone for SessionVar {
    fn clone(&self) -> Self {
        SessionVar {
            definition: self.definition.clone(),
            default_value: self.default_value.as_ref().map(|v| v.box_clone()),
            local_value: self.local_value.as_ref().map(|v| v.box_clone()),
            staged_value: self.staged_value.as_ref().map(|v| v.box_clone()),
            session_value: self.session_value.as_ref().map(|v| v.box_clone()),
        }
    }
}

impl SessionVar {
    pub fn new(var: VarDefinition) -> Self {
        SessionVar {
            definition: var,
            default_value: None,
            local_value: None,
            staged_value: None,
            session_value: None,
        }
    }

    /// Parse the input and update the stored value to match.
    pub fn set(&mut self, input: VarInput, local: bool) -> Result<(), VarError> {
        let v = self.definition.parse(input)?;

        // Validate our parsed value.
        self.validate_constraints(v.as_ref())?;

        if local {
            self.local_value = Some(v);
        } else {
            self.local_value = None;
            self.staged_value = Some(v);
        }
        Ok(())
    }

    /// Sets the default value for the variable.
    pub fn set_default(&mut self, input: VarInput) -> Result<(), VarError> {
        let v = self.definition.parse(input)?;
        self.validate_constraints(v.as_ref())?;
        self.default_value = Some(v);
        Ok(())
    }

    /// Reset the stored value to the default.
    pub fn reset(&mut self, local: bool) {
        let value = self
            .default_value
            .as_ref()
            .map(|v| v.as_ref())
            .unwrap_or(self.definition.value.value());
        if local {
            self.local_value = Some(value.box_clone());
        } else {
            self.local_value = None;
            self.staged_value = Some(value.box_clone());
        }
    }

    /// Returns a possibly new SessionVar if this needs to mutate at transaction end.
    #[must_use]
    pub fn end_transaction(&self, action: EndTransactionAction) -> Option<Self> {
        if !self.is_mutating() {
            return None;
        }
        let mut next: Self = self.clone();
        next.local_value = None;
        match action {
            EndTransactionAction::Commit if next.staged_value.is_some() => {
                next.session_value = next.staged_value.take()
            }
            _ => next.staged_value = None,
        }
        Some(next)
    }

    /// Whether this Var needs to mutate at the end of a transaction.
    pub fn is_mutating(&self) -> bool {
        self.local_value.is_some() || self.staged_value.is_some()
    }

    pub fn value_dyn(&self) -> &dyn Value {
        self.local_value
            .as_deref()
            .or_else(|| self.staged_value.as_deref())
            .or_else(|| self.session_value.as_deref())
            .or_else(|| self.default_value.as_deref())
            .unwrap_or(self.definition.value.value())
    }

    /// Returns the [`Value`] that is currently stored as the `session_value`.
    ///
    /// Note: This should __only__ be used for inspection, if you want to determine the current
    /// value of this [`SessionVar`] you should use [`SessionVar::value`].
    pub fn inspect_session_value(&self) -> Option<&dyn Value> {
        self.session_value.as_deref()
    }

    fn validate_constraints(&self, val: &dyn Value) -> Result<(), VarError> {
        if let Some(constraint) = &self.definition.constraint {
            constraint.check_constraint(self, self.value_dyn(), val)
        } else {
            Ok(())
        }
    }
}

impl Var for SessionVar {
    fn name(&self) -> &'static str {
        self.definition.name.as_str()
    }

    fn value(&self) -> String {
        self.value_dyn().format()
    }

    fn description(&self) -> &'static str {
        self.definition.description
    }

    fn type_name(&self) -> Cow<'static, str> {
        self.definition.type_name()
    }

    fn visible(
        &self,
        user: &User,
        system_vars: Option<&super::vars::SystemVars>,
    ) -> Result<(), super::vars::VarError> {
        self.definition.visible(user, system_vars)
    }
}

/// Session variables.
///
/// See the [`crate::session::vars`] module documentation for more details on the
/// Materialize configuration model.
#[derive(Debug, Clone)]
pub struct SessionVars {
    /// The set of all session variables.
    vars: OrdMap<&'static UncasedStr, SessionVar>,
    /// Inputs to computed variables.
    build_info: &'static BuildInfo,
    /// Information about the user associated with this Session.
    user: User,
}

impl SessionVars {
    /// Creates a new [`SessionVars`] without considering the System or Role defaults.
    pub fn new_unchecked(build_info: &'static BuildInfo, user: User) -> SessionVars {
        use definitions::*;

        let vars = [
            &FAILPOINTS,
            &SERVER_VERSION,
            &SERVER_VERSION_NUM,
            &SQL_SAFE_UPDATES,
            &REAL_TIME_RECENCY,
            &EMIT_TIMESTAMP_NOTICE,
            &EMIT_TRACE_ID_NOTICE,
            &AUTO_ROUTE_INTROSPECTION_QUERIES,
            &ENABLE_SESSION_RBAC_CHECKS,
            &ENABLE_SESSION_CARDINALITY_ESTIMATES,
            &MAX_QUERY_RESULT_SIZE,
            &MAX_IDENTIFIER_LENGTH,
            &STATEMENT_LOGGING_SAMPLE_RATE,
            &EMIT_INTROSPECTION_QUERY_NOTICE,
            &UNSAFE_NEW_TRANSACTION_WALL_TIME,
            &WELCOME_MESSAGE,
        ]
        .into_iter()
        .chain(SystemVars::SESSION_VARS.iter().map(|(_name, var)| *var))
        .map(|var| (var.name, SessionVar::new(var.clone())))
        .collect();

        SessionVars {
            vars,
            build_info,
            user,
        }
    }

    fn expect_value<V: Value>(&self, var: &VarDefinition) -> &V {
        let var = self
            .vars
            .get(var.name)
            .expect("provided var should be in state");
        let val = var.value_dyn();
        val.as_any().downcast_ref::<V>().expect("success")
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values for this session.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        #[allow(clippy::as_conversions)]
        self.vars
            .values()
            .map(|v| v.as_var())
            .chain([self.build_info as &dyn Var, &self.user])
    }

    /// Returns an iterator over configuration parameters (and their current
    /// values for this session) that are expected to be sent to the client when
    /// a new connection is established or when their value changes.
    pub fn notify_set(&self) -> impl Iterator<Item = &dyn Var> {
        [
            &APPLICATION_NAME,
            &CLIENT_ENCODING,
            &DATE_STYLE,
            &INTEGER_DATETIMES,
            &SERVER_VERSION,
            &STANDARD_CONFORMING_STRINGS,
            &TIMEZONE,
            &INTERVAL_STYLE,
            // Including `cluster`, `cluster_replica`, `database`, and `search_path` in the notify
            // set is a Materialize extension. Doing so allows users to more easily identify where
            // their queries will be executing, which is important to know when you consider the
            // size of a cluster, what indexes are present, etc.
            &CLUSTER,
            &CLUSTER_REPLICA,
            &DATABASE,
            &SEARCH_PATH,
        ]
        .into_iter()
        .map(|p| self.get(None, p.name()).expect("SystemVars known to exist"))
        // Including `mz_version` in the notify set is a Materialize
        // extension. Doing so allows applications to detect whether they
        // are talking to Materialize or PostgreSQL without an additional
        // network roundtrip. This is known to be safe because CockroachDB
        // has an analogous extension [0].
        // [0]: https://github.com/cockroachdb/cockroach/blob/369c4057a/pkg/sql/pgwire/conn.go#L1840
        .chain(std::iter::once(self.build_info.as_var()))
    }

    /// Resets all variables to their default value.
    pub fn reset_all(&mut self) {
        let names: Vec<_> = self.vars.keys().copied().collect();
        for name in names {
            self.vars[name].reset(false);
        }
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
    pub fn get(&self, system_vars: Option<&SystemVars>, name: &str) -> Result<&dyn Var, VarError> {
        let name = UncasedStr::new(name);
        if name == MZ_VERSION_NAME {
            Ok(self.build_info)
        } else if name == IS_SUPERUSER_NAME {
            Ok(&self.user)
        } else {
            self.vars
                .get(name)
                .map(|v| {
                    v.visible(&self.user, system_vars)?;
                    Ok(v.as_var())
                })
                .transpose()?
                .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
        }
    }

    /// Returns a [`SessionVar`] for inspection.
    ///
    /// Note: If you're trying to determine the value of the variable with `name` you should
    /// instead use the named accessor, or [`SessionVars::get`].
    pub fn inspect(&self, name: &str) -> Option<&SessionVar> {
        self.vars.get(UncasedStr::new(name))
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// The new value may be either committed or rolled back by the next call to
    /// [`SessionVars::end_transaction`]. If `local` is true, the new value is always
    /// discarded by the next call to [`SessionVars::end_transaction`], even if the
    /// transaction is marked to commit.
    ///
    /// Like with [`SessionVars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(
        &mut self,
        system_vars: Option<&SystemVars>,
        name: &str,
        input: VarInput,
        local: bool,
    ) -> Result<(), VarError> {
        let name = UncasedStr::new(name);
        self.check_read_only(name)?;

        self.vars
            .get_mut(name)
            .map(|v| {
                v.visible(&self.user, system_vars)?;
                v.set(input, local)
            })
            .transpose()?
            .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
    }

    /// Sets the default value for the parameter named `name` to the value
    /// represented by `value`.
    pub fn set_default(&mut self, name: &str, input: VarInput) -> Result<(), VarError> {
        let name = UncasedStr::new(name);
        self.check_read_only(name)?;

        self.vars
            .get_mut(name)
            // Note: visibility is checked when persisting a role default.
            .map(|v| v.set_default(input))
            .transpose()?
            .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// The new value may be either committed or rolled back by the next call to
    /// [`SessionVars::end_transaction`]. If `local` is true, the new value is
    /// always discarded by the next call to [`SessionVars::end_transaction`],
    /// even if the transaction is marked to commit.
    ///
    /// Like with [`SessionVars::get`], configuration parameters are matched
    /// case insensitively. If the named configuration parameter does not exist,
    /// an error is returned.
    ///
    /// If the variable does not exist or the user does not have the visibility
    /// requires, this function returns an error.
    pub fn reset(
        &mut self,
        system_vars: Option<&SystemVars>,
        name: &str,
        local: bool,
    ) -> Result<(), VarError> {
        let name = UncasedStr::new(name);
        self.check_read_only(name)?;

        self.vars
            .get_mut(name)
            .map(|v| {
                v.visible(&self.user, system_vars)?;
                v.reset(local);
                Ok(())
            })
            .transpose()?
            .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
    }

    /// Returns an error if the variable corresponding to `name` is read only.
    fn check_read_only(&self, name: &UncasedStr) -> Result<(), VarError> {
        if name == MZ_VERSION_NAME {
            Err(VarError::ReadOnlyParameter(MZ_VERSION_NAME.as_str()))
        } else if name == IS_SUPERUSER_NAME {
            Err(VarError::ReadOnlyParameter(IS_SUPERUSER_NAME.as_str()))
        } else if name == MAX_IDENTIFIER_LENGTH.name {
            Err(VarError::ReadOnlyParameter(
                MAX_IDENTIFIER_LENGTH.name.as_str(),
            ))
        } else {
            Ok(())
        }
    }

    /// Commits or rolls back configuration parameter updates made via
    /// [`SessionVars::set`] since the last call to `end_transaction`.
    ///
    /// Returns any session parameters that changed because the transaction ended.
    #[mz_ore::instrument(level = "debug")]
    pub fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> BTreeMap<&'static str, String> {
        let mut changed = BTreeMap::new();
        let mut updates = Vec::new();
        for (name, var) in self.vars.iter() {
            if !var.is_mutating() {
                continue;
            }
            let before = var.value();
            let next = var.end_transaction(action).expect("must mutate");
            let after = next.value();
            updates.push((*name, next));

            // Report the new value of the parameter.
            if before != after {
                changed.insert(var.name(), after);
            }
        }
        self.vars.extend(updates);
        changed
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.expect_value::<String>(&APPLICATION_NAME).as_str()
    }

    /// Returns the build info.
    pub fn build_info(&self) -> &'static BuildInfo {
        self.build_info
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &ClientEncoding {
        self.expect_value(&CLIENT_ENCODING)
    }

    /// Returns the value of the `client_min_messages` configuration parameter.
    pub fn client_min_messages(&self) -> &ClientSeverity {
        self.expect_value(&CLIENT_MIN_MESSAGES)
    }

    /// Returns the value of the `cluster` configuration parameter.
    pub fn cluster(&self) -> &str {
        self.expect_value::<String>(&CLUSTER).as_str()
    }

    /// Returns the value of the `cluster_replica` configuration parameter.
    pub fn cluster_replica(&self) -> Option<&str> {
        self.expect_value::<Option<String>>(&CLUSTER_REPLICA)
            .as_deref()
    }

    /// Returns the value of the `DateStyle` configuration parameter.
    pub fn date_style(&self) -> &[&str] {
        &self.expect_value::<DateStyle>(&DATE_STYLE).0
    }

    /// Returns the value of the `database` configuration parameter.
    pub fn database(&self) -> &str {
        self.expect_value::<String>(&DATABASE).as_str()
    }

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.expect_value(&EXTRA_FLOAT_DIGITS)
    }

    /// Returns the value of the `integer_datetimes` configuration parameter.
    pub fn integer_datetimes(&self) -> bool {
        *self.expect_value(&INTEGER_DATETIMES)
    }

    /// Returns the value of the `intervalstyle` configuration parameter.
    pub fn intervalstyle(&self) -> &IntervalStyle {
        self.expect_value(&INTERVAL_STYLE)
    }

    /// Returns the value of the `mz_version` configuration parameter.
    pub fn mz_version(&self) -> String {
        self.build_info.value()
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &[Ident] {
        self.expect_value::<Vec<Ident>>(&SEARCH_PATH).as_slice()
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &str {
        self.expect_value::<String>(&SERVER_VERSION).as_str()
    }

    /// Returns the value of the `server_version_num` configuration parameter.
    pub fn server_version_num(&self) -> i32 {
        *self.expect_value(&SERVER_VERSION_NUM)
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.expect_value(&SQL_SAFE_UPDATES)
    }

    /// Returns the value of the `standard_conforming_strings` configuration
    /// parameter.
    pub fn standard_conforming_strings(&self) -> bool {
        *self.expect_value(&STANDARD_CONFORMING_STRINGS)
    }

    /// Returns the value of the `statement_timeout` configuration parameter.
    pub fn statement_timeout(&self) -> &Duration {
        self.expect_value(&STATEMENT_TIMEOUT)
    }

    /// Returns the value of the `idle_in_transaction_session_timeout` configuration parameter.
    pub fn idle_in_transaction_session_timeout(&self) -> &Duration {
        self.expect_value(&IDLE_IN_TRANSACTION_SESSION_TIMEOUT)
    }

    /// Returns the value of the `timezone` configuration parameter.
    pub fn timezone(&self) -> &TimeZone {
        self.expect_value(&TIMEZONE)
    }

    /// Returns the value of the `transaction_isolation` configuration
    /// parameter.
    pub fn transaction_isolation(&self) -> &IsolationLevel {
        self.expect_value(&TRANSACTION_ISOLATION)
    }

    /// Returns the value of `real_time_recency` configuration parameter.
    pub fn real_time_recency(&self) -> bool {
        *self.expect_value(&REAL_TIME_RECENCY)
    }

    /// Returns the value of `emit_timestamp_notice` configuration parameter.
    pub fn emit_timestamp_notice(&self) -> bool {
        *self.expect_value(&EMIT_TIMESTAMP_NOTICE)
    }

    /// Returns the value of `emit_trace_id_notice` configuration parameter.
    pub fn emit_trace_id_notice(&self) -> bool {
        *self.expect_value(&EMIT_TRACE_ID_NOTICE)
    }

    /// Returns the value of `auto_route_introspection_queries` configuration parameter.
    pub fn auto_route_introspection_queries(&self) -> bool {
        *self.expect_value(&AUTO_ROUTE_INTROSPECTION_QUERIES)
    }

    /// Returns the value of `enable_session_rbac_checks` configuration parameter.
    pub fn enable_session_rbac_checks(&self) -> bool {
        *self.expect_value(&ENABLE_SESSION_RBAC_CHECKS)
    }

    /// Returns the value of `enable_session_cardinality_estimates` configuration parameter.
    pub fn enable_session_cardinality_estimates(&self) -> bool {
        *self.expect_value(&ENABLE_SESSION_CARDINALITY_ESTIMATES)
    }

    /// Returns the value of `is_superuser` configuration parameter.
    pub fn is_superuser(&self) -> bool {
        self.user.is_superuser()
    }

    /// Returns the user associated with this `SessionVars` instance.
    pub fn user(&self) -> &User {
        &self.user
    }

    /// Returns the value of the `max_query_result_size` configuration parameter.
    pub fn max_query_result_size(&self) -> u64 {
        self.expect_value::<ByteSize>(&MAX_QUERY_RESULT_SIZE)
            .as_bytes()
    }

    /// Sets the external metadata associated with the user.
    pub fn set_external_user_metadata(&mut self, metadata: ExternalUserMetadata) {
        self.user.external_metadata = Some(metadata);
    }

    pub fn set_cluster(&mut self, cluster: String) {
        self.set(None, CLUSTER.name(), VarInput::Flat(&cluster), false)
            .expect("setting cluster from string succeeds");
    }

    pub fn get_statement_logging_sample_rate(&self) -> Numeric {
        *self.expect_value(&STATEMENT_LOGGING_SAMPLE_RATE)
    }

    /// Returns the value of the `emit_introspection_query_notice` configuration parameter.
    pub fn emit_introspection_query_notice(&self) -> bool {
        *self.expect_value(&EMIT_INTROSPECTION_QUERY_NOTICE)
    }

    pub fn unsafe_new_transaction_wall_time(&self) -> Option<CheckedTimestamp<DateTime<Utc>>> {
        *self.expect_value(&UNSAFE_NEW_TRANSACTION_WALL_TIME)
    }

    /// Returns the value of the `welcome_message` configuration parameter.
    pub fn welcome_message(&self) -> bool {
        *self.expect_value(&WELCOME_MESSAGE)
    }
}

/// A `SystemVar` is persisted on disk value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
pub struct SystemVar {
    definition: VarDefinition,
    /// Value currently persisted to disk.
    persisted_value: Option<Box<dyn Value>>,
    /// Current default, not persisted to disk.
    dynamic_default: Option<Box<dyn Value>>,
}

impl Clone for SystemVar {
    fn clone(&self) -> Self {
        SystemVar {
            definition: self.definition.clone(),
            persisted_value: self.persisted_value.as_ref().map(|v| v.box_clone()),
            dynamic_default: self.dynamic_default.as_ref().map(|v| v.box_clone()),
        }
    }
}

impl SystemVar {
    pub fn new(definition: VarDefinition) -> Self {
        SystemVar {
            definition,
            persisted_value: None,
            dynamic_default: None,
        }
    }

    fn is_default(&self, input: VarInput) -> Result<bool, VarError> {
        let v = self.definition.parse(input)?;
        Ok(self.definition.default_value() == v.as_ref())
    }

    pub fn value_dyn(&self) -> &dyn Value {
        self.persisted_value
            .as_deref()
            .or_else(|| self.dynamic_default.as_deref())
            .unwrap_or(self.definition.default_value())
    }

    pub fn value<V: 'static>(&self) -> &V {
        let val = self.value_dyn();
        val.as_any().downcast_ref::<V>().expect("success")
    }

    fn set(&mut self, input: VarInput) -> Result<bool, VarError> {
        let v = self.definition.parse(input)?;

        // Validate our parsed value.
        self.validate_constraints(v.as_ref())?;

        if self.persisted_value.as_ref() != Some(&v) {
            self.persisted_value = Some(v);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn reset(&mut self) -> bool {
        if self.persisted_value.is_some() {
            self.persisted_value = None;
            true
        } else {
            false
        }
    }

    fn set_default(&mut self, input: VarInput) -> Result<(), VarError> {
        let v = self.definition.parse(input)?;
        self.dynamic_default = Some(v);
        Ok(())
    }

    fn validate_constraints(&self, val: &dyn Value) -> Result<(), VarError> {
        if let Some(constraint) = &self.definition.constraint {
            constraint.check_constraint(self, self.value_dyn(), val)
        } else {
            Ok(())
        }
    }
}

impl Var for SystemVar {
    fn name(&self) -> &'static str {
        self.definition.name.as_str()
    }

    fn value(&self) -> String {
        self.value_dyn().format()
    }

    fn description(&self) -> &'static str {
        self.definition.description
    }

    fn type_name(&self) -> Cow<'static, str> {
        self.definition.type_name()
    }

    fn visible(
        &self,
        user: &User,
        system_vars: Option<&super::vars::SystemVars>,
    ) -> Result<(), super::vars::VarError> {
        self.definition.visible(user, system_vars)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ConnectionCounter {
    pub current: u64,
    // Callers must ensure this is always <= limit.
    pub superuser_reserved: u64,
    pub limit: u64,
}

impl ConnectionCounter {
    pub fn new(limit: u64, superuser_reserved: u64) -> Self {
        ConnectionCounter {
            current: 0,
            limit,
            superuser_reserved,
        }
    }

    fn assert(&self) {
        self.non_reserved_remaining();
        self.reserved_remaining();
        self.non_reserved_limit();
    }

    /// Whether a non-reserved connection is available.
    pub fn non_reserved_available(&self) -> bool {
        self.non_reserved_remaining() > 0
    }

    /// Whether a reserved connection is available.
    pub fn reserved_available(&self) -> bool {
        self.reserved_remaining() > 0
    }

    /// The number of non-reserved connections available.
    pub fn non_reserved_remaining(&self) -> u64 {
        // Saturate because there can be more connections than non-reserved slots.
        self.non_reserved_limit().saturating_sub(self.current)
    }

    /// The number of reserved connections available.
    pub fn reserved_remaining(&self) -> u64 {
        // Panic because there should never be more connections than the total limit.
        self.limit.checked_sub(self.current).expect("underflow")
    }

    /// The total limit for non-reserved connections.
    pub fn non_reserved_limit(&self) -> u64 {
        // Panic because superuser_reserved should always be <= limit.
        self.limit
            .checked_sub(self.superuser_reserved)
            .expect("underflow")
    }

    /// The total limit for reserved connections.
    pub fn reserved_limit(&self) -> u64 {
        self.limit
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    /// There were too many connections
    TooManyConnections { current: u64, limit: u64 },
}

#[derive(Debug)]
pub struct DropConnection {
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

impl Drop for DropConnection {
    fn drop(&mut self) {
        let mut connections = self.active_connection_count.lock().expect("lock poisoned");
        assert_ne!(connections.current, 0);
        connections.current -= 1;
        connections.assert();
    }
}

impl DropConnection {
    pub fn new_connection(
        user: &User,
        active_connection_count: Arc<Mutex<ConnectionCounter>>,
    ) -> Result<Option<Self>, ConnectionError> {
        Ok(if user.limit_max_connections() {
            {
                let mut connections = active_connection_count.lock().expect("lock poisoned");
                if user.is_external_admin() {
                    if !connections.reserved_available() {
                        return Err(ConnectionError::TooManyConnections {
                            current: connections.current,
                            limit: connections.reserved_limit(),
                        });
                    }
                } else if !connections.non_reserved_available() {
                    return Err(ConnectionError::TooManyConnections {
                        current: connections.current,
                        limit: connections.non_reserved_limit(),
                    });
                }
                connections.current += 1;
                connections.assert();
            }
            Some(DropConnection {
                active_connection_count,
            })
        } else {
            None
        })
    }
}

/// On disk variables.
///
/// See the [`crate::session::vars`] module documentation for more details on the
/// Materialize configuration model.
#[derive(Debug, Clone)]
pub struct SystemVars {
    /// Allows "unsafe" parameters to be set.
    allow_unsafe: bool,
    /// Set of all [`SystemVar`]s.
    vars: BTreeMap<&'static UncasedStr, SystemVar>,

    active_connection_count: Arc<Mutex<ConnectionCounter>>,
    /// NB: This is intentionally disconnected from the one that is plumbed
    /// around to various components (initially, just persist). This is so we
    /// can explictly control and reason about when changes to config values are
    /// propagated to the rest of the system.
    ///
    /// TODO(cfg): Rename this when components other than persist are pulled
    /// into it.
    persist_configs: ConfigSet,
}

impl Default for SystemVars {
    fn default() -> Self {
        Self::new(Arc::new(Mutex::new(ConnectionCounter::new(0, 0))))
    }
}

impl SystemVars {
    /// Set of [`SystemVar`]s that can also get set at a per-Session level.
    ///
    /// TODO(parkmycar): Instead of a separate list, make this a field on VarDefinition.
    const SESSION_VARS: Lazy<BTreeMap<&'static UncasedStr, &'static VarDefinition>> =
        Lazy::new(|| {
            [
                &APPLICATION_NAME,
                &CLIENT_ENCODING,
                &CLIENT_MIN_MESSAGES,
                &CLUSTER,
                &CLUSTER_REPLICA,
                &DATABASE,
                &DATE_STYLE,
                &EXTRA_FLOAT_DIGITS,
                &INTEGER_DATETIMES,
                &INTERVAL_STYLE,
                &SEARCH_PATH,
                &STANDARD_CONFORMING_STRINGS,
                &STATEMENT_TIMEOUT,
                &IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                &TIMEZONE,
                &TRANSACTION_ISOLATION,
            ]
            .into_iter()
            .map(|var| (UncasedStr::new(var.name()), var))
            .collect()
        });

    pub fn new(active_connection_count: Arc<Mutex<ConnectionCounter>>) -> Self {
        let system_vars = vec![
            &MAX_KAFKA_CONNECTIONS,
            &MAX_POSTGRES_CONNECTIONS,
            &MAX_AWS_PRIVATELINK_CONNECTIONS,
            &MAX_TABLES,
            &MAX_SOURCES,
            &MAX_SINKS,
            &MAX_MATERIALIZED_VIEWS,
            &MAX_CLUSTERS,
            &MAX_REPLICAS_PER_CLUSTER,
            &MAX_CREDIT_CONSUMPTION_RATE,
            &MAX_DATABASES,
            &MAX_SCHEMAS_PER_DATABASE,
            &MAX_OBJECTS_PER_SCHEMA,
            &MAX_SECRETS,
            &MAX_ROLES,
            &MAX_RESULT_SIZE,
            &MAX_COPY_FROM_SIZE,
            &ALLOWED_CLUSTER_REPLICA_SIZES,
            &DISK_CLUSTER_REPLICAS_DEFAULT,
            &upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK,
            &upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES,
            &upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE,
            &upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
            &upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
            &upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO,
            &upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM,
            &upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE,
            &upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE,
            &upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE,
            &upsert_rocksdb::UPSERT_ROCKSDB_RETRY_DURATION,
            &upsert_rocksdb::UPSERT_ROCKSDB_STATS_LOG_INTERVAL_SECONDS,
            &upsert_rocksdb::UPSERT_ROCKSDB_STATS_PERSIST_INTERVAL_SECONDS,
            &upsert_rocksdb::UPSERT_ROCKSDB_POINT_LOOKUP_BLOCK_CACHE_SIZE_MB,
            &upsert_rocksdb::UPSERT_ROCKSDB_SHRINK_ALLOCATED_BUFFERS_BY_RATIO,
            &upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_CLUSTER_MEMORY_FRACTION,
            &upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_MEMORY_BYTES,
            &upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_ALLOW_STALL,
            &COMPUTE_DATAFLOW_MAX_INFLIGHT_BYTES,
            &STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES,
            &STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_TO_CLUSTER_SIZE_FRACTION,
            &STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_DISK_ONLY,
            &STORAGE_STATISTICS_INTERVAL,
            &STORAGE_STATISTICS_COLLECTION_INTERVAL,
            &STORAGE_DATAFLOW_DELAY_SOURCES_PAST_REHYDRATION,
            &STORAGE_SHRINK_UPSERT_UNUSED_BUFFERS_BY_RATIO,
            &STORAGE_RECORD_SOURCE_SINK_NAMESPACED_ERRORS,
            &PERSIST_FAST_PATH_LIMIT,
            &PERSIST_TXN_TABLES,
            &CATALOG_KIND_IMPL,
            &METRICS_RETENTION,
            &UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP,
            &ENABLE_RBAC_CHECKS,
            &PG_SOURCE_CONNECT_TIMEOUT,
            &PG_SOURCE_KEEPALIVES_IDLE,
            &PG_SOURCE_KEEPALIVES_INTERVAL,
            &PG_SOURCE_KEEPALIVES_RETRIES,
            &PG_SOURCE_TCP_USER_TIMEOUT,
            &PG_SOURCE_SNAPSHOT_STATEMENT_TIMEOUT,
            &PG_SOURCE_SNAPSHOT_COLLECT_STRICT_COUNT,
            &PG_SOURCE_SNAPSHOT_FALLBACK_TO_STRICT_COUNT,
            &PG_SOURCE_SNAPSHOT_WAIT_FOR_COUNT,
            &SSH_CHECK_INTERVAL,
            &SSH_CONNECT_TIMEOUT,
            &SSH_KEEPALIVES_IDLE,
            &KAFKA_SOCKET_KEEPALIVE,
            &KAFKA_SOCKET_TIMEOUT,
            &KAFKA_TRANSACTION_TIMEOUT,
            &KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT,
            &KAFKA_FETCH_METADATA_TIMEOUT,
            &KAFKA_PROGRESS_RECORD_FETCH_TIMEOUT,
            &KAFKA_DEFAULT_METADATA_FETCH_INTERVAL,
            &ENABLE_LAUNCHDARKLY,
            &MAX_CONNECTIONS,
            &SUPERUSER_RESERVED_CONNECTIONS,
            &KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES,
            &KEEP_N_SINK_STATUS_HISTORY_ENTRIES,
            &KEEP_N_PRIVATELINK_STATUS_HISTORY_ENTRIES,
            &ENABLE_MZ_JOIN_CORE,
            &LINEAR_JOIN_YIELDING,
            &DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT,
            &DEFAULT_ARRANGEMENT_EXERT_PROPORTIONALITY,
            &ENABLE_STORAGE_SHARD_FINALIZATION,
            &ENABLE_CONSOLIDATE_AFTER_UNION_NEGATE,
            &ENABLE_DEFAULT_CONNECTION_VALIDATION,
            &MIN_TIMESTAMP_INTERVAL,
            &MAX_TIMESTAMP_INTERVAL,
            &LOGGING_FILTER,
            &OPENTELEMETRY_FILTER,
            &LOGGING_FILTER_DEFAULTS,
            &OPENTELEMETRY_FILTER_DEFAULTS,
            &SENTRY_FILTERS,
            &WEBHOOKS_SECRETS_CACHING_TTL_SECS,
            &COORD_SLOW_MESSAGE_WARN_THRESHOLD,
            &grpc_client::CONNECT_TIMEOUT,
            &grpc_client::HTTP2_KEEP_ALIVE_INTERVAL,
            &grpc_client::HTTP2_KEEP_ALIVE_TIMEOUT,
            &cluster_scheduling::CLUSTER_MULTI_PROCESS_REPLICA_AZ_AFFINITY_WEIGHT,
            &cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY,
            &cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT,
            &cluster_scheduling::CLUSTER_ENABLE_TOPOLOGY_SPREAD,
            &cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE,
            &cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_MAX_SKEW,
            &cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_SOFT,
            &cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY,
            &cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY_WEIGHT,
            &cluster_scheduling::CLUSTER_ALWAYS_USE_DISK,
            &grpc_client::HTTP2_KEEP_ALIVE_TIMEOUT,
            &STATEMENT_LOGGING_MAX_SAMPLE_RATE,
            &STATEMENT_LOGGING_DEFAULT_SAMPLE_RATE,
            &STATEMENT_LOGGING_TARGET_DATA_RATE,
            &STATEMENT_LOGGING_MAX_DATA_CREDIT,
            &OPTIMIZER_STATS_TIMEOUT,
            &OPTIMIZER_ONESHOT_STATS_TIMEOUT,
            &PRIVATELINK_STATUS_UPDATE_QUOTA_PER_MINUTE,
            &WEBHOOK_CONCURRENT_REQUEST_LIMIT,
            &ENABLE_COLUMNATION_LGALLOC,
            &ENABLE_COMPUTE_CHUNKED_STACK,
            &ENABLE_LGALLOC_EAGER_RECLAMATION,
            &ENABLE_STATEMENT_LIFECYCLE_LOGGING,
            &ENABLE_DEPENDENCY_READ_HOLD_ASSERTS,
            &TIMESTAMP_ORACLE_IMPL,
            &PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_SIZE,
            &PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_WAIT,
            &PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL,
            &PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL_STAGGER,
            &USER_STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION,
        ];

        let persist_configs = mz_dyncfgs::all_dyncfgs();
        let persist_vars: Vec<_> = persist_configs
            .entries()
            .map(|cfg| match cfg.default() {
                ConfigVal::Bool(default) => VarDefinition::new_runtime(
                    cfg.name(),
                    <bool as ConfigType>::get(default),
                    cfg.desc(),
                    true,
                ),
                ConfigVal::U32(default) => VarDefinition::new_runtime(
                    cfg.name(),
                    <u32 as ConfigType>::get(default),
                    cfg.desc(),
                    true,
                ),
                ConfigVal::Usize(default) => VarDefinition::new_runtime(
                    cfg.name(),
                    <usize as ConfigType>::get(default),
                    cfg.desc(),
                    true,
                ),
                ConfigVal::String(default) => VarDefinition::new_runtime(
                    cfg.name(),
                    <String as ConfigType>::get(default),
                    cfg.desc(),
                    true,
                ),
                ConfigVal::Duration(default) => VarDefinition::new_runtime(
                    cfg.name(),
                    <Duration as ConfigType>::get(default),
                    cfg.desc(),
                    true,
                ),
            })
            .collect();

        let vars: BTreeMap<_, _> = system_vars
            .into_iter()
            // Include all of our feature flags.
            .chain(definitions::FEATURE_FLAGS.iter().copied())
            // Include the subset of Session variables we allow system defaults for.
            .chain(Self::SESSION_VARS.values().copied())
            .cloned()
            // Include Persist configs.
            .chain(persist_vars.into_iter())
            .map(|var| (var.name, SystemVar::new(var)))
            .collect();

        let mut vars = SystemVars {
            vars,
            active_connection_count,
            allow_unsafe: false,
            persist_configs,
        };
        vars.refresh_internal_state();

        vars
    }

    pub fn set_unsafe(mut self, allow_unsafe: bool) -> Self {
        self.allow_unsafe = allow_unsafe;
        self
    }

    pub fn allow_unsafe(&self) -> bool {
        self.allow_unsafe
    }

    fn expect_value<V: 'static>(&self, var: &VarDefinition) -> &V {
        let val = self
            .vars
            .get(var.name)
            .expect("provided var should be in state");

        val.value_dyn()
            .as_any()
            .downcast_ref::<V>()
            .expect("provided var type should matched stored var")
    }

    fn expect_config_value<V: ConfigType + 'static>(&self, name: &UncasedStr) -> &V {
        let val = self
            .vars
            .get(name)
            .unwrap_or_else(|| panic!("provided var {name} should be in state"));

        val.value_dyn()
            .as_any()
            .downcast_ref()
            .expect("provided var type should matched stored var")
    }

    /// Reset all the values to their defaults (preserving
    /// defaults from `VarMut::set_default).
    pub fn reset_all(&mut self) {
        for (_, var) in &mut self.vars {
            var.reset();
        }
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values on disk.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        self.vars
            .values()
            .map(|v| v.as_var())
            .filter(|v| !Self::SESSION_VARS.contains_key(UncasedStr::new(v.name())))
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values on disk. Compared to [`SystemVars::iter`], this should omit vars
    /// that shouldn't be synced by SystemParameterFrontend.
    pub fn iter_synced(&self) -> impl Iterator<Item = &dyn Var> {
        self.iter().filter(|v| v.name() != ENABLE_LAUNCHDARKLY.name)
    }

    /// Returns an iterator over the configuration parameters that can be overriden per-Session.
    pub fn iter_session(&self) -> impl Iterator<Item = &dyn Var> {
        self.vars
            .values()
            .map(|v| v.as_var())
            .filter(|v| Self::SESSION_VARS.contains_key(UncasedStr::new(v.name())))
    }

    /// Returns whether or not this parameter can be modified by a superuser.
    pub fn user_modifiable(&self, name: &str) -> bool {
        Self::SESSION_VARS.contains_key(UncasedStr::new(name)) || name == ENABLE_RBAC_CHECKS.name()
    }

    /// Returns a [`Var`] representing the configuration parameter with the
    /// specified name.
    ///
    /// Configuration parameters are matched case insensitively. If no such
    /// configuration parameter exists, `get` returns an error.
    ///
    /// Note that:
    /// - If `name` is known at compile time, you should instead use the named
    /// accessor to access the variable with its true Rust type. For example,
    /// `self.get("max_tables").value()` returns the string `"25"` or the
    /// current value, while `self.max_tables()` returns an i32.
    ///
    /// - This function does not check that the access variable should be
    /// visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn get(&self, name: &str) -> Result<&dyn Var, VarError> {
        self.vars
            .get(UncasedStr::new(name))
            .map(|v| v.as_var())
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
    }

    /// Check if the given `values` is the default value for the [`Var`]
    /// identified by `name`.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `values` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn is_default(&self, name: &str, input: VarInput) -> Result<bool, VarError> {
        self.vars
            .get(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.is_default(input))
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`SystemVars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    ///
    /// Return a `bool` value indicating whether the [`Var`] identified by
    /// `name` was modified by this call (it won't be if it already had the
    /// given `value`).
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `value` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn set(&mut self, name: &str, input: VarInput) -> Result<bool, VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.set(input))?;
        self.propagate_var_change(name);
        Ok(result)
    }

    /// Set the default for this variable. This is the value this
    /// variable will be be `reset` to. If no default is set, the static default in the
    /// variable definition is used instead.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    pub fn set_default(&mut self, name: &str, input: VarInput) -> Result<(), VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.set_default(input))?;
        self.propagate_var_change(name);
        Ok(result)
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// Like with [`SystemVars::get`], configuration parameters are matched case
    /// insensitively. If the named configuration parameter does not exist, an
    /// error is returned.
    ///
    /// Return a `bool` value indicating whether the [`Var`] identified by
    /// `name` was modified by this call (it won't be if was already reset).
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn reset(&mut self, name: &str) -> Result<bool, VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .map(|v| v.reset())?;
        self.propagate_var_change(name);
        Ok(result)
    }

    /// Propagate a change to the parameter named `name` to our state.
    fn propagate_var_change(&mut self, name: &str) {
        if name == MAX_CONNECTIONS.name || name == SUPERUSER_RESERVED_CONNECTIONS.name {
            let limit = *self.expect_value::<u32>(&MAX_CONNECTIONS);
            let superuser_reserved = *self.expect_value::<u32>(&SUPERUSER_RESERVED_CONNECTIONS);
            // If superuser_reserved > max_connections, prefer max_connections.
            let superuser_reserved = std::cmp::min(limit, superuser_reserved);
            let mut connections = self.active_connection_count.lock().expect("lock poisoned");
            connections.assert();
            connections.limit = u64::cast_from(limit);
            connections.superuser_reserved = u64::cast_from(superuser_reserved);
            connections.assert();
        }
    }

    /// Make sure that the internal state matches the SystemVars. Generally
    /// only needed when initializing, `set`, `set_default`, and `reset`
    /// are responsible for keeping the internal state in sync with
    /// the affected SystemVars.
    fn refresh_internal_state(&mut self) {
        self.propagate_var_change(MAX_CONNECTIONS.name.as_str());
        self.propagate_var_change(SUPERUSER_RESERVED_CONNECTIONS.name.as_str());
    }

    /// Returns the system default for the [`CLUSTER`] session variable. To know the active cluster
    /// for the current session, you must check the [`SessionVars`].
    pub fn default_cluster(&self) -> String {
        self.expect_value::<String>(&CLUSTER).to_owned()
    }

    /// Returns the value of the `max_kafka_connections` configuration parameter.
    pub fn max_kafka_connections(&self) -> u32 {
        *self.expect_value(&MAX_KAFKA_CONNECTIONS)
    }

    /// Returns the value of the `max_postgres_connections` configuration parameter.
    pub fn max_postgres_connections(&self) -> u32 {
        *self.expect_value(&MAX_POSTGRES_CONNECTIONS)
    }

    /// Returns the value of the `max_aws_privatelink_connections` configuration parameter.
    pub fn max_aws_privatelink_connections(&self) -> u32 {
        *self.expect_value(&MAX_AWS_PRIVATELINK_CONNECTIONS)
    }

    /// Returns the value of the `max_tables` configuration parameter.
    pub fn max_tables(&self) -> u32 {
        *self.expect_value(&MAX_TABLES)
    }

    /// Returns the value of the `max_sources` configuration parameter.
    pub fn max_sources(&self) -> u32 {
        *self.expect_value(&MAX_SOURCES)
    }

    /// Returns the value of the `max_sinks` configuration parameter.
    pub fn max_sinks(&self) -> u32 {
        *self.expect_value(&MAX_SINKS)
    }

    /// Returns the value of the `max_materialized_views` configuration parameter.
    pub fn max_materialized_views(&self) -> u32 {
        *self.expect_value(&MAX_MATERIALIZED_VIEWS)
    }

    /// Returns the value of the `max_clusters` configuration parameter.
    pub fn max_clusters(&self) -> u32 {
        *self.expect_value(&MAX_CLUSTERS)
    }

    /// Returns the value of the `max_replicas_per_cluster` configuration parameter.
    pub fn max_replicas_per_cluster(&self) -> u32 {
        *self.expect_value(&MAX_REPLICAS_PER_CLUSTER)
    }

    /// Returns the value of the `max_credit_consumption_rate` configuration parameter.
    pub fn max_credit_consumption_rate(&self) -> Numeric {
        *self.expect_value(&MAX_CREDIT_CONSUMPTION_RATE)
    }

    /// Returns the value of the `max_databases` configuration parameter.
    pub fn max_databases(&self) -> u32 {
        *self.expect_value(&MAX_DATABASES)
    }

    /// Returns the value of the `max_schemas_per_database` configuration parameter.
    pub fn max_schemas_per_database(&self) -> u32 {
        *self.expect_value(&MAX_SCHEMAS_PER_DATABASE)
    }

    /// Returns the value of the `max_objects_per_schema` configuration parameter.
    pub fn max_objects_per_schema(&self) -> u32 {
        *self.expect_value(&MAX_OBJECTS_PER_SCHEMA)
    }

    /// Returns the value of the `max_secrets` configuration parameter.
    pub fn max_secrets(&self) -> u32 {
        *self.expect_value(&MAX_SECRETS)
    }

    /// Returns the value of the `max_roles` configuration parameter.
    pub fn max_roles(&self) -> u32 {
        *self.expect_value(&MAX_ROLES)
    }

    /// Returns the value of the `max_result_size` configuration parameter.
    pub fn max_result_size(&self) -> u64 {
        self.expect_value::<ByteSize>(&MAX_RESULT_SIZE).as_bytes()
    }

    /// Returns the value of the `max_copy_from_size` configuration parameter.
    pub fn max_copy_from_size(&self) -> u32 {
        *self.expect_value(&MAX_COPY_FROM_SIZE)
    }

    /// Returns the value of the `allowed_cluster_replica_sizes` configuration parameter.
    pub fn allowed_cluster_replica_sizes(&self) -> Vec<String> {
        self.expect_value::<Vec<Ident>>(&ALLOWED_CLUSTER_REPLICA_SIZES)
            .into_iter()
            .map(|s| s.as_str().into())
            .collect()
    }

    /// Returns the `disk_cluster_replicas_default` configuration parameter.
    pub fn disk_cluster_replicas_default(&self) -> bool {
        *self.expect_value(&DISK_CLUSTER_REPLICAS_DEFAULT)
    }

    pub fn upsert_rocksdb_auto_spill_to_disk(&self) -> bool {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK)
    }

    pub fn upsert_rocksdb_auto_spill_threshold_bytes(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES)
    }

    pub fn upsert_rocksdb_compaction_style(&self) -> mz_rocksdb_types::config::CompactionStyle {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE)
    }

    pub fn upsert_rocksdb_optimize_compaction_memtable_budget(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET)
    }

    pub fn upsert_rocksdb_level_compaction_dynamic_level_bytes(&self) -> bool {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES)
    }

    pub fn upsert_rocksdb_universal_compaction_ratio(&self) -> i32 {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO)
    }

    pub fn upsert_rocksdb_parallelism(&self) -> Option<i32> {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM)
    }

    pub fn upsert_rocksdb_compression_type(&self) -> mz_rocksdb_types::config::CompressionType {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE)
    }

    pub fn upsert_rocksdb_bottommost_compression_type(
        &self,
    ) -> mz_rocksdb_types::config::CompressionType {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE)
    }

    pub fn upsert_rocksdb_batch_size(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE)
    }

    pub fn upsert_rocksdb_retry_duration(&self) -> Duration {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_RETRY_DURATION)
    }

    pub fn upsert_rocksdb_stats_log_interval_seconds(&self) -> u32 {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_STATS_LOG_INTERVAL_SECONDS)
    }

    pub fn upsert_rocksdb_stats_persist_interval_seconds(&self) -> u32 {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_STATS_PERSIST_INTERVAL_SECONDS)
    }

    pub fn upsert_rocksdb_point_lookup_block_cache_size_mb(&self) -> Option<u32> {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_POINT_LOOKUP_BLOCK_CACHE_SIZE_MB)
    }

    pub fn upsert_rocksdb_shrink_allocated_buffers_by_ratio(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_SHRINK_ALLOCATED_BUFFERS_BY_RATIO)
    }

    pub fn upsert_rocksdb_write_buffer_manager_cluster_memory_fraction(&self) -> Option<Numeric> {
        *self.expect_value(
            &upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_CLUSTER_MEMORY_FRACTION,
        )
    }

    pub fn upsert_rocksdb_write_buffer_manager_memory_bytes(&self) -> Option<usize> {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_MEMORY_BYTES)
    }

    pub fn upsert_rocksdb_write_buffer_manager_allow_stall(&self) -> bool {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_ALLOW_STALL)
    }

    pub fn persist_fast_path_limit(&self) -> usize {
        *self.expect_value(&PERSIST_FAST_PATH_LIMIT)
    }

    pub fn persist_txn_tables(&self) -> PersistTxnTablesImpl {
        *self.expect_value(&PERSIST_TXN_TABLES)
    }

    pub fn catalog_kind(&self) -> Option<CatalogKind> {
        *self.expect_value(&CATALOG_KIND_IMPL)
    }

    /// Returns the `pg_source_connect_timeout` configuration parameter.
    pub fn pg_source_connect_timeout(&self) -> Duration {
        *self.expect_value(&PG_SOURCE_CONNECT_TIMEOUT)
    }

    /// Returns the `pg_source_keepalives_retries` configuration parameter.
    pub fn pg_source_keepalives_retries(&self) -> u32 {
        *self.expect_value(&PG_SOURCE_KEEPALIVES_RETRIES)
    }

    /// Returns the `pg_source_keepalives_idle` configuration parameter.
    pub fn pg_source_keepalives_idle(&self) -> Duration {
        *self.expect_value(&PG_SOURCE_KEEPALIVES_IDLE)
    }

    /// Returns the `pg_source_keepalives_interval` configuration parameter.
    pub fn pg_source_keepalives_interval(&self) -> Duration {
        *self.expect_value(&PG_SOURCE_KEEPALIVES_INTERVAL)
    }

    /// Returns the `pg_source_tcp_user_timeout` configuration parameter.
    pub fn pg_source_tcp_user_timeout(&self) -> Duration {
        *self.expect_value(&PG_SOURCE_TCP_USER_TIMEOUT)
    }

    /// Returns the `pg_source_snapshot_statement_timeout` configuration parameter.
    pub fn pg_source_snapshot_statement_timeout(&self) -> Duration {
        *self.expect_value(&PG_SOURCE_SNAPSHOT_STATEMENT_TIMEOUT)
    }

    /// Returns the `pg_source_snapshot_collect_strict_count` configuration parameter.
    pub fn pg_source_snapshot_collect_strict_count(&self) -> bool {
        *self.expect_value(&PG_SOURCE_SNAPSHOT_COLLECT_STRICT_COUNT)
    }
    /// Returns the `pg_source_snapshot_fallback_to_strict_count` configuration parameter.
    pub fn pg_source_snapshot_fallback_to_strict_count(&self) -> bool {
        *self.expect_value(&PG_SOURCE_SNAPSHOT_FALLBACK_TO_STRICT_COUNT)
    }
    /// Returns the `pg_source_snapshot_collect_strict_count` configuration parameter.
    pub fn pg_source_snapshot_wait_for_count(&self) -> bool {
        *self.expect_value(&PG_SOURCE_SNAPSHOT_WAIT_FOR_COUNT)
    }

    /// Returns the `ssh_check_interval` configuration parameter.
    pub fn ssh_check_interval(&self) -> Duration {
        *self.expect_value(&SSH_CHECK_INTERVAL)
    }

    /// Returns the `ssh_connect_timeout` configuration parameter.
    pub fn ssh_connect_timeout(&self) -> Duration {
        *self.expect_value(&SSH_CONNECT_TIMEOUT)
    }

    /// Returns the `ssh_keepalives_idle` configuration parameter.
    pub fn ssh_keepalives_idle(&self) -> Duration {
        *self.expect_value(&SSH_KEEPALIVES_IDLE)
    }

    /// Returns the `kafka_socket_keepalive` configuration parameter.
    pub fn kafka_socket_keepalive(&self) -> bool {
        *self.expect_value(&KAFKA_SOCKET_KEEPALIVE)
    }

    /// Returns the `kafka_socket_timeout` configuration parameter.
    pub fn kafka_socket_timeout(&self) -> Duration {
        *self.expect_value(&KAFKA_SOCKET_TIMEOUT)
    }

    /// Returns the `kafka_transaction_timeout` configuration parameter.
    pub fn kafka_transaction_timeout(&self) -> Duration {
        *self.expect_value(&KAFKA_TRANSACTION_TIMEOUT)
    }

    /// Returns the `kafka_socket_connection_setup_timeout` configuration parameter.
    pub fn kafka_socket_connection_setup_timeout(&self) -> Duration {
        *self.expect_value(&KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT)
    }

    /// Returns the `kafka_fetch_metadata_timeout` configuration parameter.
    pub fn kafka_fetch_metadata_timeout(&self) -> Duration {
        *self.expect_value(&KAFKA_FETCH_METADATA_TIMEOUT)
    }

    /// Returns the `kafka_progress_record_fetch_timeout` configuration parameter.
    pub fn kafka_progress_record_fetch_timeout(&self) -> Duration {
        *self.expect_value(&KAFKA_PROGRESS_RECORD_FETCH_TIMEOUT)
    }

    /// Returns the `kafka_default_metadata_fetch_interval` configuration parameter.
    pub fn kafka_default_metadata_fetch_interval(&self) -> Duration {
        *self.expect_value(&KAFKA_DEFAULT_METADATA_FETCH_INTERVAL)
    }

    /// Returns the `crdb_connect_timeout` configuration parameter.
    pub fn crdb_connect_timeout(&self) -> Duration {
        *self.expect_config_value(UncasedStr::new(
            mz_persist_client::cfg::CRDB_CONNECT_TIMEOUT.name(),
        ))
    }

    /// Returns the `crdb_tcp_user_timeout` configuration parameter.
    pub fn crdb_tcp_user_timeout(&self) -> Duration {
        *self.expect_config_value(UncasedStr::new(
            mz_persist_client::cfg::CRDB_TCP_USER_TIMEOUT.name(),
        ))
    }

    /// Returns the `compute_dataflow_max_inflight_bytes` configuration parameter.
    pub fn compute_dataflow_max_inflight_bytes(&self) -> Option<usize> {
        *self.expect_value(&COMPUTE_DATAFLOW_MAX_INFLIGHT_BYTES)
    }

    /// Returns the `storage_dataflow_max_inflight_bytes` configuration parameter.
    pub fn storage_dataflow_max_inflight_bytes(&self) -> Option<usize> {
        *self.expect_value(&STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES)
    }

    /// Returns the `storage_dataflow_max_inflight_bytes_to_cluster_size_fraction` configuration parameter.
    pub fn storage_dataflow_max_inflight_bytes_to_cluster_size_fraction(&self) -> Option<Numeric> {
        *self.expect_value(&STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_TO_CLUSTER_SIZE_FRACTION)
    }

    /// Returns the `storage_dataflow_max_inflight_bytes` configuration parameter.
    pub fn storage_dataflow_delay_sources_past_rehydration(&self) -> bool {
        *self.expect_value(&STORAGE_DATAFLOW_DELAY_SOURCES_PAST_REHYDRATION)
    }

    /// Returns the `storage_shrink_upsert_unused_buffers_by_ratio` configuration parameter.
    pub fn storage_shrink_upsert_unused_buffers_by_ratio(&self) -> usize {
        *self.expect_value(&STORAGE_SHRINK_UPSERT_UNUSED_BUFFERS_BY_RATIO)
    }

    /// Returns the `storage_dataflow_max_inflight_bytes_disk_only` configuration parameter.
    pub fn storage_dataflow_max_inflight_bytes_disk_only(&self) -> bool {
        *self.expect_value(&STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_DISK_ONLY)
    }

    /// Returns the `storage_statistics_interval` configuration parameter.
    pub fn storage_statistics_interval(&self) -> Duration {
        *self.expect_value(&STORAGE_STATISTICS_INTERVAL)
    }

    /// Returns the `storage_statistics_collection_interval` configuration parameter.
    pub fn storage_statistics_collection_interval(&self) -> Duration {
        *self.expect_value(&STORAGE_STATISTICS_COLLECTION_INTERVAL)
    }

    /// Returns the `storage_record_source_sink_namespaced_errors` configuration parameter.
    pub fn storage_record_source_sink_namespaced_errors(&self) -> bool {
        *self.expect_value(&STORAGE_RECORD_SOURCE_SINK_NAMESPACED_ERRORS)
    }

    /// Returns the `persist_stats_filter_enabled` configuration parameter.
    pub fn persist_stats_filter_enabled(&self) -> bool {
        *self.expect_config_value(UncasedStr::new(
            mz_persist_client::stats::STATS_FILTER_ENABLED.name(),
        ))
    }

    pub fn persist_configs(&self) -> PersistConfigUpdates {
        let mut updates = PersistConfigUpdates::default();
        for entry in self.persist_configs.entries() {
            let name = UncasedStr::new(entry.name());
            match entry.val() {
                ConfigVal::Bool(x) => {
                    <bool as ConfigType>::set(x, *self.expect_config_value::<bool>(name))
                }
                ConfigVal::U32(x) => {
                    <u32 as ConfigType>::set(x, *self.expect_config_value::<u32>(name))
                }
                ConfigVal::Usize(x) => {
                    <usize as ConfigType>::set(x, *self.expect_config_value::<usize>(name))
                }
                ConfigVal::String(x) => {
                    <String as ConfigType>::set(x, self.expect_config_value::<String>(name).clone())
                }
                ConfigVal::Duration(x) => {
                    <Duration as ConfigType>::set(x, *self.expect_config_value::<Duration>(name))
                }
            };
            updates.add(entry);
        }
        updates
    }

    /// Returns the `metrics_retention` configuration parameter.
    pub fn metrics_retention(&self) -> Duration {
        *self.expect_value(&METRICS_RETENTION)
    }

    /// Returns the `unsafe_mock_audit_event_timestamp` configuration parameter.
    pub fn unsafe_mock_audit_event_timestamp(&self) -> Option<mz_repr::Timestamp> {
        *self.expect_value(&UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP)
    }

    /// Returns the `enable_rbac_checks` configuration parameter.
    pub fn enable_rbac_checks(&self) -> bool {
        *self.expect_value(&ENABLE_RBAC_CHECKS)
    }

    /// Returns the `max_connections` configuration parameter.
    pub fn max_connections(&self) -> u32 {
        *self.expect_value(&MAX_CONNECTIONS)
    }

    /// Returns the `superuser_reserved_connections` configuration parameter.
    pub fn superuser_reserved_connections(&self) -> u32 {
        *self.expect_value(&SUPERUSER_RESERVED_CONNECTIONS)
    }

    pub fn keep_n_source_status_history_entries(&self) -> usize {
        *self.expect_value(&KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES)
    }

    pub fn keep_n_sink_status_history_entries(&self) -> usize {
        *self.expect_value(&KEEP_N_SINK_STATUS_HISTORY_ENTRIES)
    }

    pub fn keep_n_privatelink_status_history_entries(&self) -> usize {
        *self.expect_value(&KEEP_N_PRIVATELINK_STATUS_HISTORY_ENTRIES)
    }

    /// Returns the `enable_mz_join_core` configuration parameter.
    pub fn enable_mz_join_core(&self) -> bool {
        *self.expect_value(&ENABLE_MZ_JOIN_CORE)
    }

    /// Returns the `linear_join_yielding` configuration parameter.
    pub fn linear_join_yielding(&self) -> &Cow<'static, str> {
        self.expect_value(&LINEAR_JOIN_YIELDING)
    }

    /// Returns the `default_idle_arrangement_merge_effort` configuration parameter.
    pub fn default_idle_arrangement_merge_effort(&self) -> u32 {
        *self.expect_value(&DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT)
    }

    /// Returns the `default_arrangement_exert_proportionality` configuration parameter.
    pub fn default_arrangement_exert_proportionality(&self) -> u32 {
        *self.expect_value(&DEFAULT_ARRANGEMENT_EXERT_PROPORTIONALITY)
    }

    /// Returns the `enable_storage_shard_finalization` configuration parameter.
    pub fn enable_storage_shard_finalization(&self) -> bool {
        *self.expect_value(&ENABLE_STORAGE_SHARD_FINALIZATION)
    }

    pub fn enable_consolidate_after_union_negate(&self) -> bool {
        *self.expect_value(&ENABLE_CONSOLIDATE_AFTER_UNION_NEGATE)
    }

    /// Returns the `enable_default_connection_validation` configuration parameter.
    pub fn enable_default_connection_validation(&self) -> bool {
        *self.expect_value(&ENABLE_DEFAULT_CONNECTION_VALIDATION)
    }

    /// Returns the `min_timestamp_interval` configuration parameter.
    pub fn min_timestamp_interval(&self) -> Duration {
        *self.expect_value(&MIN_TIMESTAMP_INTERVAL)
    }
    /// Returns the `max_timestamp_interval` configuration parameter.
    pub fn max_timestamp_interval(&self) -> Duration {
        *self.expect_value(&MAX_TIMESTAMP_INTERVAL)
    }

    pub fn logging_filter(&self) -> CloneableEnvFilter {
        self.expect_value::<CloneableEnvFilter>(&LOGGING_FILTER)
            .clone()
    }

    pub fn opentelemetry_filter(&self) -> CloneableEnvFilter {
        self.expect_value::<CloneableEnvFilter>(&OPENTELEMETRY_FILTER)
            .clone()
    }

    pub fn logging_filter_defaults(&self) -> Vec<SerializableDirective> {
        self.expect_value::<Vec<SerializableDirective>>(&LOGGING_FILTER_DEFAULTS)
            .clone()
    }

    pub fn opentelemetry_filter_defaults(&self) -> Vec<SerializableDirective> {
        self.expect_value::<Vec<SerializableDirective>>(&OPENTELEMETRY_FILTER_DEFAULTS)
            .clone()
    }

    pub fn sentry_filters(&self) -> Vec<SerializableDirective> {
        self.expect_value::<Vec<SerializableDirective>>(&SENTRY_FILTERS)
            .clone()
    }

    pub fn webhooks_secrets_caching_ttl_secs(&self) -> usize {
        *self.expect_value(&WEBHOOKS_SECRETS_CACHING_TTL_SECS)
    }

    pub fn coord_slow_message_warn_threshold(&self) -> Duration {
        *self.expect_value(&COORD_SLOW_MESSAGE_WARN_THRESHOLD)
    }

    pub fn grpc_client_http2_keep_alive_interval(&self) -> Duration {
        *self.expect_value(&grpc_client::HTTP2_KEEP_ALIVE_INTERVAL)
    }

    pub fn grpc_client_http2_keep_alive_timeout(&self) -> Duration {
        *self.expect_value(&grpc_client::HTTP2_KEEP_ALIVE_TIMEOUT)
    }

    pub fn grpc_connect_timeout(&self) -> Duration {
        *self.expect_value(&grpc_client::CONNECT_TIMEOUT)
    }

    pub fn cluster_multi_process_replica_az_affinity_weight(&self) -> Option<i32> {
        *self.expect_value(&cluster_scheduling::CLUSTER_MULTI_PROCESS_REPLICA_AZ_AFFINITY_WEIGHT)
    }

    pub fn cluster_soften_replication_anti_affinity(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY)
    }

    pub fn cluster_soften_replication_anti_affinity_weight(&self) -> i32 {
        *self.expect_value(&cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT)
    }

    pub fn cluster_enable_topology_spread(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_ENABLE_TOPOLOGY_SPREAD)
    }

    pub fn cluster_topology_spread_ignore_non_singular_scale(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE)
    }

    pub fn cluster_topology_spread_max_skew(&self) -> i32 {
        *self.expect_value(&cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_MAX_SKEW)
    }

    pub fn cluster_topology_spread_soft(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_SOFT)
    }

    pub fn cluster_soften_az_affinity(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY)
    }

    pub fn cluster_soften_az_affinity_weight(&self) -> i32 {
        *self.expect_value(&cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY_WEIGHT)
    }

    pub fn cluster_always_use_disk(&self) -> bool {
        *self.expect_value(&cluster_scheduling::CLUSTER_ALWAYS_USE_DISK)
    }

    /// Returns the `privatelink_status_update_quota_per_minute` configuration parameter.
    pub fn privatelink_status_update_quota_per_minute(&self) -> u32 {
        *self.expect_value(&PRIVATELINK_STATUS_UPDATE_QUOTA_PER_MINUTE)
    }

    pub fn statement_logging_target_data_rate(&self) -> Option<usize> {
        *self.expect_value(&STATEMENT_LOGGING_TARGET_DATA_RATE)
    }

    pub fn statement_logging_max_data_credit(&self) -> Option<usize> {
        *self.expect_value(&STATEMENT_LOGGING_MAX_DATA_CREDIT)
    }

    /// Returns the `statement_logging_max_sample_rate` configuration parameter.
    pub fn statement_logging_max_sample_rate(&self) -> Numeric {
        *self.expect_value(&STATEMENT_LOGGING_MAX_SAMPLE_RATE)
    }

    /// Returns the `statement_logging_default_sample_rate` configuration parameter.
    pub fn statement_logging_default_sample_rate(&self) -> Numeric {
        *self.expect_value(&STATEMENT_LOGGING_DEFAULT_SAMPLE_RATE)
    }

    /// Returns the `optimizer_stats_timeout` configuration parameter.
    pub fn optimizer_stats_timeout(&self) -> Duration {
        *self.expect_value(&OPTIMIZER_STATS_TIMEOUT)
    }

    /// Returns the `optimizer_oneshot_stats_timeout` configuration parameter.
    pub fn optimizer_oneshot_stats_timeout(&self) -> Duration {
        *self.expect_value(&OPTIMIZER_ONESHOT_STATS_TIMEOUT)
    }

    /// Returns the `webhook_concurrent_request_limit` configuration parameter.
    pub fn webhook_concurrent_request_limit(&self) -> usize {
        *self.expect_value(&WEBHOOK_CONCURRENT_REQUEST_LIMIT)
    }

    /// Returns the `enable_columnation_lgalloc` configuration parameter.
    pub fn enable_columnation_lgalloc(&self) -> bool {
        *self.expect_value(&ENABLE_COLUMNATION_LGALLOC)
    }

    /// Returns the `enable_compute_chunked_stack` configuration parameter.
    pub fn enable_compute_chunked_stack(&self) -> bool {
        *self.expect_value(&ENABLE_COMPUTE_CHUNKED_STACK)
    }

    /// Returns the `enable_lgalloc_eager_reclamation` configuration parameter.
    pub fn enable_lgalloc_eager_reclamation(&self) -> bool {
        *self.expect_value(&ENABLE_LGALLOC_EAGER_RECLAMATION)
    }

    pub fn enable_statement_lifecycle_logging(&self) -> bool {
        *self.expect_value(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
    }

    /// Returns the `timestamp_oracle` configuration parameter.
    pub fn timestamp_oracle_impl(&self) -> TimestampOracleImpl {
        *self.expect_value(&TIMESTAMP_ORACLE_IMPL)
    }

    /// Returns the `pg_timestamp_oracle_connection_pool_max_size` configuration parameter.
    pub fn pg_timestamp_oracle_connection_pool_max_size(&self) -> usize {
        *self.expect_value(&PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_SIZE)
    }

    /// Returns the `pg_timestamp_oracle_connection_pool_max_wait` configuration parameter.
    pub fn pg_timestamp_oracle_connection_pool_max_wait(&self) -> Option<Duration> {
        *self.expect_value(&PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_WAIT)
    }

    /// Returns the `pg_timestamp_oracle_connection_pool_ttl` configuration parameter.
    pub fn pg_timestamp_oracle_connection_pool_ttl(&self) -> Duration {
        *self.expect_value(&PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL)
    }

    /// Returns the `pg_timestamp_oracle_connection_pool_ttl_stagger` configuration parameter.
    pub fn pg_timestamp_oracle_connection_pool_ttl_stagger(&self) -> Duration {
        *self.expect_value(&PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL_STAGGER)
    }

    pub fn enable_dependency_read_hold_asserts(&self) -> bool {
        *self.expect_value(&ENABLE_DEPENDENCY_READ_HOLD_ASSERTS)
    }

    /// Returns the `user_storage_managed_collections_batch_duration` configuration parameter.
    pub fn user_storage_managed_collections_batch_duration(&self) -> Duration {
        *self.expect_value(&USER_STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION)
    }

    /// Returns whether the named variable is a compute configuration parameter
    /// (things that go in `ComputeParameters` and are sent to replicas via `UpdateConfiguration`
    /// commands).
    pub fn is_compute_config_var(&self, name: &str) -> bool {
        name == MAX_RESULT_SIZE.name()
            || name == COMPUTE_DATAFLOW_MAX_INFLIGHT_BYTES.name()
            || name == LINEAR_JOIN_YIELDING.name()
            || name == ENABLE_MZ_JOIN_CORE.name()
            || name == ENABLE_COLUMNATION_LGALLOC.name()
            || name == ENABLE_COMPUTE_CHUNKED_STACK.name()
            || name == ENABLE_COMPUTE_OPERATOR_HYDRATION_STATUS_LOGGING.name()
            || name == ENABLE_LGALLOC_EAGER_RECLAMATION.name()
            || self.is_persist_config_var(name)
            || is_tracing_var(name)
    }

    /// Returns whether the named variable is a storage configuration parameter.
    pub fn is_storage_config_var(&self, name: &str) -> bool {
        name == PG_SOURCE_CONNECT_TIMEOUT.name()
            || name == PG_SOURCE_KEEPALIVES_IDLE.name()
            || name == PG_SOURCE_KEEPALIVES_INTERVAL.name()
            || name == PG_SOURCE_KEEPALIVES_RETRIES.name()
            || name == PG_SOURCE_TCP_USER_TIMEOUT.name()
            || name == PG_SOURCE_SNAPSHOT_STATEMENT_TIMEOUT.name()
            || name == PG_SOURCE_SNAPSHOT_COLLECT_STRICT_COUNT.name()
            || name == PG_SOURCE_SNAPSHOT_FALLBACK_TO_STRICT_COUNT.name()
            || name == PG_SOURCE_SNAPSHOT_WAIT_FOR_COUNT.name()
            || name == ENABLE_STORAGE_SHARD_FINALIZATION.name()
            || name == SSH_CHECK_INTERVAL.name()
            || name == SSH_CONNECT_TIMEOUT.name()
            || name == SSH_KEEPALIVES_IDLE.name()
            || name == KAFKA_SOCKET_KEEPALIVE.name()
            || name == KAFKA_SOCKET_TIMEOUT.name()
            || name == KAFKA_TRANSACTION_TIMEOUT.name()
            || name == KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT.name()
            || name == KAFKA_FETCH_METADATA_TIMEOUT.name()
            || name == KAFKA_PROGRESS_RECORD_FETCH_TIMEOUT.name()
            || name == KAFKA_DEFAULT_METADATA_FETCH_INTERVAL.name()
            || name == STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES.name()
            || name == STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_TO_CLUSTER_SIZE_FRACTION.name()
            || name == STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_DISK_ONLY.name()
            || name == STORAGE_DATAFLOW_DELAY_SOURCES_PAST_REHYDRATION.name()
            || name == STORAGE_SHRINK_UPSERT_UNUSED_BUFFERS_BY_RATIO.name()
            || name == STORAGE_RECORD_SOURCE_SINK_NAMESPACED_ERRORS.name()
            || name == STORAGE_STATISTICS_INTERVAL.name()
            || name == STORAGE_STATISTICS_COLLECTION_INTERVAL.name()
            || name == USER_STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION.name()
            || is_upsert_rocksdb_config_var(name)
            || self.is_persist_config_var(name)
            || is_tracing_var(name)
    }

    /// Returns whether the named variable is a persist configuration parameter.
    fn is_persist_config_var(&self, name: &str) -> bool {
        self.persist_configs.entries().any(|e| name == e.name())
    }
}

pub fn is_tracing_var(name: &str) -> bool {
    name == LOGGING_FILTER.name()
        || name == LOGGING_FILTER_DEFAULTS.name()
        || name == OPENTELEMETRY_FILTER.name()
        || name == OPENTELEMETRY_FILTER_DEFAULTS.name()
        || name == SENTRY_FILTERS.name()
}

/// Returns whether the named variable is a caching configuration parameter.
pub fn is_secrets_caching_var(name: &str) -> bool {
    name == WEBHOOKS_SECRETS_CACHING_TTL_SECS.name()
}

fn is_upsert_rocksdb_config_var(name: &str) -> bool {
    name == upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_STATS_LOG_INTERVAL_SECONDS.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_STATS_PERSIST_INTERVAL_SECONDS.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_POINT_LOOKUP_BLOCK_CACHE_SIZE_MB.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_SHRINK_ALLOCATED_BUFFERS_BY_RATIO.name()
}

/// Returns whether the named variable is a Postgres/CRDB timestamp oracle
/// configuration parameter.
pub fn is_pg_timestamp_oracle_config_var(name: &str) -> bool {
    name == PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_SIZE.name()
        || name == PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_WAIT.name()
        || name == PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL.name()
        || name == PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL_STAGGER.name()
        || name == CRDB_CONNECT_TIMEOUT.name()
        || name == CRDB_TCP_USER_TIMEOUT.name()
}

/// Returns whether the named variable is a cluster scheduling config
pub fn is_cluster_scheduling_var(name: &str) -> bool {
    name == cluster_scheduling::CLUSTER_MULTI_PROCESS_REPLICA_AZ_AFFINITY_WEIGHT.name()
        || name == cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY.name()
        || name == cluster_scheduling::CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT.name()
        || name == cluster_scheduling::CLUSTER_ENABLE_TOPOLOGY_SPREAD.name()
        || name == cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE.name()
        || name == cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_MAX_SKEW.name()
        || name == cluster_scheduling::CLUSTER_TOPOLOGY_SPREAD_SOFT.name()
        || name == cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY.name()
        || name == cluster_scheduling::CLUSTER_SOFTEN_AZ_AFFINITY_WEIGHT.name()
        || name == cluster_scheduling::CLUSTER_ALWAYS_USE_DISK.name()
}

/// Returns whether the named variable is an HTTP server related config var.
pub fn is_http_config_var(name: &str) -> bool {
    name == WEBHOOK_CONCURRENT_REQUEST_LIMIT.name()
}

// Provides a wrapper to express that a particular `ServerVar` is meant to be used as a feature
/// flag.
#[derive(Debug)]
pub struct FeatureFlag {
    pub flag: &'static VarDefinition,
    pub feature_desc: &'static str,
}

impl Var for FeatureFlag {
    fn name(&self) -> &'static str {
        self.flag.name()
    }

    fn value(&self) -> String {
        self.flag.value()
    }

    fn description(&self) -> &'static str {
        self.flag.description()
    }

    fn type_name(&self) -> Cow<'static, str> {
        self.flag.type_name()
    }

    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError> {
        self.flag.visible(user, system_vars)
    }
}

impl FeatureFlag {
    pub fn enabled(
        &self,
        system_vars: Option<&SystemVars>,
        feature: Option<String>,
        detail: Option<String>,
    ) -> Result<(), VarError> {
        match system_vars {
            Some(system_vars) if *system_vars.expect_value::<bool>(self.flag) => Ok(()),
            _ => Err(VarError::RequiresFeatureFlag {
                feature: feature.unwrap_or(self.feature_desc.to_string()),
                detail,
                name_hint: system_vars
                    .map(|s| {
                        if s.allow_unsafe {
                            Some(self.flag.name)
                        } else {
                            None
                        }
                    })
                    .flatten(),
            }),
        }
    }
}

impl Var for BuildInfo {
    fn name(&self) -> &'static str {
        MZ_VERSION_NAME.as_str()
    }

    fn value(&self) -> String {
        self.human_version()
    }

    fn description(&self) -> &'static str {
        "Shows the Materialize server version (Materialize)."
    }

    fn type_name(&self) -> Cow<'static, str> {
        String::type_name()
    }

    fn visible(&self, _: &User, _: Option<&SystemVars>) -> Result<(), VarError> {
        Ok(())
    }
}

impl Var for User {
    fn name(&self) -> &'static str {
        IS_SUPERUSER_NAME.as_str()
    }

    fn value(&self) -> String {
        self.is_superuser().format()
    }

    fn description(&self) -> &'static str {
        "Reports whether the current session is a superuser (PostgreSQL)."
    }

    fn type_name(&self) -> Cow<'static, str> {
        bool::type_name()
    }

    fn visible(&self, _: &User, _: Option<&SystemVars>) -> Result<(), VarError> {
        Ok(())
    }
}
