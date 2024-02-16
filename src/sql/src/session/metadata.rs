// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_adapter_types::connection::ConnectionId;
use mz_repr::role_id::RoleId;
use mz_sql_parser::ast::Ident;

use crate::plan::PlanContext;
use crate::session::user::{RoleMetadata, User};

pub trait SessionMetadata: Debug + Sync {
    /// Returns the user who owns this session.
    fn user(&self) -> &User;
    /// Returns the database session variable.
    fn database(&self) -> &str;
    /// Returns the search path session variable.
    fn search_path(&self) -> &[Ident];
    /// Returns the connection ID associated with the session.
    fn conn_id(&self) -> &ConnectionId;
    /// Returns the current transaction's PlanContext. Panics if there is not a
    /// current transaction.
    fn pcx(&self) -> &PlanContext;
    /// Returns the session's current role ID.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    fn current_role_id(&self) -> &RoleId;
    /// Returns the session's session role ID.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    fn session_role_id(&self) -> &RoleId;
    /// Whether the current session is a superuser.
    fn is_superuser(&self) -> bool;
    fn enable_session_rbac_checks(&self) -> bool;
    /// Returns the session's role metadata.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    fn role_metadata(&self) -> &RoleMetadata;
}
