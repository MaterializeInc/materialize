// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::net::IpAddr;

use mz_adapter_types::connection::ConnectionId;
use mz_repr::role_id::RoleId;

use crate::plan::PlanContext;
use crate::session::user::{RoleMetadata, User};
use crate::session::vars::SessionVars;

pub trait SessionMetadata: Debug + Sync {
    /// Returns the session vars for this session.
    fn vars(&self) -> &SessionVars;
    /// Returns the connection ID associated with the session.
    fn conn_id(&self) -> &ConnectionId;
    /// Returns the client address associated with the session.
    fn client_ip(&self) -> Option<&IpAddr>;
    /// Returns the current transaction's PlanContext. Panics if there is not a
    /// current transaction.
    fn pcx(&self) -> &PlanContext;
    /// Returns the role metadata for this session.
    fn role_metadata(&self) -> &RoleMetadata;

    /// Returns the session's current role ID.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    fn current_role_id(&self) -> &RoleId {
        &self.role_metadata().current_role
    }

    /// Returns the session's session role ID.
    ///
    /// # Panics
    /// If the session has not connected successfully.
    fn session_role_id(&self) -> &RoleId {
        &self.role_metadata().session_role
    }

    fn user(&self) -> &User {
        self.vars().user()
    }

    fn database(&self) -> &str {
        self.vars().database()
    }

    fn search_path(&self) -> &[mz_sql_parser::ast::Ident] {
        self.vars().search_path()
    }

    fn is_superuser(&self) -> bool {
        self.vars().is_superuser()
    }

    fn enable_session_rbac_checks(&self) -> bool {
        self.vars().enable_session_rbac_checks()
    }
}
