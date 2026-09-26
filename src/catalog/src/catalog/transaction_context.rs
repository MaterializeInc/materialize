// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Borrowed connection identity for catalog transactions.

use mz_adapter_types::connection::ConnectionId;
use mz_repr::role_id::RoleId;
use mz_sql::session::user::User;
use uuid::Uuid;

/// The invoking connection's identity for auditing and temporary-object ownership.
#[derive(Debug, Clone, Copy)]
pub struct TransactionContext<'a> {
    pub user: &'a User,
    pub conn_id: &'a ConnectionId,
    pub uuid: Uuid,
    /// The role that initiated the connection, which may since have been dropped.
    pub authenticated_role_id: &'a RoleId,
}

impl TransactionContext<'_> {
    pub fn user(&self) -> &User {
        self.user
    }

    pub fn conn_id(&self) -> &ConnectionId {
        self.conn_id
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn authenticated_role_id(&self) -> &RoleId {
        self.authenticated_role_id
    }
}
