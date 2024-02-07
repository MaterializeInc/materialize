// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations around supporting the COPY TO protocol with the dataflow layer

use std::collections::BTreeSet;

use mz_adapter_types::connection::ConnectionId;
use mz_controller_types::ClusterId;
use mz_repr::GlobalId;
use mz_sql::session::user::User;

use crate::{AdapterError, ExecuteContext, ExecuteResponse};

/// A description of an active copy to from coord's perspective.
#[derive(Debug)]
pub struct ActiveCopyTo {
    /// The user of the session that created the subscribe.
    pub user: User,
    /// The connection id of the session that created the subscribe.
    pub conn_id: ConnectionId,
    /// The context about the COPY TO statement getting executed.
    /// Used to eventually call `ctx.retire` on.
    pub ctx: Option<ExecuteContext>,
    /// The cluster that the copy to is running on.
    pub cluster_id: ClusterId,
    /// All `GlobalId`s that the copy to's expression depends on.
    pub depends_on: BTreeSet<GlobalId>,
}

impl ActiveCopyTo {
    pub(crate) fn process_response(&mut self, response: Result<ExecuteResponse, AdapterError>) {
        // Using an option to get an owned value after take
        // to call `retire` on it.
        if let Some(ctx) = self.ctx.take() {
            let _ = ctx.retire(response);
        }
    }
}
