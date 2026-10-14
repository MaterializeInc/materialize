// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::durable::Snapshot;
use mz_repr::Timestamp;

use crate::AdapterError;
use crate::catalog::{Catalog, CatalogState, Op};
use crate::coord::ConnMeta;
use crate::util::ResultExt;

/// Speculative catalog state and operations for a SQL DDL transaction.
#[derive(Debug, Clone)]
pub struct DdlTransaction {
    ops: Vec<Op>,
    state: CatalogState,
    revision: u64,
    // The durable and in-memory views must describe the same operation prefix,
    // including before the first statement leaves the coordinator for purification.
    snapshot: Snapshot,
}

impl DdlTransaction {
    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn check_revision(&self, catalog: &Catalog) -> Result<(), AdapterError> {
        if self.revision != catalog.transient_revision() {
            return Err(AdapterError::DDLTransactionRace);
        }
        Ok(())
    }

    pub fn take_ops(&mut self) -> Vec<Op> {
        std::mem::take(&mut self.ops)
    }
}

impl Catalog {
    /// Captures the catalog views used to prepare a DDL transaction.
    pub async fn start_ddl_transaction(&self) -> DdlTransaction {
        let mut storage = self.storage().await;
        let tx = storage
            .transaction()
            .await
            .unwrap_or_terminate("starting DDL transaction");
        DdlTransaction {
            ops: Vec::new(),
            state: self.state.clone(),
            revision: self.transient_revision(),
            snapshot: tx.current_snapshot(),
        }
    }

    /// Evaluates a statement without committing or publishing its effects.
    pub async fn stage_ddl_transaction(
        &self,
        mut transaction: DdlTransaction,
        ops: Vec<Op>,
        session: Option<&ConnMeta>,
        oracle_write_ts: Timestamp,
    ) -> Result<DdlTransaction, AdapterError> {
        transaction.check_revision(self)?;
        let (state, snapshot) = self
            .transact_incremental_dry_run(
                &transaction.state,
                ops.clone(),
                session,
                Some(transaction.snapshot),
                oracle_write_ts,
            )
            .await?;
        transaction.ops.extend(ops);
        transaction.state = state;
        transaction.snapshot = snapshot;
        Ok(transaction)
    }
}
