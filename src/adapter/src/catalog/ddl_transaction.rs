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
use crate::catalog::{Catalog, CatalogReadRecorder, CatalogState, Op, PreparedCatalogBatch};
use crate::coord::ConnMeta;
use crate::util::ResultExt;

/// Speculative catalog state and operations for a SQL DDL transaction.
#[derive(Debug, Clone)]
pub struct DdlTransaction {
    batches: Vec<DdlBatch>,
    state: CatalogState,
    // The durable and in-memory views must describe the same operation prefix,
    // including before the first statement leaves the coordinator for purification.
    snapshot: Snapshot,
}

#[derive(Debug, Clone, Default)]
struct DdlBatch {
    reads: CatalogReadRecorder,
    ops: Vec<Op>,
}

impl DdlTransaction {
    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn ops(&self) -> impl Iterator<Item = &Op> {
        self.batches.iter().flat_map(|batch| &batch.ops)
    }

    /// Records preparation against the current speculative operation prefix.
    pub fn read_recorder(&self) -> CatalogReadRecorder {
        self.batches.last().expect("open DDL batch").reads.clone()
    }

    pub fn check_configuration(&self, catalog: &Catalog) -> Result<(), AdapterError> {
        if self.state.configuration_generation != catalog.state().configuration_generation {
            return Err(AdapterError::DDLTransactionRace);
        }
        Ok(())
    }

    /// Finalizes ordered reads and writes after operation-producing preparation has completed.
    pub(crate) fn take_prepared_batch(&mut self) -> PreparedCatalogBatch {
        PreparedCatalogBatch::ordered(
            std::mem::take(&mut self.batches)
                .into_iter()
                .flat_map(|batch| {
                    std::iter::once(Op::ValidateReads(batch.reads.snapshot())).chain(batch.ops)
                })
                .collect(),
        )
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
        let batch = DdlBatch {
            reads: self.new_read_recorder(),
            ops: Vec::new(),
        };
        DdlTransaction {
            batches: vec![batch],
            state: self.state.clone(),
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
        transaction.check_configuration(self)?;
        let (state, snapshot) = self
            .transact_incremental_dry_run(
                &transaction.state,
                ops.clone(),
                session,
                Some(transaction.snapshot),
                oracle_write_ts,
            )
            .await?;
        transaction.batches.last_mut().expect("open DDL batch").ops = ops;
        // Views of an earlier prefix keep its recorder. They cannot append
        // observations to the prefix produced by these operations.
        transaction.batches.push(DdlBatch::default());
        transaction.state = state;
        transaction.snapshot = snapshot;
        Ok(transaction)
    }
}
