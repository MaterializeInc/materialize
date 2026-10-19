// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CatalogReadRecorder, Op};
use crate::AdapterError;

/// Writes and their preparation preconditions, validated together at commit.
///
/// This is not proof that the preconditions still hold. Only catalog replay can
/// establish that. Construction requires a preparation scope, an ordered DDL
/// transaction, or an explicitly untracked synchronization contract.
#[derive(Debug)]
#[must_use = "prepared catalog writes must be committed or explicitly abandoned"]
pub(crate) struct PreparedCatalogBatch {
    ops: Vec<Op>,
}

/// The synchronization contract for writes outside observation-based preparation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum UntrackedCatalogWrite {
    /// Internal work with a synchronization contract independent of statement preparation.
    Internal,
    /// The statement retains its existing serialization and stage-validity contract.
    ExistingStatementSynchronization,
}

impl PreparedCatalogBatch {
    pub(crate) fn untracked(ops: Vec<Op>, _reason: UntrackedCatalogWrite) -> Self {
        Self { ops }
    }

    pub(super) fn ordered(ops: Vec<Op>) -> Self {
        Self { ops }
    }

    pub(crate) fn into_ops(self) -> Vec<Op> {
        self.ops
    }
}

/// The preparation scope owned by an execution context.
#[derive(Debug, Default)]
pub(crate) enum CatalogPreparation {
    #[default]
    Unclassified,
    Observed(CatalogReadRecorder),
    Transaction,
    Untracked(UntrackedCatalogWrite),
    Finished,
}

impl CatalogPreparation {
    pub(crate) fn recorder(&self) -> Option<CatalogReadRecorder> {
        match self {
            Self::Observed(reads) => Some(reads.clone()),
            _ => None,
        }
    }

    pub(crate) fn finish(
        &mut self,
        mut ops: Vec<Op>,
    ) -> Result<PreparedCatalogBatch, AdapterError> {
        match std::mem::replace(self, Self::Finished) {
            Self::Observed(reads) => {
                ops.insert(0, Op::ValidateReads(reads.snapshot()));
                Ok(PreparedCatalogBatch { ops })
            }
            Self::Untracked(reason) => Ok(PreparedCatalogBatch::untracked(ops, reason)),
            Self::Unclassified | Self::Transaction | Self::Finished => Err(AdapterError::Internal(
                "catalog writes require an active statement preparation scope".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn writes_require_a_live_preparation_scope() {
        for mut preparation in [
            CatalogPreparation::Unclassified,
            CatalogPreparation::Transaction,
            CatalogPreparation::Finished,
        ] {
            assert!(preparation.finish(Vec::new()).is_err());
        }
        let mut preparation = CatalogPreparation::Observed(CatalogReadRecorder::default());
        assert!(preparation.finish(Vec::new()).is_ok());
        assert!(preparation.finish(Vec::new()).is_err());
    }
}
