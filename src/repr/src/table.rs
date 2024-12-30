// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_persist_client::batch::ProtoBatch;
use smallvec::SmallVec;

use crate::{Diff, Row};

/// Data that will be "blindly" appended to a table at the current upper.
#[derive(Debug, Clone, PartialEq)]
pub enum TableData {
    /// Rows that still need to be persisted and appended.
    ///
    /// The contained [`Row`]s are _not_ consolidated.
    Rows(Vec<(Row, Diff)>),
    /// Batches already staged in Persist ready to be appended.
    Batches(SmallVec<[ProtoBatch; 1]>),
}

impl TableData {
    pub fn is_empty(&self) -> bool {
        match self {
            TableData::Rows(rows) => rows.is_empty(),
            TableData::Batches(batches) => batches.is_empty(),
        }
    }
}
