// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structures for caching cardinality statistics

use mz_repr::GlobalId;

use std::collections::BTreeMap;

/// Estimates of statistics about a given `GlobalId`
#[derive(Clone, Debug)]
pub struct StatisticsOracle(BTreeMap<GlobalId, usize>);

impl StatisticsOracle {
    /// Produces the BTreeMap cache underlying the statistics oracle
    pub fn as_map(&self) -> &BTreeMap<GlobalId, usize> {
        &self.0
    }

    pub fn contains_key(&self, id: &GlobalId) -> bool {
        self.0.contains_key(id)
    }

    pub fn get(&self, id: &GlobalId) -> Option<usize> {
        self.0.get(id).copied()
    }
}

impl Default for StatisticsOracle {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl From<BTreeMap<GlobalId, usize>> for StatisticsOracle {
    fn from(cache: BTreeMap<GlobalId, usize>) -> Self {
        Self(cache)
    }
}
