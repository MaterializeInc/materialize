// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`RelationType`] attribute.

use mz_expr::MirRelationExpr;
use mz_repr::ColumnType;

use super::subtree_size::SubtreeSize;
use super::Attribute;

/// Compute the column types of each subtree of a [MirRelationExpr] from the
/// bottom-up.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct RelationType {
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order.
    pub results: Vec<Vec<ColumnType>>,
}

impl Attribute for RelationType {
    type Value = Vec<ColumnType>;
    type Dependencies = SubtreeSize;

    fn derive(&mut self, expr: &MirRelationExpr, subtree_size: &SubtreeSize) {
        let n = self.results.len();
        let mut offsets = Vec::new();
        let mut offset = 1;
        for _ in 0..expr.num_inputs() {
            offsets.push(n - offset);
            offset += &subtree_size.results[n - offset];
        }
        let subtree_column_types =
            expr.col_with_input_cols(offsets.into_iter().rev().map(|o| &self.results[o]));
        self.results.push(subtree_column_types);
    }
}
