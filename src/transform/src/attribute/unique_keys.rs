// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`UniqueKeys`] attribute.

use mz_expr::MirRelationExpr;
use typemap_rev::{TypeMap, TypeMapKey};

use super::{arity::Arity, subtree_size::SubtreeSize, Attribute, AttributeBuilder};

/// Compute the unique keys of each subtree of a [MirRelationExpr] from the
/// bottom-up.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct UniqueKeys {
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order.
    pub results: Vec<Vec<Vec<usize>>>,
}

impl TypeMapKey for UniqueKeys {
    type Value = UniqueKeys;
}

impl Attribute for UniqueKeys {
    type Value = Vec<Vec<usize>>;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &TypeMap) {
        let n = self.results.len();

        let mut offsets = Vec::new();
        let mut offset = 1;
        for _ in 0..expr.num_inputs() {
            offsets.push(n - offset);
            offset += &deps.get::<SubtreeSize>().unwrap().results[n - offset];
        }

        let subtree_keys = expr.keys_with_input_keys(
            offsets
                .iter()
                .rev()
                .map(|o| deps.get::<Arity>().unwrap().results[*o]),
            offsets.iter().rev().map(|o| &self.results[*o]),
        );

        self.results.push(subtree_keys);
    }

    fn add_dependencies(builder: &mut AttributeBuilder)
    where
        Self: Sized,
    {
        builder.add_attribute::<Arity>();
        builder.add_attribute::<SubtreeSize>();
    }

    fn get_results(&self) -> &Vec<Self::Value> {
        &self.results
    }

    fn get_results_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.results
    }
}
