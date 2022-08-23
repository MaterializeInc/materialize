// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`SubtreeSize`] attribute.

use mz_expr::MirRelationExpr;

use super::Attribute;

/// Compute the number of MirRelationExpr in each subtree in a bottom-up manner.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct SubtreeSize {
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order.
    pub results: Vec<usize>,
}

impl Attribute for SubtreeSize {
    type Value = usize;
    type Dependencies = ();

    fn derive(&mut self, expr: &MirRelationExpr, _deps: &()) {
        use MirRelationExpr::*;
        let n = self.results.len();
        match expr {
            Constant { .. } => {
                self.results.push(1);
            }
            Get { .. } => {
                self.results.push(1);
            }
            Let {
                value: _, body: _, ..
            } => {
                let body = self.results[n - 1];
                let value = self.results[n - 1 - body];
                self.results.push(body + value + 1);
            }
            Project { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Map { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            FlatMap { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Filter { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Join { inputs, .. } => {
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    offset += &self.results[n - offset];
                }
                self.results.push(offset);
            }
            Reduce { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            TopK { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Negate { input: _ } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Threshold { input: _ } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
            Union { base: _, inputs } => {
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    offset += &self.results[n - offset];
                }
                offset += &self.results[n - offset]; // add base size
                self.results.push(offset);
            }
            ArrangeBy { input: _, .. } => {
                let input = self.results[n - 1];
                self.results.push(input + 1);
            }
        }
    }
}
