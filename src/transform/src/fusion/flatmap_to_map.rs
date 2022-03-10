// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Turns `FlatMap` into `Flat` if only one row is produced by flatmap.
//!

use crate::TransformArgs;
use mz_expr::{MirRelationExpr, TableFunc};

/// Fuses multiple `Filter` operators into one and deduplicates predicates.
#[derive(Debug)]
pub struct FlatMapToMap;

impl crate::Transform for FlatMapToMap {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post(&mut |e| Ok(self.action(e)))
    }
}

//fn row_count(exp : MirScalarExpr) -> Option<u64> {
//    /// Returns the number of rows produced by this expression
//    match exp {
//
//    }
//}

impl FlatMapToMap {
    /// Fuses multiple `Filter` operators into one and canonicalizes predicates.
    fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let TableFunc::Wrap { types: _, width } = func {
                // Each expression must yield a scalar.
                assert!(exprs.len() % *width == 0);
                if *width == exprs.len() {
                    println!("Performing FlatMap -> Map Rewrite!");
                    *relation = MirRelationExpr::Map {
                        input: input.clone(),
                        scalars: exprs.clone(),
                    };
                    //relation.take_dangerous();
                    //*relation = mapExp;
                }

                //println!("!!! Found FlatMap with wrap! {:#?}   exprs=", relation);
                //println!("## wrap width = {:?}", width);
            }
        }
    }
}
