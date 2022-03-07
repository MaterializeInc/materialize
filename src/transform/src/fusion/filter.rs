// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Filter` operators into one; deduplicates predicates.
//!
//! If the `Filter` operator is empty, removes it.
//!
//! ```rust
//! use mz_expr::{MirRelationExpr, MirScalarExpr};
//! use mz_repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! use mz_transform::fusion::filter::Filter;
//!
//! let input = MirRelationExpr::constant(vec![], RelationType::new(vec![
//!     ScalarType::Bool.nullable(false),
//! ]));
//!
//! let predicate0 = MirScalarExpr::Column(0);
//! let predicate1 = MirScalarExpr::Column(0);
//! let predicate2 = MirScalarExpr::Column(0);
//!
//! let mut expr =
//! input
//!     .clone()
//!     .filter(vec![predicate0.clone()])
//!     .filter(vec![predicate1.clone()])
//!     .filter(vec![predicate2.clone()]);
//!
//! // .transform() will deduplicate any predicates
//! use mz_transform::{Transform, TransformArgs};
//! Filter.transform(&mut expr, TransformArgs {
//!   id_gen: &mut Default::default(),
//!   indexes: &mz_transform::EmptyIndexOracle,
//! });
//!
//! let correct = input.filter(vec![predicate0]);
//!
//! assert_eq!(expr, correct);
//! ```

use crate::TransformArgs;
use mz_expr::MirRelationExpr;

/// Fuses multiple `Filter` operators into one and deduplicates predicates.
#[derive(Debug)]
pub struct Filter;

impl crate::Transform for Filter {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_pre(&mut |e| Ok(self.action(e)))
    }
}

impl Filter {
    /// Fuses multiple `Filter` operators into one and canonicalizes predicates.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Filter { input, predicates } = relation {
            // consolidate nested filters.
            while let MirRelationExpr::Filter {
                input: inner,
                predicates: p2,
            } = &mut **input
            {
                predicates.append(p2);
                *input = Box::new(inner.take_dangerous());
            }

            mz_expr::canonicalize::canonicalize_predicates(predicates, &input.typ());

            // remove the Filter stage if empty.
            if predicates.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}
