// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Re-order relations in a join to process them in an order that makes sense.
//!
//! ```rust
//! use expr::{RelationExpr, ScalarExpr};
//! use expr::transform::fusion::filter::Filter;
//! use repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! let input = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]));
//!
//! let predicate0 = ScalarExpr::Column(0);
//! let predicate1 = ScalarExpr::Column(0);
//! let predicate2 = ScalarExpr::Column(0);
//!
//! let mut expr =
//! input
//!     .clone()
//!     .filter(vec![predicate0.clone()])
//!     .filter(vec![predicate1.clone()])
//!     .filter(vec![predicate2.clone()]);
//!
//! // .transform() will deduplicate any predicates
//! Filter.transform(&mut expr);
//!
//! let correct = input.filter(vec![predicate0]);
//!
//! assert_eq!(expr, correct);
//! ```

use crate::RelationExpr;

#[derive(Debug)]
pub struct Filter;

impl crate::transform::Transform for Filter {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl Filter {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Filter { input, predicates } = relation {
            // consolidate nested filters.
            while let RelationExpr::Filter {
                input: inner,
                predicates: p2,
            } = &mut **input
            {
                predicates.extend(p2.drain(..));
                *input = Box::new(inner.take_dangerous());
            }

            predicates.sort();
            predicates.dedup();

            // remove the Filter stage if empty.
            if predicates.is_empty() {
                *relation = input.take_dangerous();
            }
        }
    }
}
