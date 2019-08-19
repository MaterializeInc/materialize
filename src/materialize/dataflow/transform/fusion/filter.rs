// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Re-order relations in a join to process them in an order that makes sense.
//!
//! ```rust
//! use materialize::dataflow::{RelationExpr, ScalarExpr};
//! use materialize::dataflow::transform::fusion::filter::Filter;
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
//! let typ = RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]);
//!
//! Filter.transform(&mut expr, &typ);
//!
//! let correct = input.filter(vec![predicate0, predicate1, predicate2]);
//!
//! assert_eq!(expr, correct);
//! ```

use expr::RelationExpr;
use repr::RelationType;

pub struct Filter;

impl crate::dataflow::transform::Transform for Filter {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl Filter {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }
    pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        if let RelationExpr::Filter { input, predicates } = relation {
            // consolidate nested filters.
            while let RelationExpr::Filter {
                input: inner,
                predicates: p2,
            } = &mut **input
            {
                predicates.extend(p2.drain(..));
                let empty = Box::new(RelationExpr::Constant {
                    rows: vec![],
                    typ: metadata.to_owned(),
                });
                *input = std::mem::replace(inner, empty);
            }

            // remove the Filter stage if empty.
            if predicates.is_empty() {
                let empty = RelationExpr::Constant {
                    rows: vec![],
                    typ: metadata.to_owned(),
                };
                *relation = std::mem::replace(input, empty);
            }
        }
    }
}
