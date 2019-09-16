// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Common query transformation helper functions.

use crate::RelationExpr;
use std::borrow::Borrow;
use std::ops::Range;

/// Converts between column indexes and relation indexes when an expression's
/// output is the product of several inputs (e.g., a join).
#[derive(Debug)]
pub struct IndexTracker {
    arities: Vec<usize>,
    relation: Vec<usize>,
    offsets: Vec<usize>,
}

impl IndexTracker {
    /// Constructs a new [`IndexTracker`] from an iterator over input arities.
    pub fn new(arities: impl IntoIterator<Item = usize>) -> IndexTracker {
        let arities: Vec<_> = arities.into_iter().collect();

        let mut offset = 0;
        let mut offsets = Vec::new();
        for input in 0..arities.len() {
            offsets.push(offset);
            offset += arities[input];
        }

        let relation = arities
            .iter()
            .enumerate()
            .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
            .collect();

        IndexTracker {
            arities,
            relation,
            offsets,
        }
    }

    pub fn for_inputs<I>(inputs: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<RelationExpr>,
    {
        Self::new(inputs.into_iter().map(|input| input.borrow().arity()))
    }

    /// Returns the index of the relation that provides `column`.
    pub fn relation_of(&self, column: usize) -> usize {
        self.relation[column]
    }

    /// Makes the column index `column` local (i.e., relative to the start of
    /// its table), rather than global (i.e., relative to the start of the
    /// join).
    pub fn local_of(&self, column: usize) -> usize {
        column - self.offsets[self.relation_of(column)]
    }

    /// Computes the global index for a column index `column` that is relative
    /// to `relation`.
    pub fn global_of(&self, relation: usize, column: usize) -> usize {
        self.offsets[relation] + column
    }

    /// Returns the arity of the `relation`.
    pub fn arity_of(&self, relation: usize) -> usize {
        self.arities[relation]
    }

    /// Returns the range of column indices that belong to `relation`.
    pub fn range_of(&self, relation: usize) -> Range<usize> {
        self.offsets[relation]..self.arity_of(relation)
    }
}
