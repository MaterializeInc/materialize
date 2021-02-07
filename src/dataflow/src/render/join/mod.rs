// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of `MirRelationExpr::Join` operators, and supporting types.
//!
//! Join planning proceeds by repeatedly introducing collections that
//! extend the set of available output columns. The expected location
//! of each output column is determined by the order of inputs to the
//! join operator: columns are appended in that order.
//!
//! While planning the join, we also have access to logic in the form
//! of expressions, predicates, and projections that we intended to
//! apply to the output of the join. This logic uses "output column
//! reckoning" where columns are identified by their intended output
//! position.
//!
//! As we consider applying expressions to partial results, we will
//! place the results in column locations *after* the intended output
//! column locations. These output locations in addition to the new
//! distinct identifiers for constructed expressions is "extended
//! output column reckoning", as is what we use when reasoning about
//! work still available to be done on the partial join results.

mod delta_join;
mod linear_join;

use std::collections::HashMap;

use expr::{MapFilterProject, MirScalarExpr};
use repr::{Datum, Row, RowArena, RowPacker};

/// A manual closure implementation of filtering and logic application.
///
/// This manual implementation exists to express lifetime constraints clearly,
/// as there is a relationship between the borrowed lifetime of the closed-over
/// state and the arguments it takes when invoked. It was not clear how to do
/// this with a Rust closure (glorious battle was waged, but ultimately lost).
struct JoinClosure {
    ready_equivalences: Vec<Vec<MirScalarExpr>>,
    before: MapFilterProject,
}

impl JoinClosure {
    /// Applies per-row filtering and logic.
    #[inline(always)]
    fn apply<'a>(
        &'a self,
        datums: &mut Vec<Datum<'a>>,
        temp_storage: &'a RowArena,
        row_packer: &mut RowPacker,
    ) -> Result<Option<Row>, expr::EvalError> {
        for exprs in self.ready_equivalences.iter() {
            // Each list of expressions should be equal to the same value.
            let val = exprs[0].eval(&datums[..], &temp_storage)?;
            for expr in exprs[1..].iter() {
                if expr.eval(&datums, &temp_storage)? != val {
                    return Ok(None);
                }
            }
        }
        self.before.evaluate(datums, &temp_storage, row_packer)
    }

    /// Construct an instance of the closure from available columns.
    ///
    /// This method updates the available columns, equivalences, and
    /// the `MapFilterProject` instance. The columns are updated to
    /// include reference to any columns added by the application of
    /// this logic, which might result from partial application of
    /// the `MapFilterProject` instance.
    ///
    /// If all columns are available for `mfp`, this method works
    /// extra hard to ensure that the closure contains all the work,
    /// and `mfp` is left as an identity transform (which can then
    /// be ignored).
    fn build(
        columns: &mut HashMap<usize, usize>,
        equivalences: &mut Vec<Vec<MirScalarExpr>>,
        mfp: &mut MapFilterProject,
    ) -> Self {
        // First, determine which columns should be compared due to `equivalences`.
        let mut ready_equivalences = Vec::new();
        for equivalence in equivalences.iter_mut() {
            if let Some(pos) = equivalence
                .iter()
                .position(|e| e.support().into_iter().all(|c| columns.contains_key(&c)))
            {
                let mut should_equate = Vec::new();
                let mut cursor = pos + 1;
                while cursor < equivalence.len() {
                    if equivalence[cursor]
                        .support()
                        .into_iter()
                        .all(|c| columns.contains_key(&c))
                    {
                        // Remove expression and equate with the first bound expression.
                        should_equate.push(equivalence.remove(cursor));
                    } else {
                        cursor += 1;
                    }
                }
                if !should_equate.is_empty() {
                    should_equate.push(equivalence[pos].clone());
                    ready_equivalences.push(should_equate);
                }
            }
        }
        equivalences.retain(|e| e.len() > 1);

        // Update ready_equivalences to reference correct column locations.
        for exprs in ready_equivalences.iter_mut() {
            for expr in exprs.iter_mut() {
                expr.permute_map(&columns);
            }
        }

        // Next, partition `mfp` into `before` and `after`, the former of which can be
        // applied now.
        let (mut before, after) = std::mem::replace(mfp, MapFilterProject::new(mfp.input_arity))
            .partition(columns, columns.len());

        // Add any newly created columns to `columns`. These columns may be referenced
        // by `after`, and it will be important to track their locations.
        let bonus_columns = before.projection.len() - before.input_arity;
        for bonus_column in 0..bonus_columns {
            columns.insert(mfp.input_arity + bonus_column, columns.len());
        }

        *mfp = after;

        // Before constructing and returning the result, we can remove output columns of `before`
        // that are not needed in further `equivalences` or by `after` (now `mfp`).
        let mut demand = Vec::new();
        demand.extend(mfp.demand());
        for equivalence in equivalences.iter() {
            for expr in equivalence.iter() {
                demand.extend(expr.support());
            }
        }
        demand.sort();
        demand.dedup();
        // We only want to remove columns that are presented as outputs (i.e. can be found as in
        // `columns`). Other columns have yet to be introduced, and we shouldn't have any opinion
        // about them yet.
        demand.retain(|column| columns.contains_key(column));
        // Project `before` output columns using current locations of demanded columns.
        before = before.project(demand.iter().map(|column| columns[column]));
        // Update `columns` to reflect location of retained columns.
        columns.clear();
        for (index, column) in demand.iter().enumerate() {
            columns.insert(*column, index);
        }

        // If `mfp` is a permutation of the columns present in `columns`, then we can
        // apply that permutation to `before` and `columns`, so that `mfp` becomes the
        // identity operation.
        if mfp.expressions.is_empty()
            && mfp.predicates.is_empty()
            && mfp.projection.len() == columns.len()
            && mfp.projection.iter().all(|col| columns.contains_key(col))
            && columns.keys().all(|col| mfp.projection.contains(col))
        {
            // The projection we want to apply to `before`  comes to us from `mfp` in the
            // extended output column reckoning.
            let projection = mfp
                .projection
                .iter()
                .map(|col| columns[col])
                .collect::<Vec<_>>();
            before = before.project(projection);
            // Update the physical locations of each output column.
            columns.clear();
            for (index, column) in mfp.projection.iter().enumerate() {
                columns.insert(*column, index);
            }
        }

        // `before` should not be modified after this point.
        before.optimize();

        // Cons up an instance of the closure with the closed-over state.
        Self {
            ready_equivalences,
            before,
        }
    }

    /// True iff the closure neither filters nor transforms recorcds.
    fn is_identity(&self) -> bool {
        self.ready_equivalences.is_empty() && self.before.is_identity()
    }
}

/// Maintained state as we construct join dataflows.
///
/// This state primarily tracks the *remaining* work that has not yet been applied to a
/// stream of partial results.
///
/// This state is meant to reconcile the logical operations that remain to apply (e.g.
/// filtering, expressions, projection) and the physical organization of the current stream
/// of data, which columns may be partially assembled in non-standard locations and which
/// may already have been partially subjected to logic we need to apply.
struct JoinBuildState {
    /// Map from expected locations in extended output column reckoning to physical locations.
    column_map: HashMap<usize, usize>,
    /// A list of equivalence classes of expressions.
    ///
    /// Within each equivalence class, expressions must evaluate to the same result to pass
    /// the join expression. Importantly, "the same" should be evaluated with `Datum`s Rust
    /// equality, rather than the equality presented by the `BinaryFunc` equality operator.
    /// The distinction is important for null handling, at the least.
    equivalences: Vec<Vec<MirScalarExpr>>,
    /// The linear operator logic (maps, filters, and projection) that remains to be applied
    /// to the output of the join.
    ///
    /// We we advance through the construction of the join dataflow, we may be able to peel
    /// off some of this work, ideally reducing `mfp` to something nearly the identity.
    mfp: MapFilterProject,
}

impl JoinBuildState {
    /// Create a new join state and initial closure from initial values.
    ///
    /// The initial closure can be `None` which indicates that it is the identity operator.
    fn new(
        columns: std::ops::Range<usize>,
        equivalences: &[Vec<MirScalarExpr>],
        mfp: &MapFilterProject,
    ) -> Self {
        let mut column_map = HashMap::new();
        for column in columns {
            column_map.insert(column, column_map.len());
        }
        let mut equivalences = equivalences.to_vec();
        for equivalence in equivalences.iter_mut() {
            equivalence.sort();
            equivalence.dedup();
        }
        equivalences.sort();
        Self {
            column_map,
            equivalences,
            mfp: mfp.clone(),
        }
    }

    /// Present new columns and extract any newly available closure.
    fn add_columns(
        &mut self,
        new_columns: std::ops::Range<usize>,
        bound_expressions: &[MirScalarExpr],
    ) -> JoinClosure {
        // Remove each element of `bound_expressions` from `equivalences`, so that we
        // avoid redundant predicate work. This removal also paves the way for
        // more precise "demand" information going forward.
        for equivalence in self.equivalences.iter_mut() {
            equivalence.retain(|expr| !bound_expressions.contains(expr));
        }
        self.equivalences.retain(|e| e.len() > 1);

        // Update our map of the sources of each column in the update stream.
        for column in new_columns {
            self.column_map.insert(column, self.column_map.len());
        }

        self.extract_closure()
    }

    /// Extract a final `MapFilterProject` once all columns are available.
    ///
    /// If not all columns are available this method will likely panic.
    /// This method differs from `extract_closure` in that it forcibly
    /// completes the join, extracting projections and expressions that
    /// may not be extracted with `extract_closure` (for example, literals,
    /// permutations, and repetition of output columns).
    ///
    /// The resulting closure may be the identity operator, which can be
    /// checked with the `is_identity()` method.
    fn complete(self) -> JoinClosure {
        let Self {
            column_map,
            mut equivalences,
            mut mfp,
        } = self;

        for equivalence in equivalences.iter_mut() {
            for expr in equivalence.iter_mut() {
                expr.permute_map(&column_map);
            }
        }
        mfp.permute(&column_map, column_map.len());
        mfp.optimize();

        JoinClosure {
            ready_equivalences: equivalences,
            before: mfp,
        }
    }

    /// A method on `self` that extracts an available closure.
    ///
    /// The extracted closure is not guaranteed to be non-trivial. Sensitive users should
    /// consider using the `.is_identity()` method to determine non-triviality.
    fn extract_closure(&mut self) -> JoinClosure {
        JoinClosure::build(&mut self.column_map, &mut self.equivalences, &mut self.mfp)
    }
}
