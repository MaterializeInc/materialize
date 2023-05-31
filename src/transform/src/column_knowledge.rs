// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations based on pulling information about individual columns from sources.

use std::collections::BTreeMap;

use itertools::{zip_eq, Itertools};
use mz_expr::visit::Visit;
use mz_expr::JoinImplementation::IndexedFilter;
use mz_expr::{
    func, EvalError, LetRecLimit, MirRelationExpr, MirScalarExpr, UnaryFunc, RECURSION_LIMIT,
};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::{ColumnType, Datum, RelationType, Row, ScalarType};

use crate::{TransformArgs, TransformError};

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct ColumnKnowledge {
    recursion_guard: RecursionGuard,
}

impl Default for ColumnKnowledge {
    fn default() -> ColumnKnowledge {
        ColumnKnowledge {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for ColumnKnowledge {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for ColumnKnowledge {
    /// Transforms an expression through accumulated knowledge.
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "column_knowledge")
    )]
    fn transform(
        &self,
        expr: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        let mut knowledge_stack = Vec::<DatumKnowledge>::new();
        let result = self
            .harvest(expr, &mut BTreeMap::new(), &mut knowledge_stack)
            .map(|_| ());
        mz_repr::explain::trace_plan(&*expr);
        result
    }
}

impl ColumnKnowledge {
    /// Harvest per-column knowledge.
    ///
    /// `knowledge_stack` is a pre-allocated vector but is expected not to contain any elements.
    fn harvest(
        &self,
        expr: &mut MirRelationExpr,
        knowledge: &mut BTreeMap<mz_expr::Id, Vec<DatumKnowledge>>,
        knowledge_stack: &mut Vec<DatumKnowledge>,
    ) -> Result<Vec<DatumKnowledge>, TransformError> {
        self.checked_recur(|_| {
            let result = match expr {
                MirRelationExpr::ArrangeBy { input, .. } => {
                    self.harvest(input, knowledge, knowledge_stack)
                }
                MirRelationExpr::Get { id, typ } => {
                    Ok(knowledge.get(id).cloned().unwrap_or_else(|| {
                        typ.column_types.iter().map(DatumKnowledge::from).collect()
                    }))
                }
                MirRelationExpr::Constant { rows, typ } => {
                    // TODO: handle multi-row cases with some constant columns.
                    if let Ok([(row, _diff)]) = rows.as_deref() {
                        let knowledge = std::iter::zip(row.iter(), typ.column_types.iter())
                            .map(DatumKnowledge::from)
                            .collect();
                        Ok(knowledge)
                    } else {
                        Ok(typ.column_types.iter().map(DatumKnowledge::from).collect())
                    }
                }
                MirRelationExpr::Let { id, value, body } => {
                    let value_knowledge = self.harvest(value, knowledge, knowledge_stack)?;
                    let prior_knowledge =
                        knowledge.insert(mz_expr::Id::Local(id.clone()), value_knowledge);
                    let body_knowledge = self.harvest(body, knowledge, knowledge_stack)?;
                    knowledge.remove(&mz_expr::Id::Local(id.clone()));
                    if let Some(prior_knowledge) = prior_knowledge {
                        knowledge.insert(mz_expr::Id::Local(id.clone()), prior_knowledge);
                    }
                    Ok(body_knowledge)
                }
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                } => {
                    // Set knowledge[i][j] = DatumKnowledge::bottom() for each
                    // column j and CTE i. This corresponds to the normal
                    // evaluation semantics where each recursive CTE is
                    // initialized to the empty collection.
                    for (id, value) in zip_eq(ids.iter(), values.iter()) {
                        let id = mz_expr::Id::Local(id.clone());
                        let knowledge_new = vec![DatumKnowledge::bottom(); value.arity()];
                        let knowledge_old = knowledge.insert(id, knowledge_new);
                        assert!(knowledge_old.is_none());
                    }

                    // Sum up the arity of all ids in the enclosing LetRec node.
                    let let_rec_arity = ids.iter().fold(0, |acc, id| {
                        let id = mz_expr::Id::Local(id.clone());
                        acc + u64::cast_from(knowledge[&id].len())
                    });

                    // Sequentially union knowledge[i][j] with the result of
                    // descending into a clone of values[i]. Repeat until one of
                    // the following conditions is met:
                    //
                    // 1. The knowledge bindings have stabilized at a fixpoint.
                    // 2. No fixpoint was found after `max_iterations`. If this
                    //    is the case reset the knowledge vectors for all
                    //    recursive CTEs to DatumKnowledge::top().
                    // 3. We reach the user-specified recursion limit of any of the bindings.
                    //    In this case, we also give up similarly to 2., because we don't want to
                    //    complicate things with handling different limits per binding.
                    let min_max_iter = LetRecLimit::min_max_iter(limits);
                    let max_iterations = 100;
                    let mut curr_iteration = 0;
                    loop {
                        // Check for conditions (2) and (3).
                        if curr_iteration >= max_iterations
                            || min_max_iter
                                .map(|min_max_iter| curr_iteration >= min_max_iter)
                                .unwrap_or(false)
                        {
                            if curr_iteration > 3 * let_rec_arity {
                                soft_panic_or_log!(
                                    "LetRec loop in ColumnKnowledge has not converged in 3 * |{}|",
                                    let_rec_arity
                                );
                            }

                            for (id, value) in zip_eq(ids.iter(), values.iter()) {
                                let id = mz_expr::Id::Local(id.clone());
                                let knowledge_new = vec![DatumKnowledge::top(); value.arity()];
                                knowledge.insert(id, knowledge_new);
                            }
                            break;
                        }

                        // Check for condition (1).
                        let mut change = false;
                        for (id, mut value) in zip_eq(ids.iter(), values.iter().cloned()) {
                            let id = mz_expr::Id::Local(id.clone());
                            let value = &mut value;
                            let next_knowledge = self.harvest(value, knowledge, knowledge_stack)?;
                            let curr_knowledge = knowledge.get_mut(&id).unwrap();
                            for (curr, next) in zip_eq(curr_knowledge.iter_mut(), next_knowledge) {
                                let prev = curr.clone();
                                curr.join_assign(&next);
                                change |= prev != *curr;
                            }
                        }
                        if !change {
                            break;
                        }

                        curr_iteration += 1;
                    }

                    // Descend into the values with the inferred knowledge.
                    for value in values.iter_mut() {
                        self.harvest(value, knowledge, knowledge_stack)?;
                    }

                    // Descend and return the knowledge from the body.
                    let body_knowledge = self.harvest(body, knowledge, knowledge_stack)?;

                    // Remove shadowed bindings. This is good hygiene, as
                    // otherwise with nested LetRec blocks the `loop { ... }`
                    // above will carry inner LetRec IDs across outer LetRec
                    // iterations. As a consequence, the "no shadowing"
                    // assertion at the beginning of this block will fail at the
                    // inner LetRec for the second outer LetRec iteration.
                    for id in ids.iter() {
                        let id = mz_expr::Id::Local(id.clone());
                        knowledge.remove(&id);
                    }

                    Ok(body_knowledge)
                }
                MirRelationExpr::Project { input, outputs } => {
                    let input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    Ok(outputs
                        .iter()
                        .map(|i| input_knowledge[*i].clone())
                        .collect())
                }
                MirRelationExpr::Map { input, scalars } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let mut column_types = input.typ().column_types;
                    for scalar in scalars.iter_mut() {
                        input_knowledge.push(optimize(
                            scalar,
                            &column_types,
                            &input_knowledge[..],
                            knowledge_stack,
                        )?);
                        column_types.push(scalar.typ(&column_types));
                    }
                    Ok(input_knowledge)
                }
                MirRelationExpr::FlatMap { input, func, exprs } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    for expr in exprs {
                        optimize(
                            expr,
                            &input_typ.column_types,
                            &input_knowledge[..],
                            knowledge_stack,
                        )?;
                    }
                    let func_typ = func.output_type();
                    input_knowledge.extend(func_typ.column_types.iter().map(DatumKnowledge::from));
                    Ok(input_knowledge)
                }
                MirRelationExpr::Filter { input, predicates } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    for predicate in predicates.iter_mut() {
                        optimize(
                            predicate,
                            &input_typ.column_types,
                            &input_knowledge[..],
                            knowledge_stack,
                        )?;
                    }
                    // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                    for predicate in predicates.iter() {
                        // Equality tests allow us to unify the column knowledge of each input.
                        if let MirScalarExpr::CallBinary {
                            func: mz_expr::BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = predicate
                        {
                            // Collect knowledge about the inputs (for columns and literals).
                            let mut knowledge = DatumKnowledge::top();
                            if let MirScalarExpr::Column(c) = &**expr1 {
                                knowledge.meet_assign(&input_knowledge[*c]);
                            }
                            if let MirScalarExpr::Column(c) = &**expr2 {
                                knowledge.meet_assign(&input_knowledge[*c]);
                            }

                            // Absorb literal knowledge about columns.
                            if let MirScalarExpr::Literal(..) = &**expr1 {
                                knowledge.meet_assign(&DatumKnowledge::from(&**expr1));
                            }
                            if let MirScalarExpr::Literal(..) = &**expr2 {
                                knowledge.meet_assign(&DatumKnowledge::from(&**expr2));
                            }

                            // Write back unified knowledge to each column.
                            if let MirScalarExpr::Column(c) = &**expr1 {
                                input_knowledge[*c].meet_assign(&knowledge);
                            }
                            if let MirScalarExpr::Column(c) = &**expr2 {
                                input_knowledge[*c].meet_assign(&knowledge);
                            }
                        }
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::Not(func::Not),
                            expr,
                        } = predicate
                        {
                            if let MirScalarExpr::CallUnary {
                                func: UnaryFunc::IsNull(func::IsNull),
                                expr,
                            } = &**expr
                            {
                                if let MirScalarExpr::Column(c) = &**expr {
                                    input_knowledge[*c].meet_assign(&DatumKnowledge::any(false));
                                }
                            }
                        }
                    }

                    Ok(input_knowledge)
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                    ..
                } => {
                    // Aggregate column knowledge from each input into one `Vec`.
                    let mut knowledges = Vec::new();
                    for input in inputs.iter_mut() {
                        for mut knowledge in self.harvest(input, knowledge, knowledge_stack)? {
                            // Do not propagate error literals beyond join inputs, since that may result
                            // in them being propagated to other inputs of the join and evaluated when
                            // they should not.
                            if let DatumKnowledge::Lit { value: Err(_), .. } = knowledge {
                                knowledge.join_assign(&DatumKnowledge::any(false));
                            }
                            knowledges.push(knowledge);
                        }
                    }

                    // This only aggregates the column types of each input, not the
                    // keys of the inputs. It is unnecessary to aggregate the keys
                    // of the inputs since input keys are unnecessary for reducing
                    // `MirScalarExpr`s.
                    let folded_inputs_typ =
                        inputs.iter().fold(RelationType::empty(), |mut typ, input| {
                            typ.column_types.append(&mut input.typ().column_types);
                            typ
                        });

                    for equivalence in equivalences.iter_mut() {
                        let mut knowledge = DatumKnowledge::top();

                        // We can produce composite knowledge for everything in the equivalence class.
                        for expr in equivalence.iter_mut() {
                            if !matches!(implementation, IndexedFilter(..)) {
                                optimize(
                                    expr,
                                    &folded_inputs_typ.column_types,
                                    &knowledges,
                                    knowledge_stack,
                                )?;
                            }
                            if let MirScalarExpr::Column(c) = expr {
                                knowledge.meet_assign(&knowledges[*c]);
                            }
                            if let MirScalarExpr::Literal(..) = expr {
                                knowledge.meet_assign(&DatumKnowledge::from(&*expr));
                            }
                        }
                        for expr in equivalence.iter_mut() {
                            if let MirScalarExpr::Column(c) = expr {
                                knowledges[*c] = knowledge.clone();
                            }
                        }
                    }

                    Ok(knowledges)
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } => {
                    let input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    let mut output = group_key
                        .iter_mut()
                        .map(|k| {
                            optimize(
                                k,
                                &input_typ.column_types,
                                &input_knowledge[..],
                                knowledge_stack,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    for aggregate in aggregates.iter_mut() {
                        use mz_expr::AggregateFunc;
                        let knowledge = optimize(
                            &mut aggregate.expr,
                            &input_typ.column_types,
                            &input_knowledge[..],
                            knowledge_stack,
                        )?;
                        // This could be improved.
                        let knowledge = match aggregate.func {
                            AggregateFunc::MaxInt16
                            | AggregateFunc::MaxInt32
                            | AggregateFunc::MaxInt64
                            | AggregateFunc::MaxUInt16
                            | AggregateFunc::MaxUInt32
                            | AggregateFunc::MaxUInt64
                            | AggregateFunc::MaxMzTimestamp
                            | AggregateFunc::MaxFloat32
                            | AggregateFunc::MaxFloat64
                            | AggregateFunc::MaxBool
                            | AggregateFunc::MaxString
                            | AggregateFunc::MaxDate
                            | AggregateFunc::MaxTimestamp
                            | AggregateFunc::MaxTimestampTz
                            | AggregateFunc::MinInt16
                            | AggregateFunc::MinInt32
                            | AggregateFunc::MinInt64
                            | AggregateFunc::MinUInt16
                            | AggregateFunc::MinUInt32
                            | AggregateFunc::MinUInt64
                            | AggregateFunc::MinMzTimestamp
                            | AggregateFunc::MinFloat32
                            | AggregateFunc::MinFloat64
                            | AggregateFunc::MinBool
                            | AggregateFunc::MinString
                            | AggregateFunc::MinDate
                            | AggregateFunc::MinTimestamp
                            | AggregateFunc::MinTimestampTz
                            | AggregateFunc::Any
                            | AggregateFunc::All => {
                                // These methods propagate constant values exactly.
                                knowledge
                            }
                            AggregateFunc::Count => DatumKnowledge::any(false),
                            _ => {
                                // The remaining aggregates are non-null if
                                // their inputs are non-null. This is correct
                                // because in Mir~ we reduce an empty collection
                                // to an empty collection (in Hir~ the result
                                // often is singleton null collection).
                                DatumKnowledge::any(knowledge.nullable())
                            }
                        };
                        output.push(knowledge);
                    }
                    Ok(output)
                }
                MirRelationExpr::TopK { input, .. } => {
                    self.harvest(input, knowledge, knowledge_stack)
                }
                MirRelationExpr::Negate { input } => {
                    self.harvest(input, knowledge, knowledge_stack)
                }
                MirRelationExpr::Threshold { input } => {
                    self.harvest(input, knowledge, knowledge_stack)
                }
                MirRelationExpr::Union { base, inputs } => {
                    let mut know = self.harvest(base, knowledge, knowledge_stack)?;
                    for input in inputs {
                        know = know
                            .into_iter()
                            .zip_eq(self.harvest(input, knowledge, knowledge_stack)?)
                            .map(|(mut k1, k2)| {
                                k1.join_assign(&k2);
                                k1
                            })
                            .collect();
                    }
                    Ok(know)
                }
            }?;

            // println!("# Plan");
            // println!("{}", expr.pretty());
            // println!("# Knowledge");
            // print_knowledge_vec(&result);
            // println!("---");

            Ok(result)
        })
    }
}

/// Information about a specific column.
///
/// The values should form a [complete lattice].
///
/// [complete lattice]: https://en.wikipedia.org/wiki/Complete_lattice
#[derive(Clone, Debug, PartialEq, Eq)]
enum DatumKnowledge {
    // Any possible value, optionally known to be NOT NULL.
    Any {
        nullable: bool,
    },
    // A known literal value of a specific type.
    Lit {
        value: Result<mz_repr::Row, EvalError>,
        typ: ScalarType,
    },
    // A value that cannot exist.
    Nothing,
}

impl From<&MirScalarExpr> for DatumKnowledge {
    fn from(expr: &MirScalarExpr) -> Self {
        if let MirScalarExpr::Literal(l, t) = expr {
            let value = l.clone();
            let typ = t.scalar_type.clone();
            Self::Lit { value, typ }
        } else {
            Self::top()
        }
    }
}

impl From<(Datum<'_>, &ColumnType)> for DatumKnowledge {
    fn from((d, t): (Datum<'_>, &ColumnType)) -> Self {
        let value = Ok(Row::pack_slice(&[d.clone()]));
        let typ = t.scalar_type.clone();
        Self::Lit { value, typ }
    }
}

impl From<&ColumnType> for DatumKnowledge {
    fn from(typ: &ColumnType) -> Self {
        let nullable = typ.nullable;
        Self::Any { nullable }
    }
}

impl DatumKnowledge {
    /// The most general possible knowledge (the top of the complete lattice).
    fn top() -> Self {
        Self::Any { nullable: true }
    }

    /// The strictest possible knowledge (the bottom of the complete lattice).
    #[allow(dead_code)]
    fn bottom() -> Self {
        Self::Nothing
    }

    /// Create a [`DatumKnowledge::Any`] instance with the given nullable flag.
    fn any(nullable: bool) -> Self {
        DatumKnowledge::Any { nullable }
    }

    /// Unions (weakens) the possible states of a column.
    fn join_assign(&mut self, other: &Self) {
        use DatumKnowledge::*;

        // Each of the following `if` statements handles the cases marked with
        // `x` of the (self x other) cross product (depicted as a rows x cols
        // table where Self::bottom() is the UL and Self::top() the LR corner).
        // Cases that are already handled are marked with `+` and cases that are
        // yet to be handled marked with `-`.
        //
        // The order of handling (crossing out) of cases ensures that
        // `*.clone()` is not called unless necessary.
        //
        // The final case after the ifs can assert that both sides are Lit
        // variants.

        // x - - - : Nothing
        // x - - - : Lit { _, _ }
        // x - - - : Any { false }
        // x x x x : Any { true }
        if crate::any![
            matches!(self, Any { nullable: true }),
            matches!(other, Nothing),
        ] {
            // Nothing to do.
        }
        // + x x x : Nothing
        // + - - x : Lit { _, _ }
        // + - - x : Any { false }
        // + + + + : Any { true }
        else if crate::any![
            matches!(self, Nothing),
            matches!(other, Any { nullable: true }),
        ] {
            *self = other.clone();
        }
        // + + + + : Nothing
        // + - - + : Lit { _, _ }
        // + x x + : Any { false }
        // + + + + : Any { true }
        else if matches!(self, Any { nullable: false }) {
            if !other.nullable() {
                // Nothing to do.
            } else {
                *self = Self::top() // other: Lit { null, _ }
            }
            // Nothing to do.
        }
        // + + + + : Nothing
        // + - x + : Lit { _, _ }
        // + + + + : Any { false }
        // + + + + : Any { true }
        else if matches!(other, Any { nullable: false }) {
            if !self.nullable() {
                *self = other.clone();
            } else {
                *self = Self::top() // other: Lit { null, _ }
            }
        }
        // + + + + : Nothing
        // + x + + : Lit { _, _ }
        // + + + + : Any { false }
        // + + + + : Any { true }
        else {
            let Lit { value: s_val, typ: s_typ} = self else {
                unreachable!();
            };
            let Lit { value: o_val, typ: o_typ} = other else {
                unreachable!();
            };

            if !s_typ.base_eq(o_typ) {
                ::tracing::error!("Undefined join of non-equal base types {s_typ:?} != {o_typ:?}");
                *self = Self::top();
            } else if s_val != o_val {
                let nullable = self.nullable() || other.nullable();
                *self = Any { nullable }
            } else if s_typ != o_typ {
                // Same value but different concrete types - strip all modifiers!
                // This is identical to what ColumnType::union is doing.
                *s_typ = s_typ.without_modifiers();
            } else {
                // Value and type coincide - do nothing!
            }
        }
    }

    /// Intersects (strengthens) the possible states of a column.
    fn meet_assign(&mut self, other: &Self) {
        use DatumKnowledge::*;

        // Each of the following `if` statements handles the cases marked with
        // `x` of the (self x other) cross product (depicted as a rows x cols
        // table where Self::bottom() is the UL and Self::top() the LR corner).
        // Cases that are already handled are marked with `+` and cases that are
        // yet to be handled marked with `-`.
        //
        // The order of handling (crossing out) of cases ensures that
        // `*.clone()` is not called unless necessary.
        //
        // The final case after the ifs can assert that both sides are Lit
        // variants.

        // x x x x : Nothing
        // - - - x : Lit { _, _ }
        // - - - x : Any { false }
        // - - - x : Any { true }
        if crate::any![
            matches!(self, Nothing),
            matches!(other, Any { nullable: true }),
        ] {
            // Nothing to do.
        }
        // + + + + : Nothing
        // x - - + : Lit { _, _ }
        // x - - + : Any { false }
        // x x x + : Any { true }
        else if crate::any![
            matches!(self, Any { nullable: true }),
            matches!(other, Nothing),
        ] {
            *self = other.clone();
        }
        // + + + + : Nothing
        // + - - + : Lit { _, _ }
        // + x x + : Any { false }
        // + + + + : Any { true }
        else if matches!(self, Any { nullable: false }) {
            match other {
                Any { .. } => {
                    // Nothing to do.
                }
                Lit { .. } => {
                    if other.nullable() {
                        *self = Nothing; // other: Lit { null, _ }
                    } else {
                        *self = other.clone();
                    }
                }
                Nothing => unreachable!(),
            }
        }
        // + + + + : Nothing
        // + - x + : Lit { _, _ }
        // + + + + : Any { false }
        // + + + + : Any { true }
        else if matches!(other, Any { nullable: false }) {
            if self.nullable() {
                *self = Nothing // self: Lit { null, _ }
            }
        }
        // + + + + : Nothing
        // + x + + : Lit { _, _ }
        // + + + + : Any { false }
        // + + + + : Any { true }
        else {
            let Lit { value: s_val, typ: s_typ} = self else {
                unreachable!();
            };
            let Lit { value: o_val, typ: o_typ} = other else {
                unreachable!();
            };

            if !s_typ.base_eq(o_typ) {
                soft_panic_or_log!("Undefined meet of non-equal base types {s_typ:?} != {o_typ:?}");
                *self = Self::top(); // this really should be Nothing
            } else if s_val != o_val {
                *self = Nothing;
            } else if s_typ != o_typ {
                // Same value but different concrete types - strip all
                // modifiers! We should probably pick the more specific of the
                // two types if they are ordered or return Nothing otherwise.
                *s_typ = s_typ.without_modifiers();
            } else {
                // Value and type coincide - do nothing!
            }
        }
    }

    fn nullable(&self) -> bool {
        match self {
            DatumKnowledge::Any { nullable } => *nullable,
            DatumKnowledge::Lit { value, .. } => match value {
                Ok(value) => value.iter().next().unwrap().is_null(),
                Err(_) => false,
            },
            DatumKnowledge::Nothing => false,
        }
    }
}

/// Attempts to optimize
///
/// `knowledge_stack` is a pre-allocated vector but is expected not to contain any elements.
fn optimize(
    expr: &mut MirScalarExpr,
    column_types: &[ColumnType],
    column_knowledge: &[DatumKnowledge],
    knowledge_stack: &mut Vec<DatumKnowledge>,
) -> Result<DatumKnowledge, TransformError> {
    // Storage for `DatumKnowledge` being propagated up through the
    // `MirScalarExpr`. When a node is visited, pop off as many `DatumKnowledge`
    // as the number of children the node has, and then push the
    // `DatumKnowledge` corresponding to the node back onto the stack.
    // Post-order traversal means that if a node has `n` children, the top `n`
    // `DatumKnowledge` in the stack are the `DatumKnowledge` corresponding to
    // the children.
    assert!(knowledge_stack.is_empty());
    #[allow(deprecated)]
    expr.visit_mut_pre_post(
        &mut |e| {
            // We should not eagerly memoize `if` branches that might not be taken.
            // TODO: Memoize expressions in the intersection of `then` and `els`.
            if let MirScalarExpr::If { then, els, .. } = e {
                Some(vec![then, els])
            } else {
                None
            }
        },
        &mut |e| {
            let result = match e {
                MirScalarExpr::Column(index) => {
                    let index = *index;
                    if let DatumKnowledge::Lit { value, typ } = &column_knowledge[index] {
                        let nullable = column_knowledge[index].nullable();
                        *e = MirScalarExpr::Literal(value.clone(), typ.clone().nullable(nullable));
                    }
                    column_knowledge[index].clone()
                }
                MirScalarExpr::Literal(_, _) | MirScalarExpr::CallUnmaterializable(_) => {
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::CallUnary { func, expr: _ } => {
                    let knowledge = knowledge_stack.pop().unwrap();
                    if matches!(&knowledge, DatumKnowledge::Lit { .. }) {
                        e.reduce(column_types);
                    } else if func == &UnaryFunc::IsNull(func::IsNull) && !knowledge.nullable() {
                        *e = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
                    };
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::CallBinary {
                    func: _,
                    expr1: _,
                    expr2: _,
                } => {
                    let knowledge2 = knowledge_stack.pop().unwrap();
                    let knowledge1 = knowledge_stack.pop().unwrap();
                    if crate::any![
                        matches!(knowledge1, DatumKnowledge::Lit { .. }),
                        matches!(knowledge2, DatumKnowledge::Lit { .. }),
                    ] {
                        e.reduce(column_types);
                    }
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::CallVariadic { func: _, exprs } => {
                    // Drain the last `exprs.len()` knowledge, and reduce if any is `Lit`.
                    assert!(knowledge_stack.len() >= exprs.len());
                    if knowledge_stack
                        .drain(knowledge_stack.len() - exprs.len()..)
                        .any(|k| matches!(k, DatumKnowledge::Lit { .. }))
                    {
                        e.reduce(column_types);
                    }
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::If {
                    cond: _,
                    then: _,
                    els: _,
                } => {
                    // `cond` has been left un-optimized, as we should not remove the conditional
                    // nature of the evaluation based on column knowledge: the resulting
                    // expression could then move down past a filter or join that provided
                    // the guarantees, and would become wrong.
                    //
                    // Instead, each of the branches have been optimized, and we
                    // can union the states of their columns.
                    let know2 = knowledge_stack.pop().unwrap();
                    let mut know1 = knowledge_stack.pop().unwrap();
                    know1.join_assign(&know2);
                    know1
                }
            };
            knowledge_stack.push(result);
        },
    )?;
    let knowledge_datum = knowledge_stack.pop();
    assert!(knowledge_stack.is_empty());
    knowledge_datum.ok_or_else(|| {
        TransformError::Internal(String::from("unexpectedly empty stack in optimize"))
    })
}

#[allow(dead_code)] // keep debugging method around
fn print_knowledge_map<'a>(
    knowledge: &BTreeMap<mz_expr::Id, Vec<DatumKnowledge>>,
    ids: impl Iterator<Item = &'a mz_expr::LocalId>,
) {
    for id in ids {
        let id = mz_expr::Id::Local(id.clone());
        for (i, k) in knowledge.get(&id).unwrap().iter().enumerate() {
            println!("{id}.#{i}: {k:?}");
        }
    }
    println!("");
}

#[allow(dead_code)] // keep debugging method around
fn print_knowledge_vec(knowledge: &Vec<DatumKnowledge>) {
    for (i, k) in knowledge.iter().enumerate() {
        println!("#{i}: {k:?}");
    }
    println!("");
}
