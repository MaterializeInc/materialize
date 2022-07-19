// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations based on pulling information about individual columns from sources.

use std::collections::HashMap;

use itertools::Itertools;

use mz_expr::visit::Visit;
use mz_expr::{func, EvalError, MirRelationExpr, MirScalarExpr, UnaryFunc, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::{ColumnType, RelationType, ScalarType};
use mz_repr::{Datum, Row};

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
    fn transform(
        &self,
        expr: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        let mut knowledge_stack = Vec::<DatumKnowledge>::new();
        self.harvest(expr, &mut HashMap::new(), &mut knowledge_stack)
            .map(|_| ())
    }
}

impl ColumnKnowledge {
    /// Harvest per-column knowledge.
    ///
    /// `knowledge_stack` is a pre-allocated vector but is expected not to contain any elements.
    fn harvest(
        &self,
        expr: &mut MirRelationExpr,
        knowledge: &mut HashMap<mz_expr::Id, Vec<DatumKnowledge>>,
        knowledge_stack: &mut Vec<DatumKnowledge>,
    ) -> Result<Vec<DatumKnowledge>, TransformError> {
        self.checked_recur(|_| {
            match expr {
                MirRelationExpr::ArrangeBy { input, .. } => {
                    self.harvest(input, knowledge, knowledge_stack)
                }
                MirRelationExpr::Get { id, typ } => {
                    Ok(knowledge.get(id).cloned().unwrap_or_else(|| {
                        typ.column_types.iter().map(DatumKnowledge::from).collect()
                    }))
                }
                MirRelationExpr::Constant { rows, typ } => {
                    if let Ok([(row, _diff)]) = rows.as_deref() {
                        let knowledge = row
                            .iter()
                            .zip(typ.column_types.iter())
                            .map(|(datum, typ)| DatumKnowledge {
                                value: Some((Ok(Row::pack_slice(&[datum.clone()])), typ.clone())),
                                nullable: datum == Datum::Null,
                            })
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
                MirRelationExpr::Project { input, outputs } => {
                    let input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    Ok(outputs
                        .iter()
                        .map(|i| input_knowledge[*i].clone())
                        .collect())
                }
                MirRelationExpr::Map { input, scalars } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    for scalar in scalars.iter_mut() {
                        let know =
                            optimize(scalar, &input_typ, &input_knowledge[..], knowledge_stack)?;
                        input_knowledge.push(know);
                    }
                    Ok(input_knowledge)
                }
                MirRelationExpr::FlatMap { input, func, exprs } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    for expr in exprs {
                        optimize(expr, &input_typ, &input_knowledge[..], knowledge_stack)?;
                    }
                    let func_typ = func.output_type();
                    input_knowledge.extend(func_typ.column_types.iter().map(DatumKnowledge::from));
                    Ok(input_knowledge)
                }
                MirRelationExpr::Filter { input, predicates } => {
                    let mut input_knowledge = self.harvest(input, knowledge, knowledge_stack)?;
                    let input_typ = input.typ();
                    for predicate in predicates.iter_mut() {
                        optimize(predicate, &input_typ, &input_knowledge[..], knowledge_stack)?;
                    }
                    // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                    for predicate in predicates.iter() {
                        // Equality tests allow us to unify the column knowledge of each input.
                        if let MirScalarExpr::CallBinary { func, expr1, expr2 } = predicate {
                            if func == &mz_expr::BinaryFunc::Eq {
                                // Collect knowledge about the inputs (for columns and literals).
                                let mut knowledge = DatumKnowledge::default();
                                if let MirScalarExpr::Column(c) = &**expr1 {
                                    knowledge.absorb(&input_knowledge[*c]);
                                }
                                if let MirScalarExpr::Column(c) = &**expr2 {
                                    knowledge.absorb(&input_knowledge[*c]);
                                }
                                // Absorb literal knowledge about columns.
                                knowledge.absorb(&DatumKnowledge::from(&**expr1));
                                knowledge.absorb(&DatumKnowledge::from(&**expr2));

                                // Write back unified knowledge to each column.
                                if let MirScalarExpr::Column(c) = &**expr1 {
                                    input_knowledge[*c].absorb(&knowledge);
                                }
                                if let MirScalarExpr::Column(c) = &**expr2 {
                                    input_knowledge[*c].absorb(&knowledge);
                                }
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
                                    input_knowledge[*c].nullable = false;
                                }
                            }
                        }
                    }

                    Ok(input_knowledge)
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    // Aggregate column knowledge from each input into one `Vec`.
                    let mut knowledges = Vec::new();
                    for input in inputs.iter_mut() {
                        for mut knowledge in self.harvest(input, knowledge, knowledge_stack)? {
                            // Do not propagate error literals beyond join inputs, since that may result
                            // in them being propagated to other inputs of the join and evaluated when
                            // they should not.
                            if let Some((Err(_), _)) = knowledge.value {
                                knowledge.value = None;
                            }
                            knowledges.push(knowledge);
                        }
                    }

                    // This only aggregates the column types of each input, not the
                    // keys of the inputs. It is unnecessary to aggregate the keys
                    // of the inputs since input keys are unnecessary for reducing
                    // `MirScalarExpr`s.
                    let folded_inputs_typ =
                        inputs.iter().fold(RelationType::empty(), |mut typ, i| {
                            typ.column_types.append(&mut i.typ().column_types);
                            typ
                        });

                    for equivalence in equivalences.iter_mut() {
                        let mut knowledge = DatumKnowledge::default();

                        // We can produce composite knowledge for everything in the equivalence class.
                        for expr in equivalence.iter_mut() {
                            optimize(expr, &folded_inputs_typ, &knowledges, knowledge_stack)?;
                            if let MirScalarExpr::Column(c) = expr {
                                knowledge.absorb(&knowledges[*c]);
                            }
                            knowledge.absorb(&DatumKnowledge::from(&*expr));
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
                        .map(|k| optimize(k, &input_typ, &input_knowledge[..], knowledge_stack))
                        .collect::<Result<Vec<_>, _>>()?;
                    for aggregate in aggregates.iter_mut() {
                        use mz_expr::AggregateFunc;
                        let knowledge = optimize(
                            &mut aggregate.expr,
                            &input_typ,
                            &input_knowledge[..],
                            knowledge_stack,
                        )?;
                        // This could be improved.
                        let knowledge = match aggregate.func {
                            AggregateFunc::MaxInt16
                            | AggregateFunc::MaxInt32
                            | AggregateFunc::MaxInt64
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
                            AggregateFunc::Count => DatumKnowledge {
                                value: None,
                                nullable: false,
                            },
                            _ => {
                                // The remaining aggregates are non-null if their inputs are non-null.
                                DatumKnowledge {
                                    value: None,
                                    nullable: knowledge.nullable,
                                }
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
                                k1.union(&k2);
                                k1
                            })
                            .collect();
                    }
                    Ok(know)
                }
            }
        })
    }
}

/// Information about a specific column.
#[derive(Clone, Debug)]
pub struct DatumKnowledge {
    /// If set, a specific value for the column.
    value: Option<(Result<mz_repr::Row, EvalError>, ColumnType)>,
    /// If false, the value is not `Datum::Null`.
    nullable: bool,
}

impl DatumKnowledge {
    // Intersects the possible states of a column.
    fn absorb(&mut self, other: &Self) {
        self.nullable &= other.nullable;
        if self.value.is_none() {
            self.value = other.value.clone()
        }
    }
    // Unions (weakens) the possible states of a column.
    fn union(&mut self, other: &Self) {
        self.nullable |= other.nullable;
        if self.value != other.value {
            self.value = None;
        }
    }
}

impl Default for DatumKnowledge {
    fn default() -> Self {
        Self {
            value: None,
            nullable: true,
        }
    }
}

impl From<&MirScalarExpr> for DatumKnowledge {
    fn from(expr: &MirScalarExpr) -> Self {
        if let MirScalarExpr::Literal(l, t) = expr {
            Self {
                value: Some((l.clone(), t.clone())),
                nullable: expr.is_literal_null(),
            }
        } else {
            Self::default()
        }
    }
}

impl From<&ColumnType> for DatumKnowledge {
    fn from(typ: &ColumnType) -> Self {
        Self {
            value: None,
            nullable: typ.nullable,
        }
    }
}

/// Attempts to optimize
///
/// `knowledge_stack` is a pre-allocated vector but is expected not to contain any elements.
pub fn optimize(
    expr: &mut MirScalarExpr,
    input_type: &RelationType,
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
                    if let Some((datum, typ)) = &column_knowledge[index].value {
                        *e = MirScalarExpr::Literal(datum.clone(), typ.clone());
                    }
                    column_knowledge[index].clone()
                }
                MirScalarExpr::Literal(_, _) | MirScalarExpr::CallUnmaterializable(_) => {
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::CallUnary { func, expr: _ } => {
                    let knowledge = knowledge_stack.pop().unwrap();
                    if knowledge.value.is_some() {
                        e.reduce(input_type);
                    } else if func == &UnaryFunc::IsNull(func::IsNull) && !knowledge.nullable {
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
                    if knowledge1.value.is_some() && knowledge2.value.is_some() {
                        e.reduce(input_type);
                    }
                    DatumKnowledge::from(&*e)
                }
                MirScalarExpr::CallVariadic { func: _, exprs } => {
                    if (0..exprs.len()).all(|_| knowledge_stack.pop().unwrap().value.is_some()) {
                        e.reduce(input_type);
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
                    know1.union(&know2);
                    know1
                }
            };
            knowledge_stack.push(result);
        },
    )?;
    let knowledge_datum = knowledge_stack.pop();
    knowledge_datum.ok_or_else(|| {
        TransformError::Internal(String::from("unexpectedly empty stack in optimize"))
    })
}
