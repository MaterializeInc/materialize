// Copyright Materialize, Inc. All rights reserved.
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

use expr::{EvalError, MirRelationExpr, MirScalarExpr, UnaryFunc};
use repr::Datum;
use repr::{ColumnType, RelationType, ScalarType};

use crate::TransformArgs;

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct ColumnKnowledge;

impl crate::Transform for ColumnKnowledge {
    fn transform(
        &self,
        expr: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.transform(expr)
    }
}

impl ColumnKnowledge {
    /// Transforms an expression through accumulated knowledge.
    pub fn transform(&self, expr: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        ColumnKnowledge::harvest(expr, &mut HashMap::new())?;
        Ok(())
    }

    /// Harvest per-column knowledge.
    fn harvest(
        expr: &mut MirRelationExpr,
        knowledge: &mut HashMap<expr::Id, Vec<DatumKnowledge>>,
    ) -> Result<Vec<DatumKnowledge>, crate::TransformError> {
        Ok(match expr {
            MirRelationExpr::ArrangeBy { input, .. } => ColumnKnowledge::harvest(input, knowledge)?,
            MirRelationExpr::Get { id, typ } => knowledge
                .get(id)
                .cloned()
                .unwrap_or_else(|| typ.column_types.iter().map(DatumKnowledge::from).collect()),
            MirRelationExpr::Constant { rows, typ } => {
                if let Ok([(row, _diff)]) = rows.as_deref() {
                    let mut row_packer = repr::RowPacker::new();
                    row.iter()
                        .zip(typ.column_types.iter())
                        .map(|(datum, typ)| DatumKnowledge {
                            value: Some((Ok(row_packer.pack(Some(datum.clone()))), typ.clone())),
                            nullable: datum == Datum::Null,
                        })
                        .collect()
                } else {
                    typ.column_types.iter().map(DatumKnowledge::from).collect()
                }
            }
            MirRelationExpr::Let { id, value, body } => {
                let value_knowledge = ColumnKnowledge::harvest(value, knowledge)?;
                let prior_knowledge =
                    knowledge.insert(expr::Id::Local(id.clone()), value_knowledge);
                let body_knowledge = ColumnKnowledge::harvest(body, knowledge)?;
                knowledge.remove(&expr::Id::Local(id.clone()));
                if let Some(prior_knowledge) = prior_knowledge {
                    knowledge.insert(expr::Id::Local(id.clone()), prior_knowledge);
                }
                body_knowledge
            }
            MirRelationExpr::Project { input, outputs } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                outputs
                    .iter()
                    .map(|i| input_knowledge[*i].clone())
                    .collect()
            }
            MirRelationExpr::Map { input, scalars } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for scalar in scalars.iter_mut() {
                    let know = optimize(scalar, &input.typ(), &input_knowledge[..]);
                    input_knowledge.push(know);
                }
                input_knowledge
            }
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for expr in exprs {
                    optimize(expr, &input.typ(), &input_knowledge[..]);
                }
                let func_typ = func.output_type();
                input_knowledge.extend(func_typ.column_types.iter().map(DatumKnowledge::from));
                input_knowledge
            }
            MirRelationExpr::Filter { input, predicates } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input.typ(), &input_knowledge[..]);
                }
                // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                for predicate in predicates.iter() {
                    // Equality tests allow us to unify the column knowledge of each input.
                    if let MirScalarExpr::CallBinary { func, expr1, expr2 } = predicate {
                        if func == &expr::BinaryFunc::Eq {
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
                        func: UnaryFunc::Not,
                        expr,
                    } = predicate
                    {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::IsNull,
                            expr,
                        } = &**expr
                        {
                            if let MirScalarExpr::Column(c) = &**expr {
                                input_knowledge[*c].nullable = false;
                            }
                        }
                    }
                }

                input_knowledge
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let mut knowledges = Vec::new();
                for input in inputs.iter_mut() {
                    for knowledge in ColumnKnowledge::harvest(input, knowledge)? {
                        knowledges.push(knowledge);
                    }
                }

                for equivalence in equivalences.iter_mut() {
                    let mut knowledge = DatumKnowledge::default();

                    // We can produce composite knowledge for everything in the equivalence class.
                    for expr in equivalence.iter_mut() {
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

                knowledges
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                let mut output = group_key
                    .iter_mut()
                    .map(|k| optimize(k, &input.typ(), &input_knowledge[..]))
                    .collect::<Vec<_>>();
                for aggregate in aggregates.iter_mut() {
                    use expr::AggregateFunc;
                    let knowledge =
                        optimize(&mut aggregate.expr, &input.typ(), &input_knowledge[..]);
                    // This could be improved.
                    let knowledge = match aggregate.func {
                        AggregateFunc::MaxInt32
                        | AggregateFunc::MaxInt64
                        | AggregateFunc::MaxFloat32
                        | AggregateFunc::MaxFloat64
                        | AggregateFunc::MaxDecimal
                        | AggregateFunc::MaxBool
                        | AggregateFunc::MaxString
                        | AggregateFunc::MaxDate
                        | AggregateFunc::MaxTimestamp
                        | AggregateFunc::MaxTimestampTz
                        | AggregateFunc::MinInt32
                        | AggregateFunc::MinInt64
                        | AggregateFunc::MinFloat32
                        | AggregateFunc::MinFloat64
                        | AggregateFunc::MinDecimal
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
                        _ => {
                            // All aggregates are non-null if their inputs are non-null.
                            DatumKnowledge {
                                value: None,
                                nullable: knowledge.nullable,
                            }
                        }
                    };
                    output.push(knowledge);
                }
                output
            }
            MirRelationExpr::TopK { input, .. } => ColumnKnowledge::harvest(input, knowledge)?,
            MirRelationExpr::Negate { input } => ColumnKnowledge::harvest(input, knowledge)?,
            MirRelationExpr::Threshold { input } => ColumnKnowledge::harvest(input, knowledge)?,
            MirRelationExpr::DeclareKeys { input, .. } => {
                ColumnKnowledge::harvest(input, knowledge)?
            }
            MirRelationExpr::Union { base, inputs } => {
                let mut know = ColumnKnowledge::harvest(base, knowledge)?;
                for input in inputs {
                    know = know
                        .into_iter()
                        .zip_eq(ColumnKnowledge::harvest(input, knowledge)?)
                        .map(|(mut k1, k2)| {
                            k1.union(&k2);
                            k1
                        })
                        .collect();
                }
                know
            }
        })
    }
}

/// Information about a specific column.
#[derive(Clone, Debug)]
pub struct DatumKnowledge {
    /// If set, a specific value for the column.
    value: Option<(Result<repr::Row, EvalError>, ColumnType)>,
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
pub fn optimize(
    expr: &mut MirScalarExpr,
    input_type: &RelationType,
    column_knowledge: &[DatumKnowledge],
) -> DatumKnowledge {
    match expr {
        MirScalarExpr::Column(index) => {
            let index = *index;
            if let Some((datum, typ)) = &column_knowledge[index].value {
                *expr = MirScalarExpr::Literal(datum.clone(), typ.clone());
            }
            column_knowledge[index].clone()
        }
        MirScalarExpr::Literal(row, typ) => DatumKnowledge {
            value: Some((row.clone(), typ.clone())),
            nullable: match row {
                Ok(row) => row.unpack_first() == Datum::Null,
                Err(_) => false,
            },
        },
        MirScalarExpr::CallNullary(_) => DatumKnowledge::default(),
        MirScalarExpr::CallUnary { func, expr: inner } => {
            let knowledge = optimize(inner, input_type, column_knowledge);
            if knowledge.value.is_some() {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)
            } else if func == &UnaryFunc::IsNull && !knowledge.nullable {
                *expr = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
                optimize(expr, input_type, column_knowledge)
            } else {
                DatumKnowledge::default()
            }
        }
        MirScalarExpr::CallBinary {
            func: _,
            expr1,
            expr2,
        } => {
            let knowledge1 = optimize(expr1, input_type, column_knowledge);
            let knowledge2 = optimize(expr2, input_type, column_knowledge);
            if knowledge1.value.is_some() && knowledge2.value.is_some() {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)
            } else {
                DatumKnowledge::default()
            }
        }
        MirScalarExpr::CallVariadic { func: _, exprs } => {
            let mut knows = Vec::new();
            for expr in exprs.iter_mut() {
                knows.push(optimize(expr, input_type, column_knowledge));
            }

            if knows.iter().all(|k| k.value.is_some()) {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)
            } else {
                DatumKnowledge::default()
            }
        }
        MirScalarExpr::If { cond: _, then, els } => {
            // Leave `cond` un-optimized, as we should not remove the conditional
            // nature of the evaluation based on column knowledge: the resulting
            // expression could then move down past a filter or join that provided
            // the guarantees, and would become wrong.
            //
            // Instead, we can optimize each of the branches, and union the states
            // of their columns.
            let mut know1 = optimize(then, input_type, column_knowledge);
            let know2 = optimize(els, input_type, column_knowledge);
            know1.union(&know2);
            know1
        }
    }
}
