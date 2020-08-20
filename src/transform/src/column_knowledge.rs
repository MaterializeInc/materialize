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

use crate::TransformArgs;
use expr::{RelationExpr, ScalarExpr, UnaryFunc};
use repr::Datum;
use repr::{ColumnType, RelationType, ScalarType};

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct ColumnKnowledge;

impl crate::Transform for ColumnKnowledge {
    fn transform(
        &self,
        expr: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.transform(expr)
    }
}

impl ColumnKnowledge {
    /// Transforms an expression through accumulated knowledge.
    pub fn transform(&self, expr: &mut RelationExpr) -> Result<(), crate::TransformError> {
        ColumnKnowledge::harvest(expr, &mut HashMap::new())?;
        Ok(())
    }

    /// Harvest per-column knowledge.
    fn harvest(
        expr: &mut RelationExpr,
        knowledge: &mut HashMap<expr::Id, Vec<DatumKnowledge>>,
    ) -> Result<Vec<DatumKnowledge>, crate::TransformError> {
        Ok(match expr {
            RelationExpr::ArrangeBy { input, .. } => ColumnKnowledge::harvest(input, knowledge)?,
            RelationExpr::Get { id, typ } => knowledge
                .get(id)
                .cloned()
                .unwrap_or_else(|| typ.column_types.iter().map(DatumKnowledge::from).collect()),
            RelationExpr::Constant { rows, typ } => {
                if rows.len() == 1 {
                    let mut row_packer = repr::RowPacker::new();
                    rows[0]
                        .0
                        .iter()
                        .zip(typ.column_types.iter())
                        .map(|(datum, typ)| DatumKnowledge {
                            value: Some((row_packer.pack(Some(datum.clone())), typ.clone())),
                            nullable: datum == Datum::Null,
                        })
                        .collect()
                } else {
                    typ.column_types.iter().map(DatumKnowledge::from).collect()
                }
            }
            RelationExpr::Let { id, value, body } => {
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
            RelationExpr::Project { input, outputs } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                outputs
                    .iter()
                    .map(|i| input_knowledge[*i].clone())
                    .collect()
            }
            RelationExpr::Map { input, scalars } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for scalar in scalars.iter_mut() {
                    let know = optimize(scalar, &input.typ(), &input_knowledge[..])?;
                    input_knowledge.push(know);
                }
                input_knowledge
            }
            RelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for expr in exprs {
                    optimize(expr, &input.typ(), &input_knowledge[..])?;
                }
                let func_typ = func.output_type();
                input_knowledge.extend(func_typ.column_types.iter().map(DatumKnowledge::from));
                input_knowledge
            }
            RelationExpr::Filter { input, predicates } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input.typ(), &input_knowledge[..])?;
                }
                // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                for predicate in predicates.iter() {
                    // Equality tests allow us to unify the column knowledge of each input.
                    if let ScalarExpr::CallBinary { func, expr1, expr2 } = predicate {
                        if func == &expr::BinaryFunc::Eq {
                            // Collect knowledge about the inputs (for columns and literals).
                            let mut knowledge = DatumKnowledge::default();
                            if let ScalarExpr::Column(c) = &**expr1 {
                                knowledge.absorb(&input_knowledge[*c]);
                            }
                            if let ScalarExpr::Column(c) = &**expr2 {
                                knowledge.absorb(&input_knowledge[*c]);
                            }
                            // Absorb literal knowledge about columns.
                            knowledge.absorb(&DatumKnowledge::from(&**expr1));
                            knowledge.absorb(&DatumKnowledge::from(&**expr2));

                            // Write back unified knowledge to each column.
                            if let ScalarExpr::Column(c) = &**expr1 {
                                input_knowledge[*c].absorb(&knowledge);
                            }
                            if let ScalarExpr::Column(c) = &**expr2 {
                                input_knowledge[*c].absorb(&knowledge);
                            }
                        }
                    }
                    if let ScalarExpr::CallUnary {
                        func: UnaryFunc::Not,
                        expr,
                    } = predicate
                    {
                        if let ScalarExpr::CallUnary {
                            func: UnaryFunc::IsNull,
                            expr,
                        } = &**expr
                        {
                            if let ScalarExpr::Column(c) = &**expr {
                                input_knowledge[*c].nullable = false;
                            }
                        }
                    }
                }

                input_knowledge
            }
            RelationExpr::Join {
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
                        if let ScalarExpr::Column(c) = expr {
                            knowledge.absorb(&knowledges[*c]);
                        }
                        knowledge.absorb(&DatumKnowledge::from(&*expr));
                    }
                    for expr in equivalence.iter_mut() {
                        if let ScalarExpr::Column(c) = expr {
                            knowledges[*c] = knowledge.clone();
                        }
                    }
                }

                knowledges
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
            } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge)?;
                let mut output = group_key
                    .iter_mut()
                    .map(|k| optimize(k, &input.typ(), &input_knowledge[..]))
                    .collect::<Result<Vec<_>, _>>()?;
                for aggregate in aggregates.iter_mut() {
                    use expr::AggregateFunc;
                    let knowledge =
                        optimize(&mut aggregate.expr, &input.typ(), &input_knowledge[..])?;
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
            RelationExpr::TopK { input, .. } => ColumnKnowledge::harvest(input, knowledge)?,
            RelationExpr::Negate { input } => ColumnKnowledge::harvest(input, knowledge)?,
            RelationExpr::Threshold { input } => ColumnKnowledge::harvest(input, knowledge)?,
            RelationExpr::Union { left, right } => {
                let know1 = ColumnKnowledge::harvest(left, knowledge)?;
                let know2 = ColumnKnowledge::harvest(right, knowledge)?;

                know1
                    .into_iter()
                    .zip(know2)
                    .map(|(k1, k2)| DatumKnowledge {
                        value: if k1.value == k2.value {
                            k1.value.clone()
                        } else {
                            None
                        },
                        nullable: k1.nullable || k2.nullable,
                    })
                    .collect()
            }
        })
    }
}

/// Information about a specific column.
#[derive(Clone, Debug)]
pub struct DatumKnowledge {
    /// If set, a specific value for the column.
    value: Option<(repr::Row, ColumnType)>,
    /// If false, the value is not `Datum::Null`.
    nullable: bool,
}

impl DatumKnowledge {
    // Intersects the two knowledge about a column.
    fn absorb(&mut self, other: &Self) {
        self.nullable &= other.nullable;
        if self.value.is_none() {
            self.value = other.value.clone()
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

impl From<&ScalarExpr> for DatumKnowledge {
    fn from(expr: &ScalarExpr) -> Self {
        if let ScalarExpr::Literal(Ok(l), t) = expr {
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
    expr: &mut ScalarExpr,
    input_type: &RelationType,
    column_knowledge: &[DatumKnowledge],
) -> Result<DatumKnowledge, crate::TransformError> {
    Ok(match expr {
        ScalarExpr::Column(index) => {
            let index = *index;
            if let Some((datum, typ)) = &column_knowledge[index].value {
                *expr = ScalarExpr::Literal(Ok(datum.clone()), typ.clone());
            }
            column_knowledge[index].clone()
        }
        ScalarExpr::Literal(res, typ) => {
            let row = match res {
                Ok(row) => row,
                Err(err) => return Err(err.clone().into()),
            };
            DatumKnowledge {
                value: Some((row.clone(), typ.clone())),
                nullable: row.unpack_first() == Datum::Null,
            }
        }
        ScalarExpr::CallNullary(_) => {
            expr.reduce(input_type);
            optimize(expr, input_type, column_knowledge)?
        }
        ScalarExpr::CallUnary { func, expr: inner } => {
            let knowledge = optimize(inner, input_type, column_knowledge)?;
            if knowledge.value.is_some() {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)?
            } else if func == &UnaryFunc::IsNull && !knowledge.nullable {
                *expr = ScalarExpr::literal_ok(Datum::False, ScalarType::Bool.nullable(false));
                optimize(expr, input_type, column_knowledge)?
            } else {
                DatumKnowledge::default()
            }
        }
        ScalarExpr::CallBinary {
            func: _,
            expr1,
            expr2,
        } => {
            let knowledge1 = optimize(expr1, input_type, column_knowledge)?;
            let knowledge2 = optimize(expr2, input_type, column_knowledge)?;
            if knowledge1.value.is_some() && knowledge2.value.is_some() {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)?
            } else {
                DatumKnowledge::default()
            }
        }
        ScalarExpr::CallVariadic { func: _, exprs } => {
            let mut knows = Vec::new();
            for expr in exprs.iter_mut() {
                knows.push(optimize(expr, input_type, column_knowledge)?);
            }

            if knows.iter().all(|k| k.value.is_some()) {
                expr.reduce(input_type);
                optimize(expr, input_type, column_knowledge)?
            } else {
                DatumKnowledge::default()
            }
        }
        ScalarExpr::If { cond, then, els } => {
            if let Some((value, _typ)) = optimize(cond, input_type, column_knowledge)?.value {
                match value.unpack_first() {
                    Datum::True => *expr = (**then).clone(),
                    Datum::False | Datum::Null => *expr = (**els).clone(),
                    d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
                }
                optimize(expr, input_type, column_knowledge)?
            } else {
                DatumKnowledge::default()
            }
        }
    })
}
