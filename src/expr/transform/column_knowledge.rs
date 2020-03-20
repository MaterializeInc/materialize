// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use repr::Datum;
use repr::{ColumnType, RelationType, ScalarType};

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr, UnaryFunc};

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct ColumnKnowledge;

impl super::Transform for ColumnKnowledge {
    fn transform(
        &self,
        expr: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) -> Result<(), super::TransformError> {
        self.transform(expr, env)
    }
}

impl ColumnKnowledge {
    /// Transforms an expression through accumulated knowledge.
    pub fn transform(
        &self,
        expr: &mut RelationExpr,
        env: &EvalEnv,
    ) -> Result<(), super::TransformError> {
        ColumnKnowledge::harvest(expr, env, &mut HashMap::new())?;
        Ok(())
    }

    /// Harvest per-column knowledge.
    fn harvest(
        expr: &mut RelationExpr,
        env: &EvalEnv,
        knowledge: &mut HashMap<crate::id::Id, Vec<DatumKnowledge>>,
    ) -> Result<Vec<DatumKnowledge>, super::TransformError> {
        Ok(match expr {
            RelationExpr::ArrangeBy { input, .. } => {
                ColumnKnowledge::harvest(input, env, knowledge)?
            }
            RelationExpr::Get { id, typ } => knowledge.get(id).cloned().unwrap_or_else(|| {
                typ.column_types
                    .iter()
                    .map(|ct| DatumKnowledge {
                        value: None,
                        nullable: ct.nullable,
                    })
                    .collect()
            }),
            RelationExpr::Constant { rows, typ } => {
                if rows.len() == 1 {
                    rows[0]
                        .0
                        .iter()
                        .zip(typ.column_types.iter())
                        .map(|(datum, typ)| DatumKnowledge {
                            value: Some((repr::Row::pack(Some(datum.clone())), typ.clone())),
                            nullable: datum == Datum::Null,
                        })
                        .collect()
                } else {
                    typ.column_types
                        .iter()
                        .map(|ct| DatumKnowledge {
                            value: None,
                            nullable: ct.nullable,
                        })
                        .collect()
                }
            }
            RelationExpr::Let { id, value, body } => {
                let value_knowledge = ColumnKnowledge::harvest(value, env, knowledge)?;
                let prior_knowledge =
                    knowledge.insert(crate::Id::Local(id.clone()), value_knowledge);
                let body_knowledge = ColumnKnowledge::harvest(body, env, knowledge)?;
                knowledge.remove(&crate::Id::Local(id.clone()));
                if let Some(prior_knowledge) = prior_knowledge {
                    knowledge.insert(crate::Id::Local(id.clone()), prior_knowledge);
                }
                body_knowledge
            }
            RelationExpr::Project { input, outputs } => {
                let input_knowledge = ColumnKnowledge::harvest(input, env, knowledge)?;
                outputs
                    .iter()
                    .map(|i| input_knowledge[*i].clone())
                    .collect()
            }
            RelationExpr::Map { input, scalars } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, env, knowledge)?;
                for scalar in scalars.iter_mut() {
                    let know = optimize(scalar, &input.typ(), env, &input_knowledge[..])?;
                    input_knowledge.push(know);
                }
                input_knowledge
            }
            RelationExpr::FlatMapUnary {
                input,
                func,
                expr,
                demand: _,
            } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, env, knowledge)?;
                optimize(expr, &input.typ(), env, &input_knowledge[..])?;
                let func_typ = func.output_type(&expr.typ(&input.typ()));
                input_knowledge.extend(func_typ.column_types.into_iter().map(|typ| {
                    DatumKnowledge {
                        value: None,
                        nullable: typ.nullable,
                    }
                }));
                input_knowledge
            }
            RelationExpr::Filter { input, predicates } => {
                let input_knowledge = ColumnKnowledge::harvest(input, env, knowledge)?;
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input.typ(), env, &input_knowledge[..])?;
                }
                // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                // I guess we implement that later on.
                input_knowledge
            }
            RelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let mut knowledges = Vec::new();
                for input in inputs.iter_mut() {
                    for knowledge in ColumnKnowledge::harvest(input, env, knowledge)? {
                        knowledges.push(knowledge);
                    }
                }

                for equivalence in equivalences.iter_mut() {
                    let mut knowledge = DatumKnowledge {
                        value: None,
                        nullable: true,
                    };

                    // We can produce composite knowledge for everything in the equivalence class.
                    for expr in equivalence.iter_mut() {
                        if let ScalarExpr::Column(c) = expr {
                            knowledge.absorb(&knowledges[*c]);
                        }
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
            } => {
                let input_knowledge = ColumnKnowledge::harvest(input, env, knowledge)?;
                let mut output = group_key
                    .iter_mut()
                    .map(|k| optimize(k, &input.typ(), env, &input_knowledge[..]))
                    .collect::<Result<Vec<_>, _>>()?;
                for _aggregate in aggregates {
                    // This could be improved.
                    output.push(DatumKnowledge {
                        value: None,
                        nullable: true,
                    });
                }
                output
            }
            RelationExpr::TopK { input, .. } => ColumnKnowledge::harvest(input, env, knowledge)?,
            RelationExpr::Negate { input } => ColumnKnowledge::harvest(input, env, knowledge)?,
            RelationExpr::Threshold { input } => ColumnKnowledge::harvest(input, env, knowledge)?,
            RelationExpr::Union { left, right } => {
                let know1 = ColumnKnowledge::harvest(left, env, knowledge)?;
                let know2 = ColumnKnowledge::harvest(right, env, knowledge)?;

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

#[derive(Clone, Debug)]
pub struct DatumKnowledge {
    /// If set, a specific value for the column.
    value: Option<(repr::Row, ColumnType)>,
    /// If false, the value is not `Datum::Null`.
    nullable: bool,
}

impl DatumKnowledge {
    fn absorb(&mut self, other: &Self) {
        self.nullable &= other.nullable;
        if self.value.is_none() {
            self.value = other.value.clone()
        }
    }
}

/// Attempts to optimize
pub fn optimize(
    expr: &mut ScalarExpr,
    input_type: &RelationType,
    env: &EvalEnv,
    column_knowledge: &[DatumKnowledge],
) -> Result<DatumKnowledge, super::TransformError> {
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
            expr.reduce(input_type, env);
            optimize(expr, input_type, env, column_knowledge)?
        }
        ScalarExpr::CallUnary { func, expr: inner } => {
            let knowledge = optimize(inner, input_type, env, column_knowledge)?;
            if knowledge.value.is_some() {
                expr.reduce(input_type, env);
                optimize(expr, input_type, env, column_knowledge)?
            } else if func == &UnaryFunc::IsNull && !knowledge.nullable {
                *expr = ScalarExpr::literal_ok(
                    Datum::False,
                    ColumnType::new(ScalarType::Bool).nullable(false),
                );
                optimize(expr, input_type, env, column_knowledge)?
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::CallBinary {
            func: _,
            expr1,
            expr2,
        } => {
            let knowledge1 = optimize(expr1, input_type, env, column_knowledge)?;
            let knowledge2 = optimize(expr2, input_type, env, column_knowledge)?;
            if knowledge1.value.is_some() && knowledge2.value.is_some() {
                expr.reduce(input_type, env);
                optimize(expr, input_type, env, column_knowledge)?
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::CallVariadic { func: _, exprs } => {
            let mut knows = Vec::new();
            for expr in exprs.iter_mut() {
                knows.push(optimize(expr, input_type, env, column_knowledge)?);
            }

            if knows.iter().all(|k| k.value.is_some()) {
                expr.reduce(input_type, env);
                optimize(expr, input_type, env, column_knowledge)?
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::If { cond, then, els } => {
            if let Some((value, _typ)) = optimize(cond, input_type, env, column_knowledge)?.value {
                match value.unpack_first() {
                    Datum::True => *expr = (**then).clone(),
                    Datum::False | Datum::Null => *expr = (**els).clone(),
                    d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
                }
                optimize(expr, input_type, env, column_knowledge)?
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
    })
}
