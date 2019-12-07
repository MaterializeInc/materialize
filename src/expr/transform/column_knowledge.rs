// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;

use crate::UnaryFunc;
use crate::{RelationExpr, ScalarExpr};
use repr::Datum;
use repr::{ColumnType, ScalarType};

/// Harvest and act upon per-column information.
#[derive(Debug)]
pub struct ColumnKnowledge;

impl super::Transform for ColumnKnowledge {
    fn transform(&self, expr: &mut RelationExpr) {
        self.transform(expr)
    }
}

impl ColumnKnowledge {
    /// Transforms an expression through accumulated knowledge.
    pub fn transform(&self, expr: &mut RelationExpr) {
        ColumnKnowledge::harvest(expr, &mut HashMap::new());
    }

    /// Harvest per-column knowledge.
    fn harvest(
        expr: &mut RelationExpr,
        knowledge: &mut HashMap<crate::id::Id, Vec<DatumKnowledge>>,
    ) -> Vec<DatumKnowledge> {
        match expr {
            RelationExpr::ArrangeBy { input, .. } => ColumnKnowledge::harvest(input, knowledge),
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
                let value_knowledge = ColumnKnowledge::harvest(value, knowledge);
                let prior_knowledge =
                    knowledge.insert(crate::Id::Local(id.clone()), value_knowledge);
                let body_knowledge = ColumnKnowledge::harvest(body, knowledge);
                knowledge.remove(&crate::Id::Local(id.clone()));
                if let Some(prior_knowledge) = prior_knowledge {
                    knowledge.insert(crate::Id::Local(id.clone()), prior_knowledge);
                }
                body_knowledge
            }
            RelationExpr::Project { input, outputs } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge);
                outputs
                    .iter()
                    .map(|i| input_knowledge[*i].clone())
                    .collect()
            }
            RelationExpr::Map { input, scalars } => {
                let mut input_knowledge = ColumnKnowledge::harvest(input, knowledge);
                for scalar in scalars.iter_mut() {
                    let know = optimize(scalar, &input_knowledge[..]);
                    input_knowledge.push(know);
                }
                input_knowledge
            }
            RelationExpr::Filter { input, predicates } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge);
                for predicate in predicates.iter_mut() {
                    optimize(predicate, &input_knowledge[..]);
                }
                // If any predicate tests a column for equality, truth, or is_null, we learn stuff.
                // I guess we implement that later on.
                input_knowledge
            }
            RelationExpr::Join {
                inputs, variables, ..
            } => {
                let mut knowledges = inputs
                    .iter_mut()
                    .map(|i| ColumnKnowledge::harvest(i, knowledge))
                    .collect::<Vec<_>>();

                for variable in variables.iter() {
                    if !variable.is_empty() {
                        let mut know = knowledges[variable[0].0][variable[0].1].clone();
                        for (rel, col) in variable {
                            know.absorb(&knowledges[*rel][*col]);
                        }
                        for (rel, col) in variable {
                            knowledges[*rel][*col] = know.clone();
                        }
                    }
                }

                knowledges.into_iter().flat_map(|k| k).collect()
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_knowledge = ColumnKnowledge::harvest(input, knowledge);
                let mut output = group_key
                    .iter()
                    .map(|k| input_knowledge[*k].clone())
                    .collect::<Vec<_>>();
                for _aggregate in aggregates {
                    // This could be improved.
                    output.push(DatumKnowledge {
                        value: None,
                        nullable: true,
                    });
                }
                output
            }
            RelationExpr::TopK { input, .. } => ColumnKnowledge::harvest(input, knowledge),
            RelationExpr::Negate { input } => ColumnKnowledge::harvest(input, knowledge),
            RelationExpr::Threshold { input } => ColumnKnowledge::harvest(input, knowledge),
            RelationExpr::Union { left, right } => {
                let know1 = ColumnKnowledge::harvest(left, knowledge);
                let know2 = ColumnKnowledge::harvest(right, knowledge);

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
        }
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
pub fn optimize(expr: &mut ScalarExpr, column_knowledge: &[DatumKnowledge]) -> DatumKnowledge {
    match expr {
        ScalarExpr::Column(index) => {
            let index = *index;
            if let Some((datum, typ)) = &column_knowledge[index].value {
                *expr = ScalarExpr::Literal(datum.clone(), typ.clone());
            }
            column_knowledge[index].clone()
        }
        ScalarExpr::Literal(row, typ) => DatumKnowledge {
            value: Some((row.clone(), typ.clone())),
            nullable: row.unpack_first() == Datum::Null,
        },
        ScalarExpr::CallUnary { func, expr: inner } => {
            let knowledge = optimize(inner, column_knowledge);
            if let Some((datum, typ)) = knowledge.value {
                let evald = (func.func())(datum.unpack_first());
                *expr = ScalarExpr::Literal(repr::Row::pack(Some(evald)), func.output_type(typ));
                optimize(expr, column_knowledge)
            } else if func == &UnaryFunc::IsNull && !knowledge.nullable {
                *expr = ScalarExpr::Literal(
                    repr::Row::pack(Some(Datum::False)),
                    ColumnType::new(ScalarType::Bool).nullable(false),
                );
                optimize(expr, column_knowledge)
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            let knowledge1 = optimize(expr1, column_knowledge);
            let knowledge2 = optimize(expr2, column_knowledge);
            match (knowledge1.value, knowledge2.value) {
                (Some((v1, t1)), Some((v2, t2))) => {
                    let value = (func.func())(v1.unpack_first(), v2.unpack_first());
                    let typ = func.output_type(t1, t2);
                    *expr = ScalarExpr::Literal(repr::Row::pack(Some(value)), typ);
                    optimize(expr, column_knowledge)
                }
                _ => DatumKnowledge {
                    value: None,
                    nullable: true,
                },
            }
        }
        ScalarExpr::CallVariadic { func, exprs } => {
            let mut knows = Vec::new();
            for expr in exprs.iter_mut() {
                knows.push(optimize(expr, column_knowledge));
            }

            if knows.iter().all(|k| k.value.is_some()) {
                let mut values = Vec::new();
                let mut types = Vec::new();
                for k in knows.iter() {
                    let (value, typ) = k.value.as_ref().unwrap();
                    values.push(value.unpack_first());
                    types.push(typ.clone());
                }
                let value = (func.func())(&values);
                let typ = func.output_type(types);
                *expr = ScalarExpr::Literal(repr::Row::pack(Some(value)), typ);
                optimize(expr, column_knowledge)
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::If { cond, then, els } => {
            if let Some((value, _typ)) = optimize(cond, column_knowledge).value {
                match value.unpack_first() {
                    Datum::True => *expr = (**then).clone(),
                    Datum::False => *expr = (**els).clone(),
                    d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
                }
                optimize(expr, column_knowledge)
            } else {
                DatumKnowledge {
                    value: None,
                    nullable: true,
                }
            }
        }
        ScalarExpr::MatchCachedRegex { .. } => DatumKnowledge {
            value: None,
            nullable: true,
        },
    }
}
