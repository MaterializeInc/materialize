// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::dataflow as old_dataflow;
use crate::repr;
use crate::repr::Datum;

pub type DatumType = repr::FType;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnType {
    pub typ: DatumType,
    pub is_nullable: bool,
}

pub type RelationType = Vec<ColumnType>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    Literal(Datum),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: old_dataflow::func::UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: old_dataflow::func::BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: old_dataflow::func::VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn eval_on(&self, data: &[Datum]) -> Datum {
        match self {
            ScalarExpr::Column(index) => data[*index].clone(),
            ScalarExpr::Literal(datum) => datum.clone(),
            ScalarExpr::CallUnary { func, expr } => {
                let eval = expr.eval_on(data);
                (func.func())(eval)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let eval1 = expr1.eval_on(data);
                let eval2 = expr2.eval_on(data);
                (func.func())(eval1, eval2)
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                let evals = exprs.into_iter().map(|e| e.eval_on(data)).collect();
                (func.func())(evals)
            }
            ScalarExpr::If { cond, then, els } => match cond.eval_on(data) {
                Datum::True => then.eval_on(data),
                Datum::False => els.eval_on(data),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub func: old_dataflow::func::AggregateFunc,
    pub expr: ScalarExpr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RelationExpr {
    /// Always return the same value
    Constant {
        rows: Vec<Vec<Datum>>,
        typ: RelationType,
    },
    /// Get an existing dataflow
    Get { name: String, typ: RelationType },
    /// Introduce a temporary dataflow
    Let {
        name: String,
        value: Box<RelationExpr>,
        body: Box<RelationExpr>,
    },
    /// Project out some columns from a dataflow
    Project {
        input: Box<RelationExpr>,
        outputs: Vec<usize>,
    },
    /// Append new columns to a dataflow
    Map {
        input: Box<RelationExpr>,
        // these are appended to output in addition to all the columns of input
        scalars: Vec<(ScalarExpr, ColumnType)>,
    },
    /// Keep rows from a dataflow where all the predicates are true
    Filter {
        input: Box<RelationExpr>,
        predicates: Vec<ScalarExpr>,
    },
    /// Join several dataflows together at once
    Join {
        inputs: Vec<RelationExpr>,
        // each HashSet is an equivalence class of (input_index, column_index)
        variables: Vec<HashSet<(usize, usize)>>,
    },
    /// Group a dataflow by some columns and aggregate over each group
    Reduce {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        // these are appended to output in addition to all the columns of input that are in group_key
        aggregates: Vec<(AggregateExpr, ColumnType)>,
    },
    /// If the input is empty, return a default row
    // Used only for some SQL aggregate edge cases
    OrDefault {
        input: Box<RelationExpr>,
        default: Vec<Datum>,
    },
    /// Return a dataflow where the row counts are negated
    Negate { input: Box<RelationExpr> },
    /// Return a dataflow where the row counts are all set to 1
    Distinct { input: Box<RelationExpr> },
    /// Return the union of two dataflows
    Union {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
    },
    // TODO Lookup/Arrange
}

impl repr::Datum {
    fn is_instance_of(&self, column_typ: &ColumnType) -> bool {
        self.ftype().is_instance_of(column_typ)
    }
}

impl DatumType {
    fn is_instance_of(&self, column_typ: &ColumnType) -> bool {
        self == &column_typ.typ || (self == &repr::FType::Null && column_typ.is_nullable)
    }
}

impl ColumnType {
    fn union(&self, other: &Self) -> Self {
        assert_eq!(self.typ, other.typ);
        ColumnType {
            typ: self.typ.clone(),
            is_nullable: self.is_nullable || other.is_nullable,
        }
    }
}

impl RelationExpr {
    fn typ(&self) -> Vec<ColumnType> {
        match self {
            RelationExpr::Constant { rows, typ } => {
                for row in rows {
                    for (datum, column_typ) in row.iter().zip(typ.iter()) {
                        assert!(datum.is_instance_of(column_typ));
                    }
                }
                typ.clone()
            }
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Let { body, .. } => body.typ(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ();
                outputs.iter().map(|&i| input_typ[i].clone()).collect()
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ();
                for (_, column_typ) in scalars {
                    typ.push(column_typ.clone());
                }
                typ
            }
            RelationExpr::Filter { input, .. } => input.typ(),
            RelationExpr::Join { inputs, .. } => {
                let mut typ = vec![];
                for input in inputs {
                    typ.append(&mut input.typ());
                }
                typ
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ();
                let mut typ = group_key
                    .iter()
                    .map(|&i| input_typ[i].clone())
                    .collect::<Vec<_>>();
                for (_, column_typ) in aggregates {
                    typ.push(column_typ.clone());
                }
                typ
            }
            RelationExpr::OrDefault { input, default } => {
                let typ = input.typ();
                for (column_typ, datum) in typ.iter().zip(default.iter()) {
                    assert!(datum.ftype().is_instance_of(column_typ));
                }
                typ
            }
            RelationExpr::Negate { input } => input.typ(),
            RelationExpr::Distinct { input } => input.typ(),
            RelationExpr::Union { left, right } => {
                let left_typ = left.typ();
                let right_typ = right.typ();
                assert_eq!(left_typ.len(), right_typ.len());
                left_typ
                    .iter()
                    .zip(right_typ.iter())
                    .map(|(l, r)| l.union(r))
                    .collect()
            }
        }
    }
}
