// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod func;

use self::func::AggregateFunc;
use crate::ScalarExpr;
use pretty::Doc::{Newline, Space};
use pretty::{BoxDoc, Doc};
use repr::{ColumnType, Datum, RelationType, ScalarType};
use serde::{Deserialize, Serialize};

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
        // each Vec<(usize, usize)> is an equivalence class of (input_index, column_index)
        variables: Vec<Vec<(usize, usize)>>,
    },
    /// Group a dataflow by some columns and aggregate over each group
    Reduce {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        // these are appended to output in addition to all the columns of input that are in group_key
        aggregates: Vec<(AggregateExpr, ColumnType)>,
    },
    /// Groups and orders within each group, limiting output.
    TopK {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        order_key: Vec<usize>,
        limit: usize,
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
    /// Keep rows from a dataflow where the row counts are positive
    Threshold { input: Box<RelationExpr> },
    /// Return the union of two dataflows
    Union {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
    },
    // TODO Lookup/Arrange
}

impl RelationExpr {
    pub fn typ(&self) -> RelationType {
        match self {
            RelationExpr::Constant { rows, typ } => {
                for row in rows {
                    for (datum, column_typ) in row.iter().zip(typ.column_types.iter()) {
                        assert!(
                            datum.is_instance_of(column_typ),
                            "Expected datum of type {:?}, got value {:?}",
                            column_typ,
                            datum
                        );
                    }
                }
                typ.clone()
            }
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Let { body, .. } => body.typ(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ();
                RelationType {
                    column_types: outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                }
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ();
                for (_, column_typ) in scalars {
                    typ.column_types.push(column_typ.clone());
                }
                typ
            }
            RelationExpr::Filter { input, .. } => input.typ(),
            RelationExpr::Join { inputs, .. } => {
                let mut column_types = vec![];
                for input in inputs {
                    column_types.append(&mut input.typ().column_types);
                }
                RelationType { column_types }
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ();
                let mut column_types = group_key
                    .iter()
                    .map(|&i| input_typ.column_types[i].clone())
                    .collect::<Vec<_>>();
                for (_, column_typ) in aggregates {
                    column_types.push(column_typ.clone());
                }
                RelationType { column_types }
            }
            RelationExpr::TopK { input, .. } => input.typ(),
            RelationExpr::OrDefault { input, default } => {
                let typ = input.typ();
                for (column_typ, datum) in typ.column_types.iter().zip(default.iter()) {
                    assert!(datum.is_instance_of(column_typ));
                }
                typ
            }
            RelationExpr::Negate { input } => input.typ(),
            RelationExpr::Distinct { input } => input.typ(),
            RelationExpr::Threshold { input } => input.typ(),
            RelationExpr::Union { left, right } => {
                let left_typ = left.typ();
                let right_typ = right.typ();
                assert_eq!(left_typ.column_types.len(), right_typ.column_types.len());
                RelationType {
                    column_types: left_typ
                        .column_types
                        .iter()
                        .zip(right_typ.column_types.iter())
                        .map(|(l, r)| l.union(r))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap(),
                }
            }
        }
    }

    pub fn arity(&self) -> usize {
        self.typ().column_types.len()
    }

    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        RelationExpr::Constant { rows, typ }
    }

    pub fn project(self, outputs: Vec<usize>) -> Self {
        RelationExpr::Project {
            input: Box::new(self),
            outputs,
        }
    }

    pub fn map(self, scalars: Vec<(ScalarExpr, ColumnType)>) -> Self {
        RelationExpr::Map {
            input: Box::new(self),
            scalars,
        }
    }

    pub fn filter(self, predicates: Vec<ScalarExpr>) -> Self {
        RelationExpr::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    pub fn product(self, right: Self) -> Self {
        RelationExpr::Join {
            inputs: vec![self, right],
            variables: vec![],
        }
    }

    pub fn join(inputs: Vec<RelationExpr>, variables: Vec<Vec<(usize, usize)>>) -> Self {
        RelationExpr::Join { inputs, variables }
    }

    pub fn reduce(
        self,
        group_key: Vec<usize>,
        aggregates: Vec<(AggregateExpr, ColumnType)>,
    ) -> Self {
        RelationExpr::Reduce {
            input: Box::new(self),
            group_key,
            aggregates,
        }
    }

    pub fn or_default(self, default: Vec<Datum>) -> Self {
        RelationExpr::OrDefault {
            input: Box::new(self),
            default,
        }
    }

    pub fn negate(self) -> Self {
        RelationExpr::Negate {
            input: Box::new(self),
        }
    }

    pub fn distinct(self) -> Self {
        RelationExpr::Distinct {
            input: Box::new(self),
        }
    }

    pub fn threshold(self) -> Self {
        RelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    pub fn union(self, other: Self) -> Self {
        RelationExpr::Union {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn left_outer(self, left: Self) -> Self {
        let both = self;
        let both_arity = both.arity();
        let left_arity = left.arity();
        assert!(both_arity >= left_arity);

        RelationExpr::Join {
            inputs: vec![
                left.union(both.project((0..left_arity).collect()).distinct().negate()),
                RelationExpr::Constant {
                    rows: vec![vec![Datum::Null; both_arity - left_arity]],
                    typ: RelationType {
                        column_types: vec![
                            ColumnType::new(ScalarType::Null).nullable(true);
                            both_arity - left_arity
                        ],
                    },
                },
            ],
            variables: vec![],
        }
    }

    pub fn right_outer(self, right: Self) -> Self {
        let both = self;
        let both_arity = both.arity();
        let right_arity = right.arity();
        assert!(both_arity >= right_arity);

        RelationExpr::Join {
            inputs: vec![
                RelationExpr::Constant {
                    rows: vec![vec![Datum::Null; both_arity - right_arity]],
                    typ: RelationType {
                        column_types: vec![
                            ColumnType::new(ScalarType::Null).nullable(true);
                            both_arity - right_arity
                        ],
                    },
                },
                right.union(
                    both.project(((both_arity - right_arity)..both_arity).collect())
                        .distinct()
                        .negate(),
                ),
            ],
            variables: vec![],
        }
    }

    /// Collects the names of the dataflows that this relation_expr depends upon.
    pub fn uses_inner<'a, 'b>(&'a self, out: &'b mut Vec<&'a str>) {
        self.visit(&mut |e| match e {
            RelationExpr::Get { name, .. } => {
                out.push(&name);
            }
            RelationExpr::Let { name, .. } => {
                out.retain(|n| n != name);
            }
            _ => (),
        });
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::OrDefault { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Distinct { input } => f(input),
            RelationExpr::Threshold { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
            }
        }
    }

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self);
    }

    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::OrDefault { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Distinct { input } => f(input),
            RelationExpr::Threshold { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
            }
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self);
    }

    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f));
    }

    pub fn to_doc(&self) -> Doc<BoxDoc<()>> {
        // Woefully incomplete helper to format only column numbers and literals for now.
        fn fmt_scalar(scalar_expr: &ScalarExpr) -> String {
            match scalar_expr {
                ScalarExpr::Column(n) => format!("#{}", n),
                ScalarExpr::Literal(d) => format!("{}", d),
                _ => String::from("..."),
            }
        }

        match self {
            RelationExpr::Constant { rows, typ: _ } => {
                fn row_to_doc(row: &Vec<Datum>) -> Doc<BoxDoc<()>> {
                    let row = Doc::intersperse(row.iter().map(Doc::as_string), to_doc!(",", Space));
                    to_doc!("(", row.nest(1), ")")
                }

                if rows.len() == 1 && rows[0].len() == 1 {
                    Doc::as_string(&rows[0][0])
                } else if rows.len() == 1 {
                    row_to_doc(&rows[0])
                } else {
                    let rows = Doc::intersperse(rows.iter().map(row_to_doc), to_doc!(",", Space));
                    to_doc!("(", rows.nest(1), ")").group()
                }
            }
            RelationExpr::Get { name, typ: _ } => Doc::text(name),
            RelationExpr::Let { name, value, body } => Doc::nil()
                .append(to_doc!("let", Space, name, Space, "=", Space).group())
                .append(to_doc!(value).nest(1).group())
                .append(to_doc!(Space, "in", Space))
                .append(to_doc!(body).nest(1).group()),
            RelationExpr::Project { input, outputs } => Doc::nil()
                .append(to_doc!("Project { ", Newline))
                .append(to_doc!(input, ",", Newline).nest(1).group())
                .append({
                    let outputs =
                        Doc::intersperse(outputs.iter().map(Doc::as_string), to_doc!(",", Space));
                    to_doc!("outputs: [", outputs.nest(1), "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::Map { input, scalars } => Doc::nil()
                .append(to_doc!("Map { ", Newline))
                .append(to_doc!(input, ",", Newline).nest(1).group())
                .append({
                    let scalars = Doc::intersperse(
                        scalars.iter().map(|(s, _)| fmt_scalar(s)),
                        to_doc!(",", Space),
                    );
                    to_doc!("scalars: [", scalars.nest(1), "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::Filter { input, predicates } => Doc::nil()
                .append(to_doc!("Filter { ", Newline))
                .append(to_doc!(input, ",", Newline).nest(1).group())
                .append({
                    let predicates = Doc::intersperse(
                        predicates.iter().map(fmt_scalar),
                        to_doc!(",", Space).nest(1).group(),
                    );
                    to_doc!("predicates: [", predicates, "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::Join { inputs, variables } => Doc::nil()
                .append(to_doc!("Join { ", Newline))
                .append({
                    let inputs = Doc::intersperse(
                        inputs.iter().map(RelationExpr::to_doc),
                        to_doc!(",", Space).group(),
                    );
                    to_doc!(inputs, ",", Newline).nest(1).group()
                })
                .append({
                    fn pair_to_doc(p: &(usize, usize)) -> Doc<BoxDoc<()>, ()> {
                        to_doc!("(", p.0.to_string(), ",", Space, p.1.to_string(), ")").group()
                    }
                    let variables = Doc::intersperse(
                        variables.iter().map(|ps| {
                            let ps = Doc::intersperse(
                                ps.iter().map(pair_to_doc),
                                to_doc!(",", Space).nest(1).group(),
                            );
                            to_doc!("[", ps.nest(1), "]").group()
                        }),
                        to_doc!(",", Space).nest(1).group(),
                    );
                    to_doc!("variables: [", variables, "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => Doc::nil()
                .append(to_doc!("Reduce { ", Newline))
                .append(to_doc!(input, ",", Newline).nest(1).group())
                .append({
                    let keys = Doc::intersperse(
                        group_key.iter().map(|k| format!("{}", k)),
                        to_doc!(",", Space),
                    );
                    to_doc!("group_key: [", keys.nest(1), "]", Newline)
                        .nest(1)
                        .group()
                })
                .append({
                    let sums = Doc::intersperse(
                        aggregates.iter().map(|(a, _)| format!("{:?}", a.func)),
                        to_doc!(",", Space),
                    );
                    to_doc!("aggregates: [", sums.nest(1), "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
            } => to_doc!("TopK { ", "<Oops, TopK is not yet implemented>", " }").group(),
            RelationExpr::OrDefault { input, default } => Doc::nil()
                .append(to_doc!("OrDefault { ", Newline))
                .append(to_doc!(input, ",", Newline).nest(1).group())
                .append({
                    let default = Doc::intersperse(
                        default.iter().map(Doc::as_string),
                        to_doc!(",", Space).group(),
                    );
                    to_doc!("default: [", default.nest(1), "]", Newline)
                        .nest(1)
                        .group()
                })
                .append(to_doc!(" }")),
            RelationExpr::Negate { input } => Doc::nil()
                .append(to_doc!("Negate { ", Newline))
                .append(to_doc!(input, Newline).nest(1).group())
                .append(to_doc!(" }")),
            RelationExpr::Distinct { input } => Doc::nil()
                .append(to_doc!("Distinct { ", Newline))
                .append(to_doc!(input, Newline).nest(1).group())
                .append(to_doc!(" }")),
            RelationExpr::Threshold { input } => Doc::nil()
                .append(to_doc!("Threshold { ", Newline))
                .append(to_doc!(input, Newline).nest(1).group())
                .append(to_doc!(" }")),
            RelationExpr::Union { left, right } => Doc::nil()
                .append(to_doc!("Union { ", Newline))
                .append(to_doc!(left, ",", Newline).nest(1).group())
                .append(to_doc!(right, Newline).nest(1).group())
                .append(to_doc!(" }")),
        }
    }

    pub fn pretty(&self) -> String {
        format!("{}", self.to_doc().pretty(70))
    }
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: ScalarExpr,
    pub distinct: bool,
}
