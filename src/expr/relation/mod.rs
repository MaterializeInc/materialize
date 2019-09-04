// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod func;

use self::func::AggregateFunc;
use crate::pretty_pretty::{to_braced_doc, to_tightly_braced_doc};
use crate::ScalarExpr;
use pretty::Doc::Space;
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

    /// Convert this [`RelationExpr`] to a [`Doc`] or document for pretty printing. This
    /// function formats each dataflow operator instance as
    /// ```
    /// <operator-name> { <argument-1>, ..., <argument-n> }
    /// ```
    /// Arguments that are *not* dataflow operator instances are listed first and arguments
    /// that are dataflow operator instances, i.e., the input dataflows, are listed second.
    /// The former are prefixed with their names, while the latter are not. For example:
    /// ```
    /// Project { outputs: [1], Map { scalars: [665], "X" } }
    /// ```
    /// identifies the outputs for `Project` and the scalars for `Map` by name, whereas the
    /// sole inputs are nameless.
    pub fn to_doc(&self) -> Doc<BoxDoc<()>> {
        // A woefully incomplete helper function to format ScalarExprs. Right now, it elides
        // most of them and only shows column numbers and literals.
        fn fmt_scalar(scalar_expr: &ScalarExpr) -> String {
            match scalar_expr {
                ScalarExpr::Column(n) => format!("#{}", n),
                ScalarExpr::Literal(d) => format!("{}", d),
                _ => String::from("..."),
            }
        }

        // Do the actual conversion from RelationExpr to Doc.
        let doc = match self {
            RelationExpr::Constant { rows, typ: _ } => {
                // Death to the demon Clippy! It would happily complicate the signature of this
                // very local helper function beyond any recognition.
                #[allow(clippy::ptr_arg)]
                fn row_to_doc(row: &Vec<Datum>) -> Doc<BoxDoc<()>> {
                    let row = Doc::intersperse(row.iter().map(Doc::as_string), to_doc!(",", Space));
                    to_tightly_braced_doc("(", row, ")")
                }

                if rows.len() == 1 && rows[0].len() == 1 {
                    Doc::as_string(&rows[0][0])
                } else if rows.len() == 1 {
                    row_to_doc(&rows[0])
                } else {
                    let rows = Doc::intersperse(
                        rows.iter().map(|r| row_to_doc(r).group()),
                        to_doc!(",", Space),
                    );
                    to_tightly_braced_doc("(", rows, ")")
                }
            }
            RelationExpr::Get { name, typ: _ } => to_braced_doc("Get {", name, "}"),
            RelationExpr::Let { name, value, body } => to_braced_doc(
                "Let {",
                to_doc!(name, " = ", value.to_doc().nest(2), ",", Space, body),
                "}",
            ),
            RelationExpr::Project { input, outputs } => {
                let outputs =
                    Doc::intersperse(outputs.iter().map(Doc::as_string), to_doc!(",", Space));
                let outputs = to_tightly_braced_doc("outputs: [", outputs.nest(2), "]").group();
                to_braced_doc("Project {", to_doc!(outputs, ",", Space, input), "}")
            }
            RelationExpr::Map { input, scalars } => {
                let scalars = Doc::intersperse(
                    scalars.iter().map(|(s, _)| fmt_scalar(s)),
                    to_doc!(",", Space),
                );
                let scalars = to_tightly_braced_doc("scalars: [", scalars.nest(2), "]").group();
                to_braced_doc("Map {", to_doc!(scalars, ",", Space, input), "}")
            }
            RelationExpr::Filter { input, predicates } => {
                let predicates =
                    Doc::intersperse(predicates.iter().map(fmt_scalar), to_doc!(",", Space));
                let predicates =
                    to_tightly_braced_doc("predicates: [", predicates.nest(2), "]").group();
                to_braced_doc("Filter {", to_doc!(predicates, ",", Space, input), "}")
            }
            RelationExpr::Join { inputs, variables } => {
                fn pair_to_doc(p: &(usize, usize)) -> Doc<BoxDoc<()>, ()> {
                    to_doc!("(", p.0.to_string(), ", ", p.1.to_string(), ")")
                }

                let variables = Doc::intersperse(
                    variables.iter().map(|ps| {
                        let ps = Doc::intersperse(ps.iter().map(pair_to_doc), to_doc!(",", Space));
                        to_tightly_braced_doc("[", ps.nest(2), "]").group()
                    }),
                    to_doc!(",", Space),
                );
                let variables =
                    to_tightly_braced_doc("variables: [", variables.nest(2), "]").group();

                let inputs =
                    Doc::intersperse(inputs.iter().map(RelationExpr::to_doc), to_doc!(",", Space));

                to_braced_doc("Join {", to_doc!(variables, ",", Space, inputs), "}")
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let keys = Doc::intersperse(
                    group_key.iter().map(|k| format!("{}", k)),
                    to_doc!(",", Space),
                );
                let keys = to_tightly_braced_doc("group_key: [", keys.nest(2), "]").group();

                let aggregates = Doc::intersperse(
                    aggregates.iter().map(|(a, _)| format!("{:?}", a.func)),
                    to_doc!(",", Space),
                );
                let aggregates =
                    to_tightly_braced_doc("aggregates: [", aggregates.nest(2), "]").group();

                to_braced_doc(
                    "Reduce {",
                    to_doc!(keys, ",", Space, aggregates, ",", Space, input),
                    "}",
                )
            }
            RelationExpr::TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
            } => to_doc!("TopK { ", "\"Oops, TopK is not yet implemented!\"", " }").group(),
            RelationExpr::OrDefault { input, default } => {
                let default =
                    Doc::intersperse(default.iter().map(Doc::as_string), to_doc!(",", Space));
                let default = to_tightly_braced_doc("default: [", default.nest(2), "]").group();
                to_braced_doc("OrDefault {", to_doc!(default, ",", Space, input), "}")
            }
            RelationExpr::Negate { input } => to_braced_doc("Negate {", input, "}"),
            RelationExpr::Distinct { input } => to_braced_doc("Distinct {", input, "}"),
            RelationExpr::Threshold { input } => to_braced_doc("Threshold {", input, "}"),
            RelationExpr::Union { left, right } => {
                to_braced_doc("Union {", to_doc!(left, ",", Space, right), "}")
            }
        };

        // INVARIANT: RelationExpr's document is grouped. Much of the code above depends on this!
        doc.group()
    }

    /// Pretty-print this RelationExpr to a string with 70 columns maximum.
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
