// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![deny(missing_docs)]

pub mod func;

use self::func::AggregateFunc;
use crate::ScalarExpr;
use pretty::Doc::{Newline, Space};
use pretty::{BoxDoc, Doc};
use repr::{ColumnType, Datum, RelationType, ScalarType};
use serde::{Deserialize, Serialize};

/// An abstract syntax tree which defines a collection.
///
/// The AST is meant reflect the capabilities of the [`differential_dataflow::Collection`] type,
/// written generically enough to avoid run-time compilation work.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum RelationExpr {
    /// Always return the same value
    Constant {
        /// Rows of the constant collection.
        rows: Vec<Vec<Datum>>,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Get an existing dataflow
    Get {
        /// The name of the collection to load.
        name: String,
        /// Schema of the colleciton.
        typ: RelationType,
    },
    /// Introduce a temporary dataflow
    Let {
        /// The name to be used in `Get` variants to retrieve `value`.
        name: String,
        /// The collection to be bound to `name`.
        value: Box<RelationExpr>,
        /// The result of the `Let`, evaluated with `name` bound to `value`.
        body: Box<RelationExpr>,
    },
    /// Project out some columns from a dataflow
    Project {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Indices of columns to retain.
        outputs: Vec<usize>,
    },
    /// Append new columns to a dataflow
    Map {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Expressions which determine values to append to each row.
        scalars: Vec<(ScalarExpr, ColumnType)>,
    },
    /// Keep rows from a dataflow where all the predicates are true
    Filter {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Predicates, each of which must be true.
        predicates: Vec<ScalarExpr>,
    },
    /// Join several collections, where some columns must be equal.
    ///
    /// For further details consult the documentation for [`RelationExpr::join`].
    Join {
        /// A sequence of input relations.
        inputs: Vec<RelationExpr>,
        /// A sequence of equivalence classes of `(input_index, column_index)`.
        ///
        /// Each element of the sequence is a set of pairs, where the values described by each pair must
        /// be equal to all other values in the same set.
        variables: Vec<Vec<(usize, usize)>>,
    },
    /// Group a dataflow by some columns and aggregate over each group
    Reduce {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Expressions which determine values to append to each row, after the group keys.
        aggregates: Vec<(AggregateExpr, ColumnType)>,
    },
    /// Groups and orders within each group, limiting output.
    TopK {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Column indices used to order rows within groups.
        order_key: Vec<usize>,
        /// Number of records to retain, where the limit may be exceeded in the case of ties.
        limit: usize,
    },
    /// If the input is empty, return a default row
    // Used only for some SQL aggregate edge cases
    OrDefault {
        /// The source collection.
        input: Box<RelationExpr>,
        /// A row to introduce should `input` be empty.
        default: Vec<Datum>,
    },
    /// Return a dataflow where the row counts are negated
    Negate {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Return a dataflow where the row counts are all set to 1
    Distinct {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Keep rows from a dataflow where the row counts are positive
    Threshold {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Adds the frequencies of elements in both sets.
    Union {
        /// A source collection.
        left: Box<RelationExpr>,
        /// A source collection.
        right: Box<RelationExpr>,
    },
    // TODO Lookup/Arrange
}

impl RelationExpr {
    /// Reports the schema of the relation.
    ///
    /// This method determines the type through recursive traversal of the
    /// relation expression, drawing from the types of base collections.
    /// As such, this is not an especially cheap method, and should be used
    /// judiciously.
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

    /// The number of columns in the relation.
    ///
    /// This number is determined from the type, which is determined recursively
    /// at non-trivial cost.
    pub fn arity(&self) -> usize {
        self.typ().column_types.len()
    }

    /// Constructs a constant collection from specific rows and schema.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        RelationExpr::Constant { rows, typ }
    }

    /// Retains only the columns specified by `output`.
    pub fn project(self, outputs: Vec<usize>) -> Self {
        RelationExpr::Project {
            input: Box::new(self),
            outputs,
        }
    }

    /// Append to each row the results of applying elements of `scalar`.
    pub fn map(self, scalars: Vec<(ScalarExpr, ColumnType)>) -> Self {
        RelationExpr::Map {
            input: Box::new(self),
            scalars,
        }
    }

    /// Retain only the rows satisifying each of several predicates.
    pub fn filter(self, predicates: Vec<ScalarExpr>) -> Self {
        RelationExpr::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    /// Form the Cartesian outer-product of rows in both inputs.
    pub fn product(self, right: Self) -> Self {
        RelationExpr::Join {
            inputs: vec![self, right],
            variables: vec![],
        }
    }

    /// Performs a relational equijoin among the input collections.
    ///
    /// The sequence `inputs` each describe different input collections, and the sequence `variables` describes
    /// equality constraints that some of their columns must satisfy. Each element in `variable` describes a set
    /// of pairs  `(input_index, column_index)` where every value described by that set must be equal.
    ///
    /// For example, the pair `(input, column)` indexes into `inputs[input][column]`, extracting the `input`th
    /// input collection and for each row examining its `column`th column.
    ///
    /// # Example
    ///
    /// ```rust
    /// use repr::{Datum, ColumnType, RelationType, ScalarType};
    /// use expr::RelationExpr;
    ///
    /// // A common schema for each input.
    /// let schema = RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Int32),
    ///     ColumnType::new(ScalarType::Int32),
    /// ]);
    ///
    /// // the specific data are not important here.
    /// let data = vec![Datum::Int32(0), Datum::Int32(1)];
    ///
    /// // Three collections that could have been different.
    /// let input0 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input1 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input2 = RelationExpr::constant(vec![data.clone()], schema.clone());
    ///
    /// // Join the three relations looking for triangles, like so.
    /// //
    /// //     Output(A,B,C) := Input0(A,B), Input1(B,C), Input2(A,C)
    /// let joined = RelationExpr::join(
    ///     vec![input0, input1, input2],
    ///     vec![
    ///         vec![(0,0), (2,0)], // fields A of inputs 0 and 2.
    ///         vec![(0,1), (1,0)], // fields B of inputs 0 and 1.
    ///         vec![(1,1), (2,1)], // fields C of inputs 1 and 2.
    ///     ],
    /// );
    ///
    /// // Technically the above produces `Output(A,B,B,C,A,C)` because the columns are concatenated.
    /// // A projection resolves this and produces the correct output.
    /// let result = joined.project(vec![0, 1, 3]);
    /// ```
    pub fn join(inputs: Vec<RelationExpr>, variables: Vec<Vec<(usize, usize)>>) -> Self {
        RelationExpr::Join { inputs, variables }
    }

    /// Perform a key-wise reduction / aggregation.
    ///
    /// The `group_key` argument indicates columns in the input collection that should
    /// be grouped, and `aggregates` lists aggregation functions each of which produces
    /// one output column in addition to the keys.
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

    /// Substitutes `default` if `self` is empty.
    pub fn or_default(self, default: Vec<Datum>) -> Self {
        RelationExpr::OrDefault {
            input: Box::new(self),
            default,
        }
    }

    /// Negates the occurrences of each row.
    pub fn negate(self) -> Self {
        RelationExpr::Negate {
            input: Box::new(self),
        }
    }

    /// Removes all but the first occurrence of each row.
    pub fn distinct(self) -> Self {
        RelationExpr::Distinct {
            input: Box::new(self),
        }
    }

    /// Discards rows with a negative frequency.
    pub fn threshold(self) -> Self {
        RelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    /// Produces one collection where each row is present with the sum of its frequencies in each input.
    pub fn union(self, other: Self) -> Self {
        RelationExpr::Union {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// A helper method to finish a left outer join.
    ///
    /// This method should be called as `left.join(right).left_outer(left)' in order
    /// to effect a left outer join. It most likely should not be called in any other
    /// context without further investigation.
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

    /// A helper method to finish a right outer join.
    ///
    /// This method should be called as `left.join(right).right_outer(right)' in order
    /// to effect a right outer join. It most likely should not be called in any other
    /// context without further investigation.
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

    /// Applies `f` to each child `RelationExpr`.
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

    /// Post-order visitor for each `RelationExpr`.
    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self);
    }

    /// Applies `f` to each child `RelationExpr`.
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

    /// Post-order visitor for each `RelationExpr`.
    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self);
    }

    /// Pre-order visitor for each `RelationExpr`.
    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f));
    }

    /// Formats the `RelationExpr`.
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
                #[allow(clippy::ptr_arg)] // Complicates signature of local helper beyond recognition!
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

/// Describes an aggregation expression.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: ScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}
