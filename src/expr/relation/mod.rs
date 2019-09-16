// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![deny(missing_docs)]

pub mod func;

use self::func::AggregateFunc;
use crate::pretty_pretty::{to_braced_doc, to_tightly_braced_doc};
use crate::ScalarExpr;
use failure::ResultExt;
use pretty::Doc::{Newline, Space};
use pretty::{BoxDoc, Doc};
use repr::{ColumnType, Datum, RelationType, ScalarType};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
        /// Schema of the collection.
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
                        .with_context(|e| format!("{}\nIn {:#?}", e, self))
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
        for row in rows.iter() {
            for (datum, column_typ) in row.iter().zip(typ.column_types.iter()) {
                assert!(
                    datum.is_instance_of(column_typ),
                    "Expected datum of type {:?}, got value {:?}",
                    column_typ,
                    datum
                );
            }
        }
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

    /// Perform a key-wise reduction order by and limit.
    ///
    /// The `group_key` argument indicates columns in the input collection that should
    /// be grouped, the `order_key` argument indicates columns that should be further
    /// used to order records within groups, and the `limit` argument constrains the
    /// total number of records that should be produced in each group.
    pub fn top_k(self, group_key: Vec<usize>, order_key: Vec<usize>, limit: usize) -> Self {
        RelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
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

    /// Indicates if this is a constant empty collection.
    ///
    /// A false value does not mean the collection is known to be non-empty,
    /// only that we cannot currently determine that it is statically empty.
    pub fn is_empty(&self) -> bool {
        if let RelationExpr::Constant { rows, .. } = self {
            rows.is_empty()
        } else {
            false
        }
    }

    /// Appends unbound names on which this expression depends.
    ///
    /// This method is complicated only by the need to handle potential shadowing of let bindings,
    /// whose binding becomes visible only in their body.
    pub fn unbound_uses<'a, 'b>(&'a self, out: &'b mut Vec<&'a str>) {
        match self {
            RelationExpr::Let { name, value, body } => {
                // Append names from `value` but discard `name` from uses in `body`.
                // TODO: We could avoid the additional allocation by noting the length
                // of `out` after the `value` call, and only discarding occurrences from
                // `out` after this length. `Vec::retain()` makes this hard.
                value.unbound_uses(out);
                let mut temp = Vec::new();
                body.unbound_uses(&mut temp);
                temp.retain(|n| n != name);
                out.extend(temp.drain(..));
            }
            RelationExpr::Get { name, .. } => {
                // Stash the name for others to see.
                out.push(&name);
            }
            e => {
                // Continue recursively on members.
                e.visit1(|e| e.unbound_uses(out))
            }
        }
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

    /// Convert this [`RelationExpr`] to a [`Doc`] or document for pretty printing. This
    /// function formats each dataflow operator instance as
    /// ```ignore
    /// <operator-name> { <argument-1>, ..., <argument-n> }
    /// ```
    /// Arguments that are *not* dataflow operator instances are listed first and arguments
    /// that are dataflow operator instances, i.e., the input dataflows, are listed second.
    /// The former are prefixed with their names, while the latter are not. For example:
    /// ```ignore
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
            RelationExpr::Let { name, value, body } => {
                let binding = to_braced_doc("Let {", to_doc!(name, " = ", value), "};").group();
                to_doc!(binding, Newline, body)
            }
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

    /// Take ownership of `self`, leaving an empty `RelationExpr::Constant` in it's place
    pub fn take(&mut self) -> RelationExpr {
        let typ = self.typ();
        std::mem::replace(self, RelationExpr::Constant { rows: vec![], typ })
    }

    /// Store `self` in a `Let` and pass the corresponding `Get` to `body`
    pub fn let_in<Body>(self, body: Body) -> Result<RelationExpr, failure::Error>
    where
        Body: FnOnce(RelationExpr) -> Result<super::RelationExpr, failure::Error>,
    {
        if let RelationExpr::Get { .. } = self {
            // already done
            body(self)
        } else {
            let name = format!("tmp_{}", Uuid::new_v4());
            let get = RelationExpr::Get {
                name: name.clone(),
                typ: self.typ(),
            };
            let body = (body)(get)?;
            Ok(RelationExpr::Let {
                name,
                value: Box::new(self),
                body: Box::new(body),
            })
        }
    }

    /// Return every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is the 'outer' part of LEFT OUTER JOIN)
    pub fn anti_lookup(
        self,
        keys_and_values: RelationExpr,
        default: Vec<(Datum, ColumnType)>,
    ) -> RelationExpr {
        let keys = self;
        assert_eq!(keys_and_values.arity() - keys.arity(), default.len());
        keys_and_values
            .let_in(|get_keys_and_values| {
                Ok(get_keys_and_values
                    .clone()
                    .project((0..keys.arity()).collect())
                    .distinct()
                    .negate()
                    .union(keys)
                    // .map(
                    //     default
                    //         .iter()
                    //         .map(|(datum, typ)| (ScalarExpr::Literal(datum.clone()), typ.clone()))
                    //         .collect(),
                    // )
                    .product(RelationExpr::constant(
                        vec![default.iter().map(|(datum, _)| datum.clone()).collect()],
                        RelationType::new(default.iter().map(|(_, typ)| typ.clone()).collect()),
                    )))
            })
            .unwrap()
    }

    /// Return:
    /// * every row in keys_and_values
    /// * every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is LEFT OUTER JOIN)
    pub fn lookup(
        self,
        keys_and_values: RelationExpr,
        default: Vec<(Datum, ColumnType)>,
    ) -> RelationExpr {
        keys_and_values
            .let_in(|get_keys_and_values| {
                Ok(get_keys_and_values
                    .clone()
                    .union(self.anti_lookup(get_keys_and_values, default)))
            })
            .unwrap()
    }

    /// Perform some operation using `self` and then inner-join the results with `self`.
    /// This is useful in some edge cases in decorrelation where we need to track duplicate rows in `self` that might be lost by `branch`
    pub fn branch<Branch>(self, branch: Branch) -> Result<RelationExpr, failure::Error>
    where
        Branch: FnOnce(RelationExpr) -> Result<super::RelationExpr, failure::Error>,
    {
        self.let_in(|get_outer| {
            // TODO(jamii) this is a correct but not optimal value of key - optimize this by looking at what columns `branch` actually uses
            let key = (0..get_outer.arity()).collect::<Vec<_>>();
            let keyed_outer = get_outer.clone().project(key.clone()).distinct();
            keyed_outer.let_in(|get_keyed_outer| {
                let branch = branch(get_keyed_outer.clone())?;
                branch.let_in(|get_branch| {
                    let joined = RelationExpr::Join {
                        inputs: vec![get_outer.clone(), get_branch.clone()],
                        variables: key
                            .iter()
                            .enumerate()
                            .map(|(i, &k)| vec![(0, k), (1, i)])
                            .collect(),
                    }
                    // throw away the right-hand copy of the key we just joined on
                    .project(
                        (0..get_outer.arity())
                            .chain(
                                get_outer.arity() + get_keyed_outer.arity()
                                    ..get_outer.arity() + get_branch.arity(),
                            )
                            .collect(),
                    );
                    Ok(joined)
                })
            })
        })
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
