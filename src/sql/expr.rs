// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use expr as dataflow_expr;
use ore::collections::CollectionExt;
use repr::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;

// these happen to be unchanged at the moment, but there might be additions later
pub use dataflow_expr::like;
pub use dataflow_expr::{AggregateFunc, BinaryFunc, ColumnOrder, UnaryFunc, VariadicFunc};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Just like dataflow_expr::RelationExpr, except where otherwise noted below
pub enum RelationExpr {
    Constant {
        rows: Vec<Row>,
        typ: RelationType,
    },
    Get {
        name: String,
        typ: RelationType,
    },
    // only needed for CTEs
    // Let {
    //     name: Name,
    //     value: Box<RelationExpr>,
    //     body: Box<RelationExpr>,
    // },
    Project {
        input: Box<RelationExpr>,
        outputs: Vec<usize>,
    },
    Map {
        input: Box<RelationExpr>,
        scalars: Vec<ScalarExpr>,
    },
    Filter {
        input: Box<RelationExpr>,
        predicates: Vec<ScalarExpr>,
    },
    /// Unlike dataflow_expr::RelationExpr, we haven't yet compiled LeftOuter/RightOuter/FullOuter joins away into more primitive exprs
    Join {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
        on: ScalarExpr,
        kind: JoinKind,
    },
    /// Unlike dataflow_expr::RelationExpr, when `key` is empty AND `input` is empty this returns a single row with the aggregates evalauted over empty groups, rather than returning zero rows
    Reduce {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
    },
    Distinct {
        input: Box<RelationExpr>,
    },
    /// Groups and orders within each group, limiting output.
    TopK {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Column indices used to order rows within groups.
        order_key: Vec<ColumnOrder>,
        /// Number of records to retain
        limit: Option<usize>,
        /// Number of records to skip
        offset: usize,
    },
    Negate {
        input: Box<RelationExpr>,
    },
    Threshold {
        input: Box<RelationExpr>,
    },
    Union {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Just like dataflow::ScalarExpr, except where otherwise noted below.
pub enum ScalarExpr {
    /// Unlike dataflow::ScalarExpr, we can nest RelationExprs via eg Exists. This means that a variable could refer to a column of the current input, or to a column of an outer relation. We use ColumnRef to denote the difference.
    Column(ColumnRef),
    Parameter(usize),
    Literal(Row, ColumnType),
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
    /// Returns true if `expr` returns any rows
    Exists(Box<RelationExpr>),
    /// Given `expr` with arity 1. If expr returns:
    /// * 0 rows, return NULL
    /// * 1 row, return the value of that row
    /// * >1 rows, the sql spec says we should throw an error but we can't
    ///   (see https://github.com/MaterializeInc/materialize/issues/489)
    ///   so instead we return all the rows.
    ///   If there are multiple `Select` expressions in a single SQL query, the result is that we take the product of all of them.
    ///   This is counter to the spec, but is consistent with eg postgres' treatment of multiple set-returning-functions
    ///   (see https://tapoueh.org/blog/2017/10/set-returning-functions-and-postgresql-10/).
    Select(Box<RelationExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnRef {
    /// References a variable from the input relation
    Inner(usize),
    /// References a variable from the outer scope
    Outer(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<ScalarExpr>,
    pub distinct: bool,
}

impl RelationExpr {
    // TODO(jamii) this can't actually return an error atm - do we still need Result?
    /// Rewrite `self` into a `dataflow_expr::RelationExpr`.
    /// This requires rewriting all correlated subqueries (nested `RelationExpr`s) into flat queries
    pub fn decorrelate(mut self) -> Result<dataflow_expr::RelationExpr, failure::Error> {
        let mut id_gen = dataflow_expr::IdGen::default();
        self.split_subquery_predicates();
        dataflow_expr::RelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
            .let_in(&mut id_gen, |id_gen, get_outer| {
                self.applied_to(id_gen, get_outer)
            })
    }

    /// Rewrites predicates that contain subqueries so that the subqueries
    /// appear in their own later predicate when possible.
    ///
    /// For example, this function rewrites this expression
    ///
    /// ```text
    /// Filter {
    ///     predicates: [a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e]
    /// }
    /// ```
    ///
    /// like so:
    ///
    /// ```text
    /// Filter {
    ///     predicates: [
    ///         a = b AND c = d,
    ///         EXISTS (<subquery>),
    ///         (<subquery 2>) = e,
    ///     ]
    /// }
    /// ```
    ///
    /// The rewrite causes decorrelation to incorporate prior predicates into
    /// the outer relation upon which the subquery is evaluated. In the above
    /// rewritten example, the `EXISTS (<subquery>)` will only be evaluated for
    /// outer rows where `a = b AND c = d`. The second subquery, `(<subquery 2>)
    /// = e`, will be further restricted to outer rows that match `A = b AND c =
    /// d AND EXISTS(<subquery>)`. This can vastly reduce the cost of the
    /// subquery, especially when the original conjuction contains join keys.
    fn split_subquery_predicates(&mut self) {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),

            RelationExpr::Project { input, .. }
            | RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input }
            | RelationExpr::Reduce { input, .. }
            | RelationExpr::TopK { input, .. } => input.split_subquery_predicates(),

            RelationExpr::Join { left, right, .. } | RelationExpr::Union { left, right } => {
                left.split_subquery_predicates();
                right.split_subquery_predicates();
            }

            RelationExpr::Map { input, scalars } => {
                input.split_subquery_predicates();
                for scalar in scalars {
                    scalar.split_subquery_predicates();
                }
            }

            RelationExpr::Filter { input, predicates } => {
                input.split_subquery_predicates();
                let mut subqueries = vec![];
                for predicate in &mut *predicates {
                    predicate.split_subquery_predicates();
                    predicate.extract_conjucted_subqueries(&mut subqueries);
                }
                // TODO(benesch): we could be smarter about the order in which
                // we emit subqueries. At the moment we just emit in the order
                // we discovered them, but ideally we'd emit them in an order
                // that accounted for their cost/selectivity. E.g., low-cost,
                // high-selectivity subqueries should go first.
                for subquery in subqueries {
                    predicates.push(subquery);
                }
            }
        }
    }

    /// Return a `dataflow_expr::RelationExpr` which evaluates `self` once for each row returned by `get_outer`.
    /// (Where `get_outer` should be a `Get` with no duplicate rows)
    fn applied_to(
        self,
        id_gen: &mut dataflow_expr::IdGen,
        get_outer: dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::RelationExpr, failure::Error> {
        use self::RelationExpr::*;
        use dataflow_expr::RelationExpr as SR;
        if let dataflow_expr::RelationExpr::Get { .. } = &get_outer {
        } else {
            panic!(
                "get_outer: expected a RelationExpr::Get, found {:?}",
                get_outer
            );
        }
        match self {
            Constant { rows, typ } => Ok(get_outer.product(SR::Constant {
                rows: rows.into_iter().map(|row| (row, 1)).collect(),
                typ,
            })),
            Get { name, typ } => Ok(get_outer.product(SR::Get { name, typ })),
            Project { input, outputs } => {
                let input = input.applied_to(id_gen, get_outer.clone())?;
                let outputs = (0..get_outer.arity())
                    .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                    .collect::<Vec<_>>();
                Ok(input.project(outputs))
            }
            Map { input, scalars } => {
                let mut input = input.applied_to(id_gen, get_outer.clone())?;
                for scalar in scalars {
                    let old_arity = input.arity();
                    let scalar = scalar.applied_to(id_gen, get_outer.arity(), &mut input)?;
                    let new_arity = input.arity();
                    input = input.map(vec![scalar]);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project((0..old_arity).chain(vec![new_arity]).collect());
                    }
                }
                Ok(input)
            }
            Filter { input, predicates } => {
                let mut input = input.applied_to(id_gen, get_outer.clone())?;
                for predicate in predicates {
                    let old_arity = input.arity();
                    let predicate = predicate.applied_to(id_gen, get_outer.arity(), &mut input)?;
                    let new_arity = input.arity();
                    input = input.filter(vec![predicate]);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project((0..old_arity).collect());
                    }
                }
                Ok(input)
            }
            Join {
                left,
                right,
                on,
                kind,
            } => {
                let oa = get_outer.arity();
                let left = left.applied_to(id_gen, get_outer.clone())?;
                let la = left.arity() - oa;
                left.let_in(id_gen, |id_gen, get_left| {
                    let right = right.applied_to(id_gen, get_outer.clone())?;
                    let ra = right.arity() - oa;
                    right.let_in(id_gen, |id_gen, get_right| {
                        let mut product = SR::Join {
                            inputs: vec![get_left.clone(), get_right.clone()],
                            variables: (0..oa).map(|i| vec![(0, i), (1, i)]).collect(),
                        }
                        // project away the repeated copy of get_outer
                        .project(
                            (0..(oa + la))
                                .chain((oa + la + oa)..(oa + la + oa + ra))
                                .collect(),
                        );
                        let old_arity = product.arity();
                        let on = on.applied_to(id_gen, get_outer.arity(), &mut product)?;
                        let mut join = product.filter(vec![on]);
                        let new_arity = join.arity();
                        if old_arity != new_arity {
                            // this means we added some columns to handle subqueries, and now we need to get rid of them
                            join = join.project((0..old_arity).collect());
                        }
                        join.let_in(id_gen, |id_gen, get_join| {
                            let mut result = get_join.clone();
                            if let JoinKind::LeftOuter | JoinKind::FullOuter = kind {
                                let left_outer = get_left.clone().anti_lookup(
                                    id_gen,
                                    get_join.clone(),
                                    (0..ra)
                                        .map(|_| (Datum::Null, ColumnType::new(ScalarType::Null)))
                                        .collect(),
                                );
                                result = result.union(left_outer);
                            }
                            if let JoinKind::RightOuter | JoinKind::FullOuter = kind {
                                let right_outer = get_right
                                    .clone()
                                    .anti_lookup(
                                        id_gen,
                                        get_join
                                            .clone()
                                            // need to swap left and right to make the anti_lookup work
                                            .project(
                                                (0..oa)
                                                    .chain((oa + la)..(oa + la + ra))
                                                    .chain((oa)..(oa + la))
                                                    .collect(),
                                            ),
                                        (0..la)
                                            .map(|_| {
                                                (Datum::Null, ColumnType::new(ScalarType::Null))
                                            })
                                            .collect(),
                                    )
                                    // swap left and right back again
                                    .project(
                                        (0..oa)
                                            .chain((oa + ra)..(oa + ra + la))
                                            .chain((oa)..(oa + ra))
                                            .collect(),
                                    );
                                result = result.union(right_outer);
                            }
                            Ok(result)
                        })
                    })
                })
            }
            Union { left, right } => {
                let left = left.applied_to(id_gen, get_outer.clone())?;
                let right = right.applied_to(id_gen, get_outer.clone())?;
                Ok(left.union(right))
            }
            Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let mut input = input.applied_to(id_gen, get_outer.clone())?;
                let applied_group_key = (0..get_outer.arity())
                    .chain(group_key.iter().map(|i| get_outer.arity() + i))
                    .collect();
                let applied_aggregates = aggregates
                    .clone()
                    .into_iter()
                    // TODO(jamii) how do we deal with the extra columns here?
                    .map(|aggregate| {
                        Ok(aggregate.applied_to(id_gen, get_outer.arity(), &mut input)?)
                    })
                    .collect::<Result<Vec<_>, failure::Error>>()?;
                let input_type = input.typ();
                let default = applied_aggregates
                    .iter()
                    .map(|agg| (agg.func.default(), agg.typ(&input_type)))
                    .collect();
                let mut reduced = input.reduce(applied_group_key, applied_aggregates);
                if group_key.is_empty() {
                    reduced = get_outer.lookup(id_gen, reduced, default);
                }
                Ok(reduced)
            }
            Distinct { input } => Ok(input.applied_to(id_gen, get_outer)?.distinct()),
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                let input = input.applied_to(id_gen, get_outer.clone())?;
                let applied_group_key = (0..get_outer.arity())
                    .chain(group_key.iter().map(|i| get_outer.arity() + i))
                    .collect();
                let applied_order_key = order_key
                    .iter()
                    .map(|column_order| ColumnOrder {
                        column: column_order.column + get_outer.arity(),
                        desc: column_order.desc,
                    })
                    .collect();
                Ok(input.top_k(applied_group_key, applied_order_key, limit, offset))
            }
            Negate { input } => Ok(input.applied_to(id_gen, get_outer)?.negate()),
            Threshold { input } => Ok(input.applied_to(id_gen, get_outer)?.threshold()),
        }
    }

    /// Visits the column references within this `RelationExpr`.
    fn visit_columns<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut ColumnRef),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),

            RelationExpr::Project { input, .. }
            | RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input }
            | RelationExpr::TopK { input, .. } => input.visit_columns(f),

            RelationExpr::Union { left, right } => {
                left.visit_columns(f);
                right.visit_columns(f);
            }

            RelationExpr::Join {
                left, right, on, ..
            } => {
                left.visit_columns(f);
                right.visit_columns(f);
                on.visit_columns(f);
            }

            RelationExpr::Reduce {
                input, aggregates, ..
            } => {
                input.visit_columns(f);
                for aggregate in aggregates {
                    aggregate.visit_columns(f);
                }
            }

            RelationExpr::Map { input, scalars } => {
                input.visit_columns(f);
                for scalar in scalars {
                    scalar.visit_columns(f);
                }
            }

            RelationExpr::Filter { input, predicates } => {
                input.visit_columns(f);
                for predicate in predicates {
                    predicate.visit_columns(f);
                }
            }
        }
    }

    /// Constructs a constant collection from specific rows and schema.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let rows = rows.into_iter().map(|row| Row::pack(row)).collect();
        RelationExpr::Constant { rows, typ }
    }
}

impl ScalarExpr {
    /// Rewrite `self` into a `dataflow_expr::ScalarExpr` which will be `Map`ped or `Filter`ed over `relation`.
    /// This requires removing all nested subqueries, which we can do moving them into `inner` using `RelationExpr::applied_to`.
    /// We expect that `relation` has already been decorrelated, so that:
    /// * the first `outer_arity` columns of `relation` hold values from the outer scope
    /// * the remaining columns of `relation` hold values from the inner scope (i.e., the direct input to `self`)
    fn applied_to(
        self,
        id_gen: &mut dataflow_expr::IdGen,
        outer_arity: usize,
        relation: &mut dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use dataflow_expr::ScalarExpr as SS;

        Ok(match self {
            Column(ColumnRef::Inner(column)) => {
                let column = outer_arity + column;
                SS::Column(column)
            }
            Column(ColumnRef::Outer(column)) => {
                assert!(column < outer_arity);
                SS::Column(column)
            }
            Literal(row, typ) => SS::Literal(row, typ),
            Parameter(_) => panic!("cannot decorrelate expression with unbound parameters"),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(id_gen, outer_arity, relation)?),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(id_gen, outer_arity, relation)?),
                expr2: Box::new(expr2.applied_to(id_gen, outer_arity, relation)?),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(id_gen, outer_arity, relation))
                    .collect::<Result<Vec<_>, failure::Error>>()?,
            },
            If { cond, then, els } => {
                // TODO(jamii) would be nice to only run subqueries in `then` when `cond` is true
                // (if subqueries later gain the ability to throw errors, this impacts correctness too)
                SS::If {
                    cond: Box::new(cond.applied_to(id_gen, outer_arity, relation)?),
                    then: Box::new(then.applied_to(id_gen, outer_arity, relation)?),
                    els: Box::new(els.applied_to(id_gen, outer_arity, relation)?),
                }
            }

            // Subqueries!
            // These are surprisingly subtle. Things to be careful of:

            // Anything in the subquery that cares about row counts (Reduce/Distinct/Negate/Threshold) must not:
            // * change the row counts of the outer query
            // * accidentally compute its own value using the row counts of the outer query
            // Use `branch` to calculate the subquery once for each __distinct__ key in the outer
            // query and then join the answers back on to the original rows of the outer query.

            // When the subquery would return 0 rows for some row in the outer query, `subquery.applied_to(get_inner)` will not have any corresponding row.
            // Use `lookup` if you need to add default values for cases when the subquery returns 0 rows.
            Exists(expr) => {
                *relation = branch(
                    id_gen,
                    relation.take_dangerous(),
                    *expr,
                    |id_gen, expr, get_relation| {
                        let exists = expr
                            // compute for every row in get_inner
                            .applied_to(id_gen, get_relation.clone())?
                            // throw away actual values and just remember whether or not there where __any__ rows
                            .distinct_by((0..get_relation.arity()).collect())
                            // Append true to anything that returned any rows. This
                            // join is logically equivalent to
                            // `.map(vec![Datum::True])`, but using a join allows
                            // for potential predicate pushdown and elision in the
                            // optimizer.
                            .product(dataflow_expr::RelationExpr::constant(
                                vec![vec![Datum::True]],
                                RelationType::new(vec![ColumnType::new(ScalarType::Bool)]),
                            ));
                        // append False to anything that didn't return any rows
                        let default = vec![(Datum::False, ColumnType::new(ScalarType::Bool))];
                        Ok(get_relation.lookup(id_gen, exists, default))
                    },
                )?;
                SS::Column(relation.arity() - 1)
            }
            Select(expr) => {
                *relation = branch(
                    id_gen,
                    relation.take_dangerous(),
                    *expr,
                    |id_gen, expr, get_relation| {
                        let select = expr
                            // compute for every row in get_relation
                            .applied_to(id_gen, get_relation.clone())?;
                        // append Null to anything that didn't return any rows
                        let default = vec![(Datum::Null, ColumnType::new(ScalarType::Null))];
                        Ok(get_relation.lookup(id_gen, select, default))
                    },
                )?;
                SS::Column(relation.arity() - 1)
            }
        })
    }

    /// Calls [`RelationExpr::split_subquery_predicates`] on any subqueries
    /// contained within this scalar expression.
    fn split_subquery_predicates(&mut self) {
        match self {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) | ScalarExpr::Parameter(_) => (),
            ScalarExpr::Exists(input) | ScalarExpr::Select(input) => {
                input.split_subquery_predicates()
            }
            ScalarExpr::CallUnary { expr, .. } => expr.split_subquery_predicates(),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.split_subquery_predicates();
                expr2.split_subquery_predicates();
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.split_subquery_predicates();
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.split_subquery_predicates();
                then.split_subquery_predicates();
                els.split_subquery_predicates();
            }
        }
    }

    /// Extracts subqueries from a conjuction into `out`.
    ///
    /// For example, given an expression like
    ///
    /// ```text
    /// a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e
    /// ```
    ///
    /// this function rewrites the expression to
    ///
    /// ```text
    /// a = b AND true AND c = d AND true
    /// ```
    ///
    /// and returns the expression fragments `EXISTS (<subquery 1>)` and
    //// `(<subquery 2>) = e` in the `out` vector.
    fn extract_conjucted_subqueries(&mut self, out: &mut Vec<ScalarExpr>) {
        fn contains_subquery(expr: &ScalarExpr) -> bool {
            match expr {
                ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) | ScalarExpr::Parameter(_) => {
                    false
                }
                ScalarExpr::Exists(_) | ScalarExpr::Select(_) => true,
                ScalarExpr::CallUnary { expr, .. } => contains_subquery(expr),
                ScalarExpr::CallBinary { expr1, expr2, .. } => {
                    contains_subquery(expr1) || contains_subquery(expr2)
                }
                ScalarExpr::CallVariadic { exprs, .. } => exprs.iter().any(contains_subquery),
                ScalarExpr::If { cond, then, els } => {
                    contains_subquery(cond) || contains_subquery(then) || contains_subquery(els)
                }
            }
        }

        match self {
            ScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1,
                expr2,
            } => {
                expr1.extract_conjucted_subqueries(out);
                expr2.extract_conjucted_subqueries(out);
            }
            expr => {
                if contains_subquery(expr) {
                    out.push(mem::replace(expr, ScalarExpr::literal_true()))
                }
            }
        }
    }

    /// Visits the column references in this scalar expression.
    fn visit_columns<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut ColumnRef),
    {
        match self {
            ScalarExpr::Literal(_, _) | ScalarExpr::Parameter(_) => (),
            ScalarExpr::Column(col_ref) => f(col_ref),
            ScalarExpr::CallUnary { expr, .. } => expr.visit_columns(f),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.visit_columns(f);
                expr2.visit_columns(f);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.visit_columns(f);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.visit_columns(f);
                then.visit_columns(f);
                els.visit_columns(f);
            }
            ScalarExpr::Exists(expr) | ScalarExpr::Select(expr) => {
                expr.visit_columns(f);
            }
        }
    }

    pub fn literal(datum: Datum, column_type: ColumnType) -> ScalarExpr {
        let row = Row::pack(&[datum]);
        ScalarExpr::Literal(row, column_type)
    }

    pub fn literal_true() -> ScalarExpr {
        ScalarExpr::literal(
            Datum::True,
            ColumnType {
                nullable: false,
                scalar_type: ScalarType::Bool,
            },
        )
    }

    pub fn literal_null() -> ScalarExpr {
        ScalarExpr::literal(
            Datum::Null,
            ColumnType {
                nullable: true,
                scalar_type: ScalarType::Null,
            },
        )
    }
}

/// Prepare to apply `inner` to `outer`. Note that `inner` is a correlated (SQL)
/// expression, while `outer` is a non-correlated (dataflow) expression. `inner`
/// will, in effect, be executed once for every distinct row in `outer`, and the
/// results will be joined with `outer`. Note that columns in `outer` that are
/// not depended upon by `inner` are thrown away before the distinct, so that we
/// don't perform needless computation of `inner`; column references in `inner`
/// are rewritten to account for these dropped columns.
///
/// The caller must supply the `apply` function that applies the rewritten
/// `inner` to `outer`.
fn branch<F>(
    id_gen: &mut dataflow_expr::IdGen,
    outer: dataflow_expr::RelationExpr,
    mut inner: RelationExpr,
    apply: F,
) -> Result<dataflow_expr::RelationExpr, failure::Error>
where
    F: FnOnce(
        &mut dataflow_expr::IdGen,
        RelationExpr,
        dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::RelationExpr, failure::Error>,
{
    let oa = outer.arity();

    // The key consists of the columns from the outer expression upon which the
    // inner relation depends. We discover these dependencies by walking the
    // inner relation expression and looking for outer column references.
    //
    // We don't consider outer column references that refer to indices that are
    // not yet available (i.e., indices greater than the arity of `outer`).
    // Those are the result of doubly-nested subqueries, and they'll be
    // incorporated in the key for the next recursive call to
    // `ScalarExpr::applied_to`.
    let mut outer_columns = HashSet::new();
    inner.visit_columns(&mut |col_ref| {
        if let ColumnRef::Outer(i) = col_ref {
            outer_columns.insert(*i);
        }
    });
    let mut permutation = HashMap::new();
    let mut key = vec![];
    for i in 0..oa {
        if outer_columns.contains(&i) {
            permutation.insert(i, key.len());
            key.push(i);
        }
    }
    let dropped = oa - key.len();
    inner.visit_columns(&mut |col_ref| {
        if let ColumnRef::Outer(i) = col_ref {
            if let Some(new_i) = permutation.get(i) {
                *i = *new_i;
            } else {
                *i -= dropped;
            }
        }
    });

    outer.let_in(id_gen, |id_gen, get_outer| {
        let keyed_outer = if key.is_empty() {
            // Don't depend on outer at all if the branch is not correlated,
            // which yields vastly better query plans. Note that this is a bit
            // weird in that the branch will be computed even if outer has no
            // rows, whereas if it had been correlated it would not (and *could*
            // not) have been computed if outer had no rows, but the callers of
            // this function don't mind these somewhat-weird semantics.
            dataflow_expr::RelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
        } else {
            get_outer.clone().distinct_by(key.clone())
        };
        keyed_outer.let_in(id_gen, |id_gen, get_keyed_outer| {
            let branch = apply(id_gen, inner, get_keyed_outer)?;
            let ba = branch.arity();
            let joined = dataflow_expr::RelationExpr::Join {
                inputs: vec![get_outer.clone(), branch],
                variables: key
                    .iter()
                    .enumerate()
                    .map(|(i, &k)| vec![(0, k), (1, i)])
                    .collect(),
            }
            // throw away the right-hand copy of the key we just joined on
            .project((0..oa).chain((oa + key.len())..(oa + ba)).collect());
            Ok(joined)
        })
    })
}

impl AggregateExpr {
    fn applied_to(
        self,
        id_gen: &mut dataflow_expr::IdGen,
        outer_arity: usize,
        inner: &mut dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::AggregateExpr, failure::Error> {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        Ok(dataflow_expr::AggregateExpr {
            func,
            expr: expr.applied_to(id_gen, outer_arity, inner)?,
            distinct,
        })
    }

    /// Visits the column references in this aggregate expression.
    fn visit_columns<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut ColumnRef),
    {
        self.expr.visit_columns(f);
    }
}

impl RelationExpr {
    pub fn typ(&self, outer: &RelationType, params: &BTreeMap<usize, ScalarType>) -> RelationType {
        match self {
            RelationExpr::Constant { typ, .. } => typ.clone(),
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ(outer, params);
                RelationType::new(outputs.iter().map(|&i| input_typ.column_types[i]).collect())
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ(outer, params);
                for scalar in scalars {
                    typ.column_types.push(scalar.typ(outer, &typ, params));
                }
                typ
            }
            RelationExpr::Filter { input, .. } | RelationExpr::TopK { input, .. } => {
                input.typ(outer, params)
            }
            RelationExpr::Join { left, right, .. } => RelationType::new(
                left.typ(outer, params)
                    .column_types
                    .into_iter()
                    .chain(right.typ(outer, params).column_types)
                    .collect(),
            ),
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ(outer, params);
                let mut column_types = group_key
                    .iter()
                    .map(|&i| input_typ.column_types[i])
                    .collect::<Vec<_>>();
                for agg in aggregates {
                    column_types.push(agg.typ(outer, &input_typ, params));
                }
                // TODO(frank): add primary key information.
                RelationType::new(column_types)
            }
            // TODO(frank): check for removal; add primary key information.
            RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => input.typ(outer, params),
            RelationExpr::Union { left, right } => {
                let left_typ = left.typ(outer, params);
                let right_typ = right.typ(outer, params);
                assert_eq!(left_typ.column_types.len(), right_typ.column_types.len());
                RelationType::new(
                    left_typ
                        .column_types
                        .iter()
                        .zip(right_typ.column_types.iter())
                        .map(|(l, r)| l.union(*r))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap(),
                )
            }
        }
    }

    pub fn project(self, outputs: Vec<usize>) -> Self {
        RelationExpr::Project {
            input: Box::new(self),
            outputs,
        }
    }

    pub fn map(self, scalars: Vec<ScalarExpr>) -> Self {
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
            left: Box::new(self),
            right: Box::new(right),
            on: ScalarExpr::literal_true(),
            kind: JoinKind::Inner,
        }
    }

    pub fn reduce(self, group_key: Vec<usize>, aggregates: Vec<AggregateExpr>) -> Self {
        RelationExpr::Reduce {
            input: Box::new(self),
            group_key,
            aggregates,
        }
    }

    #[allow(dead_code)]
    pub fn top_k(
        self,
        group_key: Vec<usize>,
        order_key: Vec<ColumnOrder>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        RelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
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

    pub fn exists(self) -> ScalarExpr {
        ScalarExpr::Exists(Box::new(self))
    }

    pub fn select(self) -> ScalarExpr {
        ScalarExpr::Select(Box::new(self))
    }
}

impl ScalarExpr {
    pub fn typ(
        &self,
        outer: &RelationType,
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        match self {
            ScalarExpr::Column(ColumnRef::Outer(i)) => outer.column_types[*i],
            ScalarExpr::Column(ColumnRef::Inner(i)) => inner.column_types[*i],
            ScalarExpr::Parameter(n) => ColumnType::new(params[&n]).nullable(true),
            ScalarExpr::Literal(_, typ) => *typ,
            ScalarExpr::CallUnary { expr, func } => {
                func.output_type(expr.typ(outer, inner, params))
            }
            ScalarExpr::CallBinary { expr1, expr2, func } => func.output_type(
                expr1.typ(outer, inner, params),
                expr2.typ(outer, inner, params),
            ),
            ScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(outer, inner, params)).collect())
            }
            ScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(outer, inner, params);
                let else_type = els.typ(outer, inner, params);
                then_type.union(else_type).unwrap()
            }
            ScalarExpr::Exists(_) => ColumnType::new(ScalarType::Bool).nullable(true),
            ScalarExpr::Select(expr) => expr
                .typ(
                    &RelationType::new(
                        outer
                            .column_types
                            .iter()
                            .cloned()
                            .chain(inner.column_types.iter().cloned())
                            .collect(),
                    ),
                    params,
                )
                .column_types
                .into_element()
                .nullable(true),
        }
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }
}

impl AggregateExpr {
    pub fn typ(
        &self,
        outer: &RelationType,
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        self.func.output_type(self.expr.typ(outer, inner, params))
    }
}
