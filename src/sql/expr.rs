// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;

use failure::ensure;

use expr as dataflow_expr;
use ore::collections::CollectionExt;
use repr::*;

use crate::Params;

// these happen to be unchanged at the moment, but there might be additions later
pub use dataflow_expr::like;
pub use dataflow_expr::{
    AggregateFunc, BinaryFunc, ColumnOrder, NullaryFunc, UnaryFunc, UnaryTableFunc, VariadicFunc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Just like dataflow_expr::RelationExpr, except where otherwise noted below
pub enum RelationExpr {
    Constant {
        rows: Vec<Row>,
        typ: RelationType,
    },
    Get {
        id: dataflow_expr::Id,
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
    FlatMapUnary {
        input: Box<RelationExpr>,
        func: UnaryTableFunc,
        expr: ScalarExpr,
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
    CallNullary(NullaryFunc),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ColumnRef {
    // scope level, where 0 is the current scope and 1+ are outer scopes
    pub level: usize,
    pub column: usize,
}

struct ColumnMap {
    inner: HashMap<ColumnRef, usize>,
}

impl ColumnMap {
    fn empty() -> ColumnMap {
        Self::new(HashMap::new())
    }

    fn new(inner: HashMap<ColumnRef, usize>) -> ColumnMap {
        ColumnMap { inner }
    }

    fn get(&self, col_ref: &ColumnRef) -> usize {
        if col_ref.level == 0 {
            self.inner.len() + col_ref.column
        } else {
            self.inner[col_ref]
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
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
                self.applied_to(id_gen, get_outer, &ColumnMap::empty())
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

            RelationExpr::FlatMapUnary {
                input,
                func: _,
                expr,
            } => {
                input.split_subquery_predicates();
                expr.split_subquery_predicates();
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
        col_map: &ColumnMap,
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
        ensure!(col_map.len() == get_outer.arity());
        match self {
            Constant { rows, typ } => Ok(get_outer.product(SR::Constant {
                rows: rows.into_iter().map(|row| (row, 1)).collect(),
                typ,
            })),
            Get { id, typ } => Ok(get_outer.product(SR::Get { id, typ })),
            Project { input, outputs } => {
                let input = input.applied_to(id_gen, get_outer.clone(), col_map)?;
                let outputs = (0..get_outer.arity())
                    .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                    .collect::<Vec<_>>();
                Ok(input.project(outputs))
            }
            Map { input, scalars } => {
                let mut input = input.applied_to(id_gen, get_outer, col_map)?;
                for scalar in scalars {
                    let old_arity = input.arity();
                    let scalar = scalar.applied_to(id_gen, col_map, &mut input)?;
                    let new_arity = input.arity();
                    input = input.map(vec![scalar]);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project((0..old_arity).chain(vec![new_arity]).collect());
                    }
                }
                Ok(input)
            }
            FlatMapUnary { input, func, expr } => {
                let mut input = input.applied_to(id_gen, get_outer, col_map)?;
                let old_arity = input.arity();
                let expr = expr.applied_to(id_gen, col_map, &mut input)?;
                let new_arity = input.arity();
                let output_arity = func.output_arity();
                input = input.flat_map_unary(func, expr);
                if old_arity != new_arity {
                    // this means we added some columns to handle subqueries, and now we need to get rid of them
                    input = input.project(
                        (0..old_arity)
                            .chain(new_arity..new_arity + output_arity)
                            .collect(),
                    );
                }
                Ok(input)
            }
            Filter { input, predicates } => {
                let mut input = input.applied_to(id_gen, get_outer, col_map)?;
                for predicate in predicates {
                    let old_arity = input.arity();
                    let predicate = predicate.applied_to(id_gen, col_map, &mut input)?;
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
                let left = left.applied_to(id_gen, get_outer.clone(), col_map)?;
                let lt = left.typ();
                let la = left.arity() - oa;
                left.let_in(id_gen, |id_gen, get_left| {
                    let right = right.applied_to(id_gen, get_outer.clone(), col_map)?;
                    let rt = right.typ();
                    let ra = right.arity() - oa;
                    right.let_in(id_gen, |id_gen, get_right| {
                        let mut product = SR::join(
                            vec![get_left.clone(), get_right.clone()],
                            (0..oa).map(|i| vec![(0, i), (1, i)]).collect(),
                        )
                        // project away the repeated copy of get_outer
                        .project(
                            (0..(oa + la))
                                .chain((oa + la + oa)..(oa + la + oa + ra))
                                .collect(),
                        );
                        let old_arity = product.arity();
                        let on = on.applied_to(id_gen, col_map, &mut product)?;
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
                                    rt.column_types
                                        .into_iter()
                                        .map(|typ| (Datum::Null, typ.nullable(true)))
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
                                            // need to swap left and right to make the anti_lookup work
                                            .project(
                                                (0..oa)
                                                    .chain((oa + la)..(oa + la + ra))
                                                    .chain((oa)..(oa + la))
                                                    .collect(),
                                            ),
                                        lt.column_types
                                            .into_iter()
                                            .map(|typ| (Datum::Null, typ.nullable(true)))
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
                let left = left.applied_to(id_gen, get_outer.clone(), col_map)?;
                let right = right.applied_to(id_gen, get_outer, col_map)?;
                Ok(left.union(right))
            }
            Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let mut input = input.applied_to(id_gen, get_outer.clone(), col_map)?;
                let applied_group_key = (0..get_outer.arity())
                    .chain(group_key.iter().map(|i| get_outer.arity() + i))
                    .collect();
                let applied_aggregates = aggregates
                    .into_iter()
                    .map(|aggregate| Ok(aggregate.applied_to(id_gen, col_map, &mut input)?))
                    .collect::<Result<Vec<_>, failure::Error>>()?;
                let input_type = input.typ();
                let default = applied_aggregates
                    .iter()
                    .map(|agg| (agg.func.default(), agg.typ(&input_type)))
                    .collect();
                // NOTE we don't need to remove any extra columns from aggregate.applied_to above because the reduce will do that anyway
                let mut reduced = input.reduce(applied_group_key, applied_aggregates);
                if group_key.is_empty() {
                    reduced = get_outer.lookup(id_gen, reduced, default);
                }
                Ok(reduced)
            }
            Distinct { input } => Ok(input.applied_to(id_gen, get_outer, col_map)?.distinct()),
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                let input = input.applied_to(id_gen, get_outer.clone(), col_map)?;
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
            Negate { input } => Ok(input.applied_to(id_gen, get_outer, col_map)?.negate()),
            Threshold { input } => Ok(input.applied_to(id_gen, get_outer, col_map)?.threshold()),
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e: &mut RelationExpr| e.visit_mut(f));
        f(self);
    }

    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::FlatMapUnary { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { left, right, .. } => {
                f(left);
                f(right);
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::Distinct { input } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => {
                f(input);
            }
            RelationExpr::Threshold { input } => {
                f(input);
            }
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
            }
        }
    }

    /// Visits the column references within this `RelationExpr`.
    fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        self.visit_mut(&mut |e| match e {
            RelationExpr::Join { on, .. } => on.visit_columns(depth, f),
            RelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    scalar.visit_columns(depth, f);
                }
            }
            RelationExpr::FlatMapUnary { expr, .. } => {
                expr.visit_columns(depth, f);
            }
            RelationExpr::Filter { predicates, .. } => {
                for predicate in predicates {
                    predicate.visit_columns(depth, f);
                }
            }
            RelationExpr::Reduce { aggregates, .. } => {
                for aggregate in aggregates {
                    aggregate.visit_columns(depth, f);
                }
            }
            RelationExpr::Constant { .. }
            | RelationExpr::Get { .. }
            | RelationExpr::Project { .. }
            | RelationExpr::Distinct { .. }
            | RelationExpr::TopK { .. }
            | RelationExpr::Negate { .. }
            | RelationExpr::Threshold { .. }
            | RelationExpr::Union { .. } => (),
        })
    }

    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `parameters`.
    pub fn bind_parameters(&mut self, parameters: &Params) {
        self.visit_mut(&mut |e| match e {
            RelationExpr::Join { on, .. } => on.bind_parameters(parameters),
            RelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    scalar.bind_parameters(parameters);
                }
            }
            RelationExpr::FlatMapUnary { expr, .. } => {
                expr.bind_parameters(parameters);
            }
            RelationExpr::Filter { predicates, .. } => {
                for predicate in predicates {
                    predicate.bind_parameters(parameters);
                }
            }
            RelationExpr::Reduce { aggregates, .. } => {
                for aggregate in aggregates {
                    aggregate.bind_parameters(parameters);
                }
            }
            RelationExpr::Constant { .. }
            | RelationExpr::Get { .. }
            | RelationExpr::Project { .. }
            | RelationExpr::Distinct { .. }
            | RelationExpr::TopK { .. }
            | RelationExpr::Negate { .. }
            | RelationExpr::Threshold { .. }
            | RelationExpr::Union { .. } => (),
        })
    }

    /// Constructs a constant collection from specific rows and schema.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let rows = rows.into_iter().map(|row| Row::pack(row)).collect();
        RelationExpr::Constant { rows, typ }
    }
}

impl ScalarExpr {
    /// Rewrite `self` into a `dataflow_expr::ScalarExpr` which will be `Map`ped or `Filter`ed over `inner`.
    /// This requires removing all nested subqueries, which we can do moving them into `inner` using `RelationExpr::applied_to`.
    /// We expect that `inner` has already been decorrelated, so that:
    /// * the first `outer_arities[0]` columns of `inner` hold values from the outermost scope
    /// * the next `outer_arities[1]` columns of `inner` hold values from the next outermost scope
    /// * ...etc
    /// * the remaining columns of `inner` hold values from the inner scope (i.e., the direct input to `self`)
    fn applied_to(
        self,
        id_gen: &mut dataflow_expr::IdGen,
        col_map: &ColumnMap,
        inner: &mut dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use dataflow_expr::ScalarExpr as SS;

        Ok(match self {
            Column(col_ref) => SS::Column(col_map.get(&col_ref)),
            Literal(row, typ) => SS::Literal(row, typ),
            Parameter(_) => panic!("cannot decorrelate expression with unbound parameters"),
            CallNullary(func) => SS::CallNullary(func),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(id_gen, col_map, inner)?),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(id_gen, col_map, inner)?),
                expr2: Box::new(expr2.applied_to(id_gen, col_map, inner)?),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(id_gen, col_map, inner))
                    .collect::<Result<Vec<_>, failure::Error>>()?,
            },
            If { cond, then, els } => {
                // TODO(jamii) would be nice to only run subqueries in `then` when `cond` is true
                // (if subqueries later gain the ability to throw errors, this impacts correctness too)
                SS::If {
                    cond: Box::new(cond.applied_to(id_gen, col_map, inner)?),
                    then: Box::new(then.applied_to(id_gen, col_map, inner)?),
                    els: Box::new(els.applied_to(id_gen, col_map, inner)?),
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
                *inner = branch(
                    id_gen,
                    inner.take_dangerous(),
                    col_map,
                    *expr,
                    |id_gen, expr, get_inner, col_map| {
                        let exists = expr
                            // compute for every row in get_inner
                            .applied_to(id_gen, get_inner.clone(), col_map)?
                            // throw away actual values and just remember whether or not there where __any__ rows
                            .distinct_by((0..get_inner.arity()).collect())
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
                        Ok(get_inner.lookup(id_gen, exists, default))
                    },
                )?;
                SS::Column(inner.arity() - 1)
            }
            Select(expr) => {
                *inner = branch(
                    id_gen,
                    inner.take_dangerous(),
                    col_map,
                    *expr,
                    |id_gen, expr, get_inner, col_map| {
                        let select = expr
                            // compute for every row in get_inner
                            .applied_to(id_gen, get_inner.clone(), col_map)?;
                        let col_type = select.typ().column_types.into_last();
                        // append Null to anything that didn't return any rows
                        let default = vec![(Datum::Null, col_type.nullable(true))];
                        Ok(get_inner.lookup(id_gen, select, default))
                    },
                )?;
                SS::Column(inner.arity() - 1)
            }
        })
    }

    /// Calls [`RelationExpr::split_subquery_predicates`] on any subqueries
    /// contained within this scalar expression.
    fn split_subquery_predicates(&mut self) {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::Parameter(_)
            | ScalarExpr::CallNullary(_) => (),
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
                ScalarExpr::Column(_)
                | ScalarExpr::Literal(_, _)
                | ScalarExpr::Parameter(_)
                | ScalarExpr::CallNullary(_) => false,
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
    fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        match self {
            ScalarExpr::Literal(_, _) | ScalarExpr::Parameter(_) | ScalarExpr::CallNullary(_) => (),
            ScalarExpr::Column(col_ref) => f(depth, col_ref),
            ScalarExpr::CallUnary { expr, .. } => expr.visit_columns(depth, f),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.visit_columns(depth, f);
                expr2.visit_columns(depth, f);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.visit_columns(depth, f);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.visit_columns(depth, f);
                then.visit_columns(depth, f);
                els.visit_columns(depth, f);
            }
            ScalarExpr::Exists(expr) | ScalarExpr::Select(expr) => {
                expr.visit_columns(depth + 1, f);
            }
        }
    }

    /// Replaces any parameter references in the expression with the
    /// corresponding datum in `parameters`.
    pub fn bind_parameters(&mut self, parameters: &Params) {
        match self {
            ScalarExpr::Literal(_, _) | ScalarExpr::Column(_) | ScalarExpr::CallNullary(_) => (),
            ScalarExpr::Parameter(n) => {
                let datum = parameters.datums.iter().nth(*n - 1).unwrap();
                let scalar_type = &parameters.types[*n - 1];
                let row = Row::pack(&[datum]);
                let column_type = ColumnType::new(scalar_type.clone()).nullable(datum.is_null());
                mem::replace(self, ScalarExpr::Literal(row, column_type));
            }
            ScalarExpr::CallUnary { expr, .. } => expr.bind_parameters(parameters),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.bind_parameters(parameters);
                expr2.bind_parameters(parameters);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.bind_parameters(parameters);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.bind_parameters(parameters);
                then.bind_parameters(parameters);
                els.bind_parameters(parameters);
            }
            ScalarExpr::Exists(expr) | ScalarExpr::Select(expr) => {
                expr.bind_parameters(parameters);
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

    pub fn literal_null(scalar_type: ScalarType) -> ScalarExpr {
        ScalarExpr::literal(
            Datum::Null,
            ColumnType {
                nullable: true,
                scalar_type,
            },
        )
    }

    /// Rewrite `self` into a `dataflow_expr::ScalarExpr`.
    /// Assumes there are no subqueries in need of decorrelating
    pub fn lower_uncorrelated(self) -> dataflow_expr::ScalarExpr {
        use self::ScalarExpr::*;
        use dataflow_expr::ScalarExpr as SS;

        match self {
            Column(ColumnRef { level: 0, column }) => SS::Column(column),
            Literal(datum, typ) => SS::Literal(datum, typ),
            CallNullary(func) => SS::CallNullary(func),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.lower_uncorrelated()),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.lower_uncorrelated()),
                expr2: Box::new(expr2.lower_uncorrelated()),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.lower_uncorrelated())
                    .collect(),
            },
            If { cond, then, els } => SS::If {
                cond: Box::new(cond.lower_uncorrelated()),
                then: Box::new(then.lower_uncorrelated()),
                els: Box::new(els.lower_uncorrelated()),
            },
            Select { .. } | Exists { .. } | Parameter(..) | Column(..) => {
                panic!("unexpected ScalarExpr in index plan: {:?}", self)
            }
        }
    }
}

/// Prepare to apply `inner` to `outer`. Note that `inner` is a correlated (SQL)
/// expression, while `outer` is a non-correlated (dataflow) expression. `inner`
/// will, in effect, be executed once for every distinct row in `outer`, and the
/// results will be joined with `outer`. Note that columns in `outer` that are
/// not depended upon by `inner` are thrown away before the distinct, so that we
/// don't perform needless computation of `inner`.
///
/// The caller must supply the `apply` function that applies the rewritten
/// `inner` to `outer`.
fn branch<F>(
    id_gen: &mut dataflow_expr::IdGen,
    outer: dataflow_expr::RelationExpr,
    col_map: &ColumnMap,
    mut inner: RelationExpr,
    apply: F,
) -> Result<dataflow_expr::RelationExpr, failure::Error>
where
    F: FnOnce(
        &mut dataflow_expr::IdGen,
        RelationExpr,
        dataflow_expr::RelationExpr,
        &ColumnMap,
    ) -> Result<dataflow_expr::RelationExpr, failure::Error>,
{
    // The key consists of the columns from the outer expression upon which the
    // inner relation depends. We discover these dependencies by walking the
    // inner relation expression and looking for column references whose level
    // escapes inner.
    //
    // At the end of this process, `key` contains the decorrelated position of
    // each outer column, according to the passed-in `col_map`, and
    // `new_col_map` maps each outer column to its new ordinal position in key.
    let mut outer_cols = BTreeSet::new();
    inner.visit_columns(0, &mut |depth, col| {
        if col.level > depth {
            outer_cols.insert(ColumnRef {
                level: col.level - depth,
                ..*col
            });
        }
    });
    let mut new_col_map = HashMap::new();
    let mut key = vec![];
    for col in outer_cols {
        new_col_map.insert(col, key.len());
        key.push(col_map.get(&ColumnRef {
            level: col.level - 1,
            ..col
        }));
    }

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
            let oa = get_outer.arity();
            let branch = apply(id_gen, inner, get_keyed_outer, &ColumnMap::new(new_col_map))?;
            let ba = branch.arity();
            let joined = dataflow_expr::RelationExpr::join(
                vec![get_outer.clone(), branch],
                key.iter()
                    .enumerate()
                    .map(|(i, &k)| vec![(0, k), (1, i)])
                    .collect(),
            )
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
        col_map: &ColumnMap,
        inner: &mut dataflow_expr::RelationExpr,
    ) -> Result<dataflow_expr::AggregateExpr, failure::Error> {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        Ok(dataflow_expr::AggregateExpr {
            func,
            expr: expr.applied_to(id_gen, col_map, inner)?,
            distinct,
        })
    }

    /// Visits the column references in this aggregate expression.
    fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        self.expr.visit_columns(depth, f);
    }

    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `parameters`.
    pub fn bind_parameters(&mut self, parameters: &Params) {
        self.expr.bind_parameters(parameters);
    }
}

impl RelationExpr {
    pub fn typ(
        &self,
        outers: &[RelationType],
        params: &BTreeMap<usize, ScalarType>,
    ) -> RelationType {
        match self {
            RelationExpr::Constant { typ, .. } => typ.clone(),
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ(outers, params);
                RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                )
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ(outers, params);
                for scalar in scalars {
                    typ.column_types.push(scalar.typ(outers, &typ, params));
                }
                typ
            }
            RelationExpr::FlatMapUnary { input, func, expr } => {
                let mut typ = input.typ(outers, params);
                let func_typ = func.output_type(&expr.typ(outers, &typ, params));
                typ.column_types.extend(func_typ.column_types);
                // FlatMap can add duplicate rows, so input keys are no longer valid
                RelationType::new(typ.column_types)
            }
            RelationExpr::Filter { input, .. } | RelationExpr::TopK { input, .. } => {
                input.typ(outers, params)
            }
            RelationExpr::Join { left, right, .. } => RelationType::new(
                left.typ(outers, params)
                    .column_types
                    .into_iter()
                    .chain(right.typ(outers, params).column_types)
                    .collect(),
            ),
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ(outers, params);
                let mut column_types = group_key
                    .iter()
                    .map(|&i| input_typ.column_types[i].clone())
                    .collect::<Vec<_>>();
                for agg in aggregates {
                    column_types.push(agg.typ(outers, &input_typ, params));
                }
                // TODO(frank): add primary key information.
                RelationType::new(column_types)
            }
            // TODO(frank): check for removal; add primary key information.
            RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => input.typ(outers, params),
            RelationExpr::Union { left, right } => {
                let left_typ = left.typ(outers, params);
                let right_typ = right.typ(outers, params);
                assert_eq!(left_typ.column_types.len(), right_typ.column_types.len());
                RelationType::new(
                    left_typ
                        .column_types
                        .iter()
                        .zip(right_typ.column_types.iter())
                        .map(|(l, r)| l.union(r))
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
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        match self {
            ScalarExpr::Column(ColumnRef { level, column }) => {
                if *level == 0 {
                    inner.column_types[*column].clone()
                } else {
                    outers[outers.len() - *level].column_types[*column].clone()
                }
            }
            ScalarExpr::Parameter(n) => ColumnType::new(params[&n].clone()).nullable(true),
            ScalarExpr::Literal(_, typ) => typ.clone(),
            ScalarExpr::CallNullary(func) => func.output_type(),
            ScalarExpr::CallUnary { expr, func } => {
                func.output_type(expr.typ(outers, inner, params))
            }
            ScalarExpr::CallBinary { expr1, expr2, func } => func.output_type(
                expr1.typ(outers, inner, params),
                expr2.typ(outers, inner, params),
            ),
            ScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(outers, inner, params)).collect())
            }
            ScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(outers, inner, params);
                let else_type = els.typ(outers, inner, params);
                then_type.union(&else_type).unwrap()
            }
            ScalarExpr::Exists(_) => ColumnType::new(ScalarType::Bool).nullable(true),
            ScalarExpr::Select(expr) => {
                let mut outers = outers.to_vec();
                outers.push(inner.clone());
                expr.typ(&outers, params)
                    .column_types
                    .into_element()
                    .nullable(true)
            }
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
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        self.func.output_type(self.expr.typ(outers, inner, params))
    }
}
