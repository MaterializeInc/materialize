// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use expr as dataflow_expr;
use repr::*;

// these happen to be unchanged at the moment, but there might be additions later
pub use dataflow_expr::like;
pub use dataflow_expr::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};

#[derive(Debug, Clone)]
pub enum RelationExpr {
    Constant {
        rows: Vec<Vec<Datum>>,
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
        scalars: Vec<(ScalarExpr, ColumnType)>,
    },
    Filter {
        input: Box<RelationExpr>,
        predicates: Vec<ScalarExpr>,
    },
    Join {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
        on: ScalarExpr,
        kind: JoinKind,
    },
    /// Group input by key
    /// SPECIAL CASE: when key is empty and input is empty, return a single row with aggregates over empty groups
    Reduce {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        aggregates: Vec<(AggregateExpr, ColumnType)>,
    },
    Distinct {
        input: Box<RelationExpr>,
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

#[derive(Debug, Clone)]
pub enum ScalarExpr {
    Column(ColumnRef),
    Literal(Datum),
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
    /// Returns true if the `RelationExpr` returns any rows
    Exists(Box<RelationExpr>),
}

#[derive(Debug, Clone)]
pub enum ColumnRef {
    /// References a variable from the input relation
    Inner(usize),
    /// References a variable from the outer scope
    Outer(usize),
}

#[derive(Debug, Clone)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<ScalarExpr>,
    pub distinct: bool,
}

impl RelationExpr {
    // TODO(jamii) this can't actually return an error atm - do we still need Result?
    /// Rewrite `self` into a `dataflow_expr::RelationExpr` which has no correlated subqueries
    pub fn decorrelate(self) -> Result<dataflow_expr::RelationExpr, failure::Error> {
        dataflow_expr::RelationExpr::Constant {
            rows: vec![vec![]],
            typ: RelationType {
                column_types: vec![],
            },
        }
        .let_in(|get_outer| self.applied_to(get_outer))
    }

    /// Evaluate `self` for each row in `outer`.
    /// (`get_outer` should be a `Get` with no duplicate rows)
    fn applied_to(
        self,
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
            Constant { rows, typ } => Ok(get_outer.product(SR::Constant { rows, typ })),
            Get { name, typ } => Ok(get_outer.product(SR::Get { name, typ })),
            Project { input, outputs } => {
                let input = input.applied_to(get_outer.clone())?;
                let outputs = (0..get_outer.arity())
                    .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                    .collect::<Vec<_>>();
                Ok(input.project(outputs))
            }
            Map { input, scalars } => {
                let mut input = input.applied_to(get_outer.clone())?;
                for (scalar, typ) in scalars {
                    let old_arity = input.arity();
                    let scalar = scalar.applied_to(get_outer.arity(), &mut input)?;
                    let new_arity = input.arity();
                    input = input.map(vec![(scalar, typ)]);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project((0..old_arity).chain(vec![new_arity]).collect());
                    }
                }
                Ok(input)
            }
            Filter { input, predicates } => {
                let mut input = input.applied_to(get_outer.clone())?;
                for predicate in predicates {
                    let old_arity = input.arity();
                    let predicate = predicate.applied_to(get_outer.arity(), &mut input)?;
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
                let left = left.applied_to(get_outer.clone())?;
                left.let_in(|get_left| {
                    let right = right.applied_to(get_outer.clone())?;
                    right.let_in(|get_right| {
                        let mut product = SR::Join {
                            inputs: vec![get_left.clone(), get_right.clone()],
                            variables: (0..get_outer.arity())
                                .map(|i| vec![(0, i), (1, i)])
                                .collect(),
                        };
                        let old_arity = product.arity();
                        let on = on.applied_to(get_outer.arity(), &mut product)?;
                        let mut join = product.filter(vec![on]);
                        let new_arity = join.arity();
                        if old_arity != new_arity {
                            // this means we added some columns to handle subqueries, and now we need to get rid of them
                            join = join.project((0..old_arity).collect());
                        }
                        join.let_in(|get_join| {
                            let mut result = get_join.clone();
                            let oa = get_outer.arity();
                            let la = get_left.arity();
                            let ra = get_right.arity();
                            if let JoinKind::LeftOuter | JoinKind::FullOuter = kind {
                                let left_outer = get_left.clone().anti_lookup(
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
                let left = left.applied_to(get_outer.clone())?;
                let right = right.applied_to(get_outer.clone())?;
                Ok(left.union(right))
            }
            Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let mut input = input.applied_to(get_outer.clone())?;
                let applied_group_key = (0..get_outer.arity())
                    .chain(group_key.iter().map(|i| get_outer.arity() + i))
                    .collect();
                let applied_aggregates = aggregates
                    .clone()
                    .into_iter()
                    // TODO(jamii) how do we deal with the extra columns here?
                    .map(|(aggregate, typ)| {
                        Ok((aggregate.applied_to(get_outer.arity(), &mut input)?, typ))
                    })
                    .collect::<Result<Vec<_>, failure::Error>>()?;
                let mut reduced = input.reduce(applied_group_key, applied_aggregates);
                if group_key.is_empty() {
                    let default = aggregates
                        .iter()
                        .map(|(aggregate, _)| {
                            let (datum, scalar_type) = aggregate.func.default();
                            let nullable = datum == Datum::Null;
                            (
                                datum,
                                ColumnType {
                                    name: None,
                                    nullable,
                                    scalar_type,
                                },
                            )
                        })
                        .collect();
                    reduced = get_outer.lookup(reduced, default);
                }
                Ok(reduced)
            }
            Distinct { input } => Ok(input.applied_to(get_outer)?.distinct()),
            Negate { input } => Ok(input.applied_to(get_outer)?.negate()),
            Threshold { input } => Ok(input.applied_to(get_outer)?.threshold()),
        }
    }
}

impl ScalarExpr {
    /// Evaluate `self` for every row in `inner`
    /// The first `outer_arity` columns of `inner` hold values from the outer query
    /// The remaining columns of `inner` hold values from the current query
    fn applied_to(
        self,
        outer_arity: usize,
        inner: &mut dataflow_expr::RelationExpr,
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
            Literal(datum) => SS::Literal(datum),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(outer_arity, inner)?),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(outer_arity, inner)?),
                expr2: Box::new(expr2.applied_to(outer_arity, inner)?),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(outer_arity, inner))
                    .collect::<Result<Vec<_>, failure::Error>>()?,
            },
            If { cond, then, els } => {
                // TODO(jamii) would be nice to only run subqueries in `then` when `cond` is true
                // (if subqueries can throw errors, this impacts correctness too)
                SS::If {
                    cond: Box::new(cond.applied_to(outer_arity, inner)?),
                    then: Box::new(then.applied_to(outer_arity, inner)?),
                    els: Box::new(els.applied_to(outer_arity, inner)?),
                }
            }
            Exists(expr) => {
                *inner = inner.take().branch(|get_inner| {
                    let exists = expr
                        .applied_to(get_inner.clone())?
                        .project((0..get_inner.arity()).collect())
                        .distinct()
                        .map(vec![(
                            SS::Literal(Datum::True),
                            ColumnType {
                                name: None,
                                scalar_type: ScalarType::Bool,
                                nullable: false,
                            },
                        )]);
                    let default = vec![(
                        Datum::False,
                        ColumnType {
                            name: None,
                            nullable: false,
                            scalar_type: ScalarType::Bool,
                        },
                    )];
                    Ok(get_inner.lookup(exists, default))
                })?;
                SS::Column(inner.arity() - 1)
            }
        })
    }
}

impl AggregateExpr {
    fn applied_to(
        self,
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
            expr: expr.applied_to(outer_arity, inner)?,
            distinct,
        })
    }
}

impl RelationExpr {
    pub fn typ(&self) -> RelationType {
        match self {
            RelationExpr::Constant { typ, .. } => typ.clone(),
            RelationExpr::Get { typ, .. } => typ.clone(),
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
            RelationExpr::Join { left, right, .. } => RelationType {
                column_types: left
                    .typ()
                    .column_types
                    .into_iter()
                    .chain(right.typ().column_types)
                    .collect(),
            },
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
            RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => input.typ(),
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

    #[allow(dead_code)]
    pub fn arity(&self) -> usize {
        self.typ().column_types.len()
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
            left: Box::new(self),
            right: Box::new(right),
            on: ScalarExpr::Literal(Datum::True),
            kind: JoinKind::Inner,
        }
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
}

impl ScalarExpr {
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
