// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use repr::*;
use uuid::Uuid;

// these happen to be unchanged at the moment, but there might be additions later
pub use super::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};

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
        include_left_outer: bool,
        include_right_outer: bool,
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
    /// Return the union of two dataflows
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
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<ScalarExpr>,
    pub distinct: bool,
}

impl super::RelationExpr {
    fn let_<Body>(self, body: Body) -> Result<super::RelationExpr, failure::Error>
    where
        Body: FnOnce(super::RelationExpr) -> Result<super::RelationExpr, failure::Error>,
    {
        if let super::RelationExpr::Get { .. } = self {
            // already done
            body(self)
        } else {
            let name = format!("tmp_{}", Uuid::new_v4());
            let get = super::RelationExpr::Get {
                name: name.clone(),
                typ: self.typ(),
            };
            let body = (body)(get)?;
            Ok(super::RelationExpr::Let {
                name,
                value: Box::new(self),
                body: Box::new(body),
            })
        }
    }

    fn branch<Branch>(
        self,
        default: Option<Vec<(Datum, ColumnType)>>,
        branch: Branch,
    ) -> Result<super::RelationExpr, failure::Error>
    where
        Branch: FnOnce(super::RelationExpr) -> Result<super::RelationExpr, failure::Error>,
    {
        // TODO(jamii) can optimize this by looking at what columns `branch` uses
        let key = (0..self.arity()).collect::<Vec<_>>();
        self.let_(|get_outer| {
            let keyed_outer = get_outer.clone().project(key.clone()).distinct();
            keyed_outer.let_(|get_keyed_outer| {
                let branch = branch(get_keyed_outer.clone())?;
                branch.let_(|get_branch| {
                    let with_default = if let Some(default) = default {
                        let missing_keys = get_branch
                            .clone()
                            .project((0..key.len()).collect())
                            .distinct()
                            .negate();
                        let default_keys = get_keyed_outer.clone().union(missing_keys).map(
                            default
                                .iter()
                                .map(|(datum, typ)| {
                                    (super::ScalarExpr::Literal(datum.clone()), typ.clone())
                                })
                                .collect(),
                        );
                        super::RelationExpr::Union {
                            left: Box::new(get_branch.clone()),
                            right: Box::new(default_keys),
                        }
                    } else {
                        get_branch.clone()
                    };
                    let joined = super::RelationExpr::Join {
                        inputs: vec![get_outer.clone(), with_default],
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

impl RelationExpr {
    // TODO(jamii) this can't actually return an error atm - do we still need Result?
    pub fn decorrelate(self) -> Result<super::RelationExpr, failure::Error> {
        self.applied_to(super::RelationExpr::Constant {
            rows: vec![vec![]],
            typ: RelationType {
                column_types: vec![],
            },
        })
    }

    // TODO(jamii) ensure outer is always distinct? or make distinct during reduce etc?
    fn applied_to(self, outer: super::RelationExpr) -> Result<super::RelationExpr, failure::Error> {
        use self::RelationExpr::*;
        use super::RelationExpr as SR;
        use super::ScalarExpr as SS;
        match self {
            Constant { rows, typ } => Ok(outer.product(SR::Constant { rows, typ })),
            Get { name, typ } => Ok(outer.product(SR::Get { name, typ })),
            Project { input, outputs } => {
                let outer_arity = outer.arity();
                let input = input.applied_to(outer)?;
                let outputs = (0..outer_arity)
                    .chain(outputs.into_iter().map(|i| outer_arity + i))
                    .collect::<Vec<_>>();
                Ok(input.project(outputs))
            }
            Map { input, scalars } => {
                let outer_arity = outer.arity();
                let mut input = input.applied_to(outer)?;
                for (scalar, typ) in scalars {
                    let old_arity = input.arity();
                    let scalar = scalar.applied_to(outer_arity, &mut input)?;
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
                let outer_arity = outer.arity();
                let mut input = input.applied_to(outer)?;
                for predicate in predicates {
                    let old_arity = input.arity();
                    let predicate = predicate.applied_to(outer_arity, &mut input)?;
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
                include_left_outer,
                include_right_outer,
            } => {
                outer.let_(|get_outer| {
                    let left = left.applied_to(get_outer.clone())?;
                    left.let_(|get_left| {
                        let right = right.applied_to(get_outer.clone())?;
                        right.let_(|get_right| {
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
                            join.let_(|get_join| {
                                let mut result = get_join.clone();
                                if include_left_outer {
                                    let left_outer = get_left
                                        .clone()
                                        .union(
                                            get_join
                                                .clone()
                                                .project(
                                                    (0..get_outer.arity() + get_left.arity())
                                                        .collect(),
                                                )
                                                .distinct()
                                                .negate(),
                                        )
                                        .map(
                                            (0..get_right.arity())
                                                .map(|_| {
                                                    (
                                                        SS::Literal(Datum::Null),
                                                        ColumnType::new(ScalarType::Null),
                                                    )
                                                })
                                                .collect(),
                                        );
                                    result = result.union(left_outer);
                                }
                                if include_right_outer {
                                    let right_outer = get_right
                                        .clone()
                                        .union(
                                            get_join
                                                .clone()
                                                .project(
                                                    (0..get_outer.arity())
                                                        .chain(
                                                            get_outer.arity() + get_left.arity()
                                                                ..get_outer.arity()
                                                                    + get_left.arity()
                                                                    + get_right.arity(),
                                                        )
                                                        .collect(),
                                                )
                                                .distinct()
                                                .negate(),
                                        )
                                        .map(
                                            (0..get_left.arity())
                                                .map(|_| {
                                                    (
                                                        SS::Literal(Datum::Null),
                                                        ColumnType::new(ScalarType::Null),
                                                    )
                                                })
                                                .collect(),
                                        )
                                        .project(
                                            (0..get_outer.arity())
                                                .chain(
                                                    get_outer.arity() + get_right.arity()
                                                        ..get_outer.arity()
                                                            + get_right.arity()
                                                            + get_left.arity(),
                                                )
                                                .chain(
                                                    get_outer.arity()
                                                        ..get_outer.arity() + get_right.arity(),
                                                )
                                                .collect(),
                                        );
                                    result = result.union(right_outer);
                                }
                                Ok(result)
                            })
                        })
                    })
                })
            }
            Union { left, right } => outer.branch(None, |get_outer| {
                let left = left.applied_to(get_outer.clone())?;
                let right = right.applied_to(get_outer.clone())?;
                Ok(left.union(right))
            }),
            Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let default = if group_key.is_empty() {
                    Some(
                        aggregates
                            .iter()
                            .map(|(aggregate, _)| {
                                let (datum, scalar_type) = aggregate.func.default();
                                let nullable = &datum == &Datum::Null;
                                (
                                    datum,
                                    ColumnType {
                                        name: None,
                                        nullable,
                                        scalar_type,
                                    },
                                )
                            })
                            .collect(),
                    )
                } else {
                    None
                };
                outer.branch(default, |get_outer| {
                    let mut input = input.applied_to(get_outer.clone())?;
                    let group_key = (0..get_outer.arity())
                        .chain(group_key.iter().map(|i| get_outer.arity() + i))
                        .collect();
                    let aggregates = aggregates
                        .into_iter()
                        // TODO(jamii) how do we deal with the extra columns here?
                        .map(|(aggregate, typ)| {
                            Ok((aggregate.applied_to(get_outer.arity(), &mut input)?, typ))
                        })
                        .collect::<Result<Vec<_>, failure::Error>>()?;
                    Ok(input.reduce(group_key, aggregates))
                })
            }
            Distinct { input } => outer.branch(None, |get_outer| {
                Ok(input.applied_to(get_outer)?.distinct())
            }),
            // TODO(jamii) not sure about Negate/Threshold
            Negate { input } => Ok(input.applied_to(outer)?.negate()),
            Threshold { input } => {
                // assumes outer doesn't have any negative counts, which is probably safe?
                Ok(input.applied_to(outer)?.threshold())
            }
        }
    }
}

impl ScalarExpr {
    fn applied_to(
        self,
        outer_arity: usize,
        inner: &mut super::RelationExpr,
    ) -> Result<super::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use super::ScalarExpr as SS;

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
                // TODO(jamii) can optimize this only projecting columns of inner that are used in expr
                let default = Some(vec![(
                    Datum::False,
                    ColumnType {
                        name: None,
                        nullable: false,
                        scalar_type: ScalarType::Bool,
                    },
                )]);
                *inner = inner.clone().branch(default, |get_inner| {
                    Ok(expr
                        .applied_to(get_inner.clone())?
                        .project((0..get_inner.arity()).collect())
                        .map(vec![(
                            SS::Literal(Datum::True),
                            ColumnType {
                                name: None,
                                scalar_type: ScalarType::Bool,
                                nullable: false,
                            },
                        )]))
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
        inner: &mut super::RelationExpr,
    ) -> Result<super::AggregateExpr, failure::Error> {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        Ok(super::AggregateExpr {
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
            left: Box::new(self),
            right: Box::new(right),
            on: ScalarExpr::Literal(Datum::True),
            include_left_outer: false,
            include_right_outer: false,
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
    pub fn columns(is: &[usize]) -> Vec<ScalarExpr> {
        is.iter()
            .map(|i| ScalarExpr::Column(ColumnRef::Inner(*i)))
            .collect()
    }

    pub fn column(column: usize) -> Self {
        ScalarExpr::Column(ColumnRef::Inner(column))
    }

    pub fn literal(datum: Datum) -> Self {
        ScalarExpr::Literal(datum)
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

    pub fn if_then_else(self, t: Self, f: Self) -> Self {
        ScalarExpr::If {
            cond: Box::new(self),
            then: Box::new(t),
            els: Box::new(f),
        }
    }
}
