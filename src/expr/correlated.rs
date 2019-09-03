// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use repr::*;
use std::collections::HashSet;
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
    Union {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
    },
}

#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// A negative column number references a variable from the outer scope
    Column(isize),
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
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<ScalarExpr>,
    pub distinct: bool,
}

impl RelationExpr {
    // TODO(jamii) use offset fix from ScalarExpr::applied_to
    fn used_outer_columns(&self) -> HashSet<isize> {
        let mut used_outer_columns = HashSet::new();
        fn visit_relation(
            expr: &RelationExpr,
            num_outer_columns: usize,
            used_outer_columns: &mut HashSet<isize>,
        ) {
            use RelationExpr::*;
            match expr {
                Constant { .. } => (),
                Get { .. } => (),
                Project { input, .. } => {
                    visit_relation(&*input, num_outer_columns, used_outer_columns);
                }
                Map { input, scalars } => {
                    visit_relation(&*input, num_outer_columns, used_outer_columns);
                    let num_outer_columns = num_outer_columns + input.arity();
                    for (i, (scalar, _)) in scalars.iter().enumerate() {
                        visit_scalar(scalar, num_outer_columns + i, used_outer_columns);
                    }
                }
                Filter { input, predicates } => {
                    let num_outer_columns = num_outer_columns + input.arity();
                    for predicate in predicates {
                        visit_scalar(predicate, num_outer_columns, used_outer_columns);
                    }
                }
                Join {
                    left, right, on, ..
                } => {
                    visit_relation(&*left, num_outer_columns, used_outer_columns);
                    visit_relation(&*right, num_outer_columns, used_outer_columns);
                    let num_outer_columns = num_outer_columns + left.arity() + right.arity();
                    visit_scalar(on, num_outer_columns, used_outer_columns);
                }
                Reduce {
                    input,
                    group_key,
                    aggregates,
                } => {
                    visit_relation(&*input, num_outer_columns, used_outer_columns);
                    let num_outer_columns = num_outer_columns + group_key.len();
                    for (i, (aggregate, _)) in aggregates.iter().enumerate() {
                        visit_scalar(&*aggregate.expr, num_outer_columns + i, used_outer_columns)
                    }
                }
                Distinct { input } => {
                    visit_relation(&*input, num_outer_columns, used_outer_columns);
                }
                Union { left, right } => {
                    visit_relation(&*left, num_outer_columns, used_outer_columns);
                    visit_relation(&*right, num_outer_columns, used_outer_columns);
                }
            }
        }
        fn visit_scalar(
            expr: &ScalarExpr,
            num_outer_columns: usize,
            used_outer_columns: &mut HashSet<isize>,
        ) {
            use ScalarExpr::*;
            match expr {
                Column(column) => {
                    let column = column + (num_outer_columns as isize);
                    if column < 0 {
                        used_outer_columns.insert(column);
                    }
                }
                Literal(_) => (),
                CallUnary { expr, .. } => {
                    visit_scalar(expr, num_outer_columns, used_outer_columns);
                }
                CallBinary { expr1, expr2, .. } => {
                    visit_scalar(expr1, num_outer_columns, used_outer_columns);
                    visit_scalar(expr2, num_outer_columns, used_outer_columns);
                }
                CallVariadic { exprs, .. } => {
                    for expr in exprs {
                        visit_scalar(expr, num_outer_columns, used_outer_columns);
                    }
                }
                If { cond, then, els } => {
                    visit_scalar(&*cond, num_outer_columns, used_outer_columns);
                    visit_scalar(&*then, num_outer_columns, used_outer_columns);
                    visit_scalar(&*els, num_outer_columns, used_outer_columns);
                }
                Exists(expr) => {
                    visit_relation(expr, num_outer_columns, used_outer_columns);
                }
            }
        }
        visit_relation(self, 0, &mut used_outer_columns);
        used_outer_columns
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
        Ok(match self {
            Constant { rows, typ } => outer.product(SR::Constant { rows, typ }),
            Get { name, typ } => outer.product(SR::Get { name, typ }),
            Project { input, outputs } => {
                let outer_arity = outer.arity();
                let input = input.applied_to(outer)?;
                let outputs = (0..outer_arity)
                    .chain(outputs.into_iter().map(|i| outer_arity + i))
                    .collect::<Vec<_>>();
                SR::Project {
                    input: Box::new(input),
                    outputs,
                }
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
                input
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
                input
            }
            Join {
                left,
                right,
                on,
                include_left_outer,
                include_right_outer,
            } => {
                if include_left_outer || include_right_outer {
                    // TODO(jamii) handle these with Branch
                    unimplemented!();
                }
                let outer_arity = outer.arity();
                let left = left.applied_to(outer)?;
                let mut product = right.applied_to(left)?;
                let old_arity = product.arity();
                let on = on.applied_to(outer_arity, &mut product)?;
                let new_arity = product.arity();
                let mut join = product.filter(vec![on]);
                if old_arity != new_arity {
                    // this means we added some columns to handle subqueries, and now we need to get rid of them
                    join = join.project((0..old_arity).collect());
                }
                join
            }
            Union { left, right } => {
                let outer_name = format!("outer_{}", Uuid::new_v4());
                let get_outer = SR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let left = left.applied_to(get_outer.clone())?;
                let right = right.applied_to(get_outer)?;
                SR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(SR::Union {
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                    default: None,
                }
            }
            Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let outer_name = format!("outer_{}", Uuid::new_v4());
                let get_outer = SR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let mut input = input.applied_to(get_outer)?;
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
                let group_key = (0..outer_arity)
                    .chain(group_key.iter().map(|i| outer_arity + i))
                    .collect();
                let aggregates = aggregates
                    .into_iter()
                    // TODO(jamii) how do we deal with the extra columns here?
                    .map(|(aggregate, typ)| {
                        Ok((aggregate.applied_to(outer_arity, &mut input)?, typ))
                    })
                    .collect::<Result<Vec<_>, failure::Error>>()?;
                SR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(SR::Reduce {
                        input: Box::new(input),
                        group_key,
                        aggregates,
                    }),
                    default,
                }
            }
            Distinct { input } => {
                let outer_name = format!("outer_{}", Uuid::new_v4());
                let get_outer = SR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let input = input.applied_to(get_outer)?;
                SR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(SR::Distinct {
                        input: Box::new(input),
                    }),
                    default: None,
                }
            }
        })
    }
}

impl ScalarExpr {
    fn applied_to(
        self,
        outer_arity: usize,
        inner: &mut super::RelationExpr,
    ) -> Result<super::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use super::RelationExpr as SR;
        use super::ScalarExpr as SS;

        Ok(match self {
            Column(column) => {
                let column = (outer_arity as isize) + column;
                assert!(column >= 0);
                SS::Column(column as usize)
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
                // TODO(jamii) can optimize this using expr.used_outer_columns as key
                let inner_name = format!("inner_{}", Uuid::new_v4());
                let get_inner = SR::Get {
                    name: inner_name.clone(),
                    typ: inner.typ(),
                };
                let inner_arity = inner.arity();
                let branch = expr
                    .applied_to(get_inner)?
                    .project((0..inner_arity).collect())
                    .map(vec![(
                        SS::Literal(Datum::True),
                        ColumnType {
                            name: None,
                            scalar_type: ScalarType::Bool,
                            nullable: false,
                        },
                    )]);
                *inner = SR::Branch {
                    name: inner_name,
                    // TODO(jamii) kind of dumb that we have to clone inner
                    input: Box::new(inner.clone()),
                    key: (0..inner_arity).collect(),
                    branch: Box::new(branch),
                    default: Some(vec![(
                        Datum::False,
                        ColumnType {
                            name: None,
                            nullable: false,
                            scalar_type: ScalarType::Bool,
                        },
                    )]),
                };
                SS::Column(inner_arity)
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
            RelationExpr::Distinct { input } => input.typ(),
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
        unimplemented!()
        // RelationExpr::Constant { rows, typ }
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

    pub fn or_default(self, default: Vec<Datum>) -> Self {
        unimplemented!()
    }

    pub fn negate(self) -> Self {
        unimplemented!()
    }

    pub fn distinct(self) -> Self {
        RelationExpr::Distinct {
            input: Box::new(self),
        }
    }

    pub fn threshold(self) -> Self {
        unimplemented!()
    }

    pub fn union(self, other: Self) -> Self {
        RelationExpr::Union {
            left: Box::new(self),
            right: Box::new(other),
        }
    }
}

impl ScalarExpr {
    pub fn columns(is: &[isize]) -> Vec<ScalarExpr> {
        is.iter().map(|i| ScalarExpr::Column(*i)).collect()
    }

    pub fn column(column: isize) -> Self {
        ScalarExpr::Column(column)
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
