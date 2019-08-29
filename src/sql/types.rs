// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use expr::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use failure::{bail, format_err};
use repr::*;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum RelationExpr {
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
    // If {
    //     cond: Box<ScalarExpr>,
    //     then: Box<ScalarExpr>,
    //     els: Box<ScalarExpr>,
    // },
    Exists(Box<RelationExpr>),
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    func: AggregateFunc,
    expr: Box<ScalarExpr>,
    distinct: bool,
}

impl RelationExpr {
    fn arity(&self) -> usize {
        use RelationExpr::*;
        match self {
            Get { typ, .. } => typ.column_types.len(),
            Project { outputs, .. } => outputs.len(),
            Map { input, .. } => input.arity() + 1,
            Filter { input, .. } => input.arity(),
            Join { left, right, .. } => left.arity() + right.arity(),
            Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            Distinct { input } => input.arity(),
            Union { left, .. } => left.arity(),
        }
    }

    // TODO(jamii) check that visit_scalar is using correct column offset
    fn used_outer_columns(&self) -> HashSet<isize> {
        let mut used_outer_columns = HashSet::new();
        fn visit_relation(
            expr: &RelationExpr,
            num_outer_columns: usize,
            used_outer_columns: &mut HashSet<isize>,
        ) {
            use RelationExpr::*;
            match expr {
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
    pub fn lower(self) -> Result<expr::RelationExpr, failure::Error> {
        self.applied_to(expr::RelationExpr::Constant {
            rows: vec![vec![]],
            typ: RelationType {
                column_types: vec![],
            },
        })
    }

    // TODO(jamii) do we actually need to track vars? currently we need them for resolving variables, but we have to have already done that in the sql layer to get the unique variable names
    // TODO(jamii) ensure outer is always distinct? or make distinct during reduce etc?
    fn applied_to(self, outer: expr::RelationExpr) -> Result<expr::RelationExpr, failure::Error> {
        use self::RelationExpr::*;
        use expr::RelationExpr as DR;
        Ok(match self {
            Get { name, typ } => outer.product(DR::Get { name, typ }),
            Project { input, outputs } => {
                let outer_arity = outer.arity();
                let input = input.applied_to(outer)?;
                let outputs = (0..outer_arity)
                    .chain(outputs.into_iter().map(|i| outer_arity + i))
                    .collect::<Vec<_>>();
                DR::Project {
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
                let get_outer = DR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let left = left.applied_to(get_outer.clone())?;
                let right = right.applied_to(get_outer)?;
                DR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(DR::Union {
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
                let get_outer = DR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let mut input = input.applied_to(get_outer)?;
                let default = if group_key.is_empty() {
                    Some(
                        aggregates
                            .iter()
                            .map(|(aggregate, _)| aggregate.func.default())
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
                DR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(DR::Reduce {
                        input: Box::new(input),
                        group_key,
                        aggregates,
                    }),
                    default,
                }
            }
            Distinct { input } => {
                let outer_name = format!("outer_{}", Uuid::new_v4());
                let get_outer = DR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let outer_arity = outer.arity();
                let input = input.applied_to(get_outer)?;
                DR::Branch {
                    name: outer_name,
                    input: Box::new(outer),
                    key: (0..outer_arity).collect(),
                    branch: Box::new(DR::Distinct {
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
        inner: &mut expr::RelationExpr,
    ) -> Result<expr::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use expr::RelationExpr as DR;
        use expr::ScalarExpr as DS;

        Ok(match self {
            Column(column) => {
                let column = (outer_arity as isize) + column;
                assert!(column >= 0);
                DS::Column(column as usize)
            }
            Literal(datum) => DS::Literal(datum),
            CallUnary { func, expr } => DS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(outer_arity, inner)?),
            },
            CallBinary { func, expr1, expr2 } => DS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(outer_arity, inner)?),
                expr2: Box::new(expr2.applied_to(outer_arity, inner)?),
            },
            CallVariadic { func, exprs } => DS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(outer_arity, inner))
                    .collect::<Result<Vec<_>, failure::Error>>()?,
            },
            Exists(expr) => {
                // TODO(jamii) can optimize this using expr.used_inner_columns as key
                let inner_name = format!("inner_{}", Uuid::new_v4());
                let get_inner = DR::Get {
                    name: inner_name.clone(),
                    typ: inner.typ(),
                };
                let inner_arity = inner.arity();
                let branch = expr
                    .applied_to(get_inner)?
                    .project((0..inner_arity).collect())
                    .map(vec![(
                        DS::Literal(Datum::True),
                        ColumnType {
                            name: None,
                            scalar_type: ScalarType::Bool,
                            nullable: false,
                        },
                    )]);
                *inner = DR::Branch {
                    name: inner_name,
                    // TODO(jamii) kind of dumb that we have to clone inner
                    input: Box::new(inner.clone()),
                    key: (0..inner_arity).collect(),
                    branch: Box::new(branch),
                    default: Some(vec![Datum::False]),
                };
                DS::Column(inner_arity)
            }
        })
    }
}

impl AggregateExpr {
    fn applied_to(
        self,
        outer_arity: usize,
        inner: &mut expr::RelationExpr,
    ) -> Result<expr::AggregateExpr, failure::Error> {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        Ok(expr::AggregateExpr {
            func,
            expr: expr.applied_to(outer_arity, inner)?,
            distinct,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn will_it_blend() {
        use expr::RelationExpr as DR;
        use expr::ScalarExpr as DS;
        use RelationExpr as R;
        use ScalarExpr as S;
        let expr = R::Map {
            input: Box::new(R::Get {
                name: "foo".to_owned(),
                typ: RelationType {
                    column_types: vec![ColumnType {
                        name: None,
                        scalar_type: ScalarType::Bool,
                        nullable: false,
                    }],
                },
            }),
            scalars: vec![(
                S::Exists(Box::new(R::Filter {
                    input: Box::new(R::Get {
                        name: "bar".to_owned(),
                        typ: RelationType {
                            column_types: vec![ColumnType {
                                name: None,
                                scalar_type: ScalarType::Bool,
                                nullable: false,
                            }],
                        },
                    }),
                    predicates: vec![S::CallBinary {
                        func: BinaryFunc::Eq,
                        expr1: Box::new(S::Column(0)),
                        expr2: Box::new(S::Column(-1)),
                    }],
                })),
                ColumnType {
                    name: None,
                    scalar_type: ScalarType::Bool,
                    nullable: false,
                },
            )],
        };
        assert_eq!(
            expr.lower().unwrap(),
            DR::Get {
                name: "foo".to_owned(),
                typ: RelationType {
                    column_types: vec![ColumnType {
                        name: None,
                        scalar_type: ScalarType::Bool,
                        nullable: false,
                    }],
                },
            }
        );
    }
}
