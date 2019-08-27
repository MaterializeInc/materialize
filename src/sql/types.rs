// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow;
use expr::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use failure::{bail, format_err};
use repr::*;
use std::collections::HashSet;
use uuid::Uuid;

/// All variables must be unique within the entire expr
pub type RelationVariable = String;
pub type ScalarVariable = String;

#[derive(Debug, Clone)]
pub enum RelationExpr {
    Get {
        name: RelationVariable,
        typ: RelationType,
        vars: Vec<ScalarVariable>,
    },
    // only needed for CTEs
    // Let {
    //     name: Name,
    //     value: Box<RelationExpr>,
    //     body: Box<RelationExpr>,
    // },
    Project {
        input: Box<RelationExpr>,
        output_vars: Vec<ScalarVariable>,
    },
    Map {
        input: Box<RelationExpr>,
        output_var: ScalarVariable,
        output_expr: ScalarExpr,
        output_type: ColumnType,
    },
    Filter {
        input: Box<RelationExpr>,
        predicate: ScalarExpr,
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
        key: Vec<ScalarVariable>,
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
    Literal(Datum),
    Get(ScalarVariable),
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
    distinct: bool,
    arg: Box<ScalarExpr>,
}

#[derive(Debug)]
pub enum ExprRef<'a> {
    RelationExpr(&'a RelationExpr),
    ScalarExpr(&'a ScalarExpr),
    AggregateExpr(&'a AggregateExpr),
}

#[derive(Debug)]
pub enum ExprRefMut<'a> {
    RelationExpr(&'a mut RelationExpr),
    ScalarExpr(&'a mut ScalarExpr),
    AggregateExpr(&'a mut AggregateExpr),
}

impl RelationExpr {
    fn visit1<'a, F>(&'a self, f: &mut F) -> Result<(), failure::Error>
    where
        F: FnMut(ExprRef<'a>) -> Result<(), failure::Error>,
    {
        use self::RelationExpr::*;
        use ExprRef::*;
        match self {
            Get { .. } => (),
            Project { input, .. } => f(RelationExpr(input))?,
            Map {
                input, output_expr, ..
            } => {
                f(RelationExpr(input))?;
                f(ScalarExpr(output_expr))?;
            }
            Filter { input, predicate } => {
                f(RelationExpr(input))?;
                f(ScalarExpr(predicate))?;
            }
            Join {
                left, right, on, ..
            } => {
                f(RelationExpr(left))?;
                f(RelationExpr(right))?;
                f(ScalarExpr(on))?;
            }
            Reduce {
                input, aggregates, ..
            } => {
                f(RelationExpr(input))?;
                for (aggregate, _) in aggregates {
                    f(AggregateExpr(aggregate))?;
                }
            }
            Distinct { input, .. } => {
                f(RelationExpr(input))?;
            }
            Union { left, right } => {
                f(RelationExpr(left))?;
                f(RelationExpr(right))?;
            }
        }
        Ok(())
    }
}

impl ScalarExpr {
    fn visit1<'a, F>(&'a self, f: &mut F) -> Result<(), failure::Error>
    where
        F: FnMut(ExprRef<'a>) -> Result<(), failure::Error>,
    {
        use self::ScalarExpr::*;
        use ExprRef::*;
        match self {
            Literal(_) => (),
            Get(_) => (),
            CallUnary { expr, .. } => f(ScalarExpr(expr))?,
            CallBinary { expr1, expr2, .. } => {
                f(ScalarExpr(expr1))?;
                f(ScalarExpr(expr2))?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(ScalarExpr(expr))?;
                }
            }
            Exists(rel) => {
                f(RelationExpr(rel))?;
            }
        }
        Ok(())
    }
}

impl AggregateExpr {
    fn visit1<'a, F>(&'a self, f: &mut F) -> Result<(), failure::Error>
    where
        F: FnMut(ExprRef<'a>) -> Result<(), failure::Error>,
    {
        use ExprRef::*;
        f(ScalarExpr(&*self.arg))?;
        Ok(())
    }
}

impl<'a> ExprRef<'a> {
    fn visit<F>(self, f: &mut F) -> Result<(), failure::Error>
    where
        F: FnMut(Self) -> Result<(), failure::Error>,
    {
        f(self)?;
        match self {
            ExprRef::RelationExpr(relation) => relation.visit1(&mut |expr| expr.visit(f)),
            ExprRef::ScalarExpr(scalar) => scalar.visit1(&mut |expr| expr.visit(f)),
            ExprRef::AggregateExpr(aggregate) => aggregate.visit1(&mut |expr| expr.visit(f)),
        }?;
        Ok(())
    }
}

impl RelationExpr {
    fn used_variables(&self) -> HashSet<&ScalarVariable> {
        let mut vars = HashSet::new();
        ExprRef::RelationExpr(self)
            .visit(&mut |expr| {
                if let ExprRef::ScalarExpr(ScalarExpr::Get(var)) = expr {
                    vars.insert(var);
                }
                Ok(())
            })
            .unwrap();
        vars
    }
}

fn resolve(vars: &[ScalarVariable], var: &ScalarVariable) -> Result<usize, failure::Error> {
    vars.iter()
        .position(|var2| var == var2)
        .ok_or_else(|| format_err!("Unbound variable: {:?}", var))
}

impl RelationExpr {
    pub fn lower(self) -> Result<(expr::RelationExpr, Vec<ScalarVariable>), failure::Error> {
        self.applied_to(
            expr::RelationExpr::Constant {
                rows: vec![vec![]],
                typ: RelationType {
                    column_types: vec![],
                },
            },
            vec![],
        )
    }

    fn applied_to(
        self,
        outer: expr::RelationExpr,
        outer_vars: Vec<ScalarVariable>,
    ) -> Result<(expr::RelationExpr, Vec<ScalarVariable>), failure::Error> {
        use self::RelationExpr::*;
        use expr::RelationExpr as DR;
        Ok(match self {
            Get { name, typ, vars } => {
                let product = outer.product(DR::Get { name, typ });
                let mut product_vars = outer_vars;
                product_vars.extend(vars);
                (product, product_vars)
            }
            Project { input, output_vars } => {
                let outer_arity = outer_vars.len();
                let (input, input_vars) = input.applied_to(outer, outer_vars)?;
                let outputs = (0..outer_arity)
                    .chain(
                        output_vars
                            .into_iter()
                            .map(|output_var| resolve(&input_vars, &output_var))
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .collect::<Vec<_>>();
                let project_vars = outputs.iter().map(|i| input_vars[*i].clone()).collect();
                let project = DR::Project {
                    input: Box::new(input),
                    outputs,
                };
                (project, project_vars)
            }
            Map {
                input,
                output_var,
                output_expr,
                output_type,
            } => {
                let (mut input, mut input_vars) = input.applied_to(outer, outer_vars)?;
                let output_expr = output_expr.applied_to(&mut input, &mut input_vars)?;
                let map = input.map(vec![(output_expr, output_type)]);
                let mut map_vars = input_vars;
                map_vars.push(output_var);
                (map, map_vars)
            }
            Filter { input, predicate } => {
                let (mut input, mut input_vars) = input.applied_to(outer, outer_vars)?;
                let predicate = predicate.applied_to(&mut input, &mut input_vars)?;
                let filter = input.filter(vec![predicate]);
                (filter, input_vars)
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
                let (left, left_vars) = left.applied_to(outer, outer_vars)?;
                let (mut product, mut product_vars) = right.applied_to(left, left_vars)?;
                let on = on.applied_to(&mut product, &mut product_vars)?;
                let join = product.filter(vec![on]);
                (join, product_vars)
            }
            Union { left, right } => {
                let outer_name = format!("outer_{}", Uuid::new_v4());
                let get_outer = DR::Get {
                    name: outer_name.clone(),
                    typ: outer.typ(),
                };
                let (left, left_vars) = left.applied_to(get_outer.clone(), outer_vars.clone())?;
                let (right, right_vars) = right.applied_to(get_outer, outer_vars)?;
                let union = DR::Let {
                    name: outer_name,
                    value: Box::new(outer),
                    body: Box::new(DR::Union {
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                };
                // TODO(jamii) this is almost certainly not right
                assert!(left_vars == right_vars);
                (union, left_vars)
            }
            // TODO reduce distinct
            _ => unimplemented!(),
        })
    }
}

impl ScalarExpr {
    fn applied_to(
        self,
        outer: &mut expr::RelationExpr,
        outer_vars: &mut Vec<ScalarVariable>,
        // group_key: Option<&[usize]>,
    ) -> Result<expr::ScalarExpr, failure::Error> {
        use self::ScalarExpr::*;
        use expr::RelationExpr as DR;
        use expr::ScalarExpr as DS;

        Ok(match self {
            Literal(datum) => DS::Literal(datum),
            Get(var) => DS::Column(resolve(outer_vars, &var)?),
            CallUnary { func, expr } => DS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(outer, outer_vars)?),
            },
            CallBinary { func, expr1, expr2 } => DS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(outer, outer_vars)?),
                expr2: Box::new(expr2.applied_to(outer, outer_vars)?),
            },
            CallVariadic { func, exprs } => DS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(outer, outer_vars))
                    .collect::<Result<Vec<_>, failure::Error>>()?,
            },
            Exists(rel) => {
                let name = format!("branch_{}", Uuid::new_v4());
                let used_vars = rel.used_variables();
                let key = outer_vars
                    .iter()
                    .filter(|var| used_vars.contains(var))
                    .map(|var| resolve(outer_vars, &var))
                    .collect::<Result<Vec<_>, _>>()?;
                let outer_typ = outer.typ();
                let (branch, branch_vars) = rel.applied_to(
                    DR::Get {
                        name: name.clone(),
                        typ: RelationType {
                            column_types: key
                                .iter()
                                .map(|i| outer_typ.column_types[*i].clone())
                                .collect(),
                        },
                    },
                    key.iter().map(|i| outer_vars[*i].clone()).collect(),
                )?;
                // TODO(jamii) more robust to project used_vars out of branch_vars?
                let branch = branch.project((0..key.len()).collect()).map(vec![(
                    DS::Literal(Datum::True),
                    ColumnType {
                        name: None,
                        scalar_type: ScalarType::Bool,
                        nullable: false,
                    },
                )]);
                *outer = DR::Branch {
                    name,
                    // TODO(jamii) kind of dumb that we have to clone outer
                    input: Box::new(outer.clone()),
                    key,
                    branch: Box::new(branch),
                    default: Some(vec![Datum::False]),
                };
                outer_vars.push(branch_vars[branch_vars.len() - 1].clone());
                DS::Column(outer_vars.len() - 1)
            }
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
                vars: vec!["foo_a".to_owned()],
            }),
            output_var: "exists".to_owned(),
            output_expr: S::Exists(Box::new(R::Filter {
                input: Box::new(R::Get {
                    name: "bar".to_owned(),
                    typ: RelationType {
                        column_types: vec![ColumnType {
                            name: None,
                            scalar_type: ScalarType::Bool,
                            nullable: false,
                        }],
                    },
                    vars: vec!["bar_a".to_owned()],
                }),
                predicate: S::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1: Box::new(S::Get("foo_a".to_owned())),
                    expr2: Box::new(S::Get("bar_a".to_owned())),
                },
            })),
            output_type: ColumnType {
                name: None,
                scalar_type: ScalarType::Bool,
                nullable: false,
            },
        };
        assert_eq!(
            expr.lower().unwrap().0,
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
