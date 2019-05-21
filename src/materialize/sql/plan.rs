// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use sqlparser::sqlast::SQLObjectName;

use crate::dataflow::{Aggregate, Expr, Plan};
use crate::repr::{FType, Type};
use ore::option::OptionExt;

/// This a massive hack - we don't have unique ids for ast nodes so we use the
/// address of the string name in func node.
pub type FuncName = *const SQLObjectName;

#[derive(Debug, Clone)]
pub struct Name {
    table_name: Option<String>,
    column_name: Option<String>,
    func_name: Option<FuncName>,
}

impl Name {
    pub fn none() -> Self {
        Name {
            table_name: None,
            column_name: None,
            func_name: None,
        }
    }
}

/// Wraps a dataflow plan with a sql scope
#[derive(Debug, Clone)]
pub struct SQLPlan {
    plan: Plan,
    columns: Vec<(Name, Type)>,
}

impl SQLPlan {
    pub fn from_source(name: &str, types: Vec<Type>) -> Self {
        SQLPlan {
            plan: Plan::Source(name.to_owned()),
            columns: types
                .into_iter()
                .map(|typ| {
                    (
                        Name {
                            table_name: Some(name.to_owned()),
                            column_name: typ.name.clone(),
                            func_name: None,
                        },
                        typ,
                    )
                })
                .collect(),
        }
    }

    pub fn alias_table(mut self, table_name: &str) -> Self {
        for (name, _) in &mut self.columns {
            name.table_name = Some(table_name.to_owned());
        }
        self
    }

    pub fn resolve_column(
        &self,
        column_name: &str,
    ) -> Result<(usize, &Name, &Type), failure::Error> {
        let mut results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| name.column_name.as_deref() == Some(column_name));
        match (results.next(), results.next()) {
            (None, None) => bail!("no column named {} in scope", column_name),
            (Some((i, (name, typ))), None) => Ok((i, name, typ)),
            (Some(_), Some(_)) => bail!("column name {} is ambiguous", column_name),
            _ => unreachable!(),
        }
    }

    pub fn resolve_table_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<(usize, &Name, &Type), failure::Error> {
        let mut results = self.columns.iter().enumerate().filter(|(_, (name, _))| {
            name.table_name.as_deref() == Some(table_name)
                && name.column_name.as_deref() == Some(column_name)
        });
        match (results.next(), results.next()) {
            (None, None) => bail!("no column named {}.{} in scope", table_name, column_name),
            (Some((i, (name, typ))), None) => Ok((i, name, typ)),
            (Some(_), Some(_)) => bail!("column name {}.{} is ambiguous", table_name, column_name),
            _ => unreachable!(),
        }
    }

    pub fn resolve_func(&self, func_name: FuncName) -> (usize, &Type) {
        let mut results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| name.func_name == Some(func_name));
        match (results.next(), results.next()) {
            (None, None) => panic!("no func named {:?} in scope", func_name),
            (Some((i, (_, typ))), None) => (i, typ),
            (Some(_), Some(_)) => panic!("func name {:?} is ambiguous", func_name),
            _ => unreachable!(),
        }
    }

    pub fn join_on(
        self,
        right: Self,
        left_key: Expr,
        right_key: Expr,
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Self {
        let SQLPlan {
            plan: left_plan,
            columns: mut left_columns,
        } = self;
        let SQLPlan {
            plan: right_plan,
            columns: mut right_columns,
        } = right;
        if include_left_outer {
            for (_, typ) in &mut right_columns {
                typ.nullable = true;
            }
        }
        if include_right_outer {
            for (_, typ) in &mut left_columns {
                typ.nullable = true;
            }
        }
        SQLPlan {
            plan: Plan::Join {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_key,
                right_key,
                include_left_outer: if include_left_outer {
                    Some(left_columns.len())
                } else {
                    None
                },
                include_right_outer: if include_right_outer {
                    Some(right_columns.len())
                } else {
                    None
                },
            },
            columns: left_columns
                .into_iter()
                .chain(right_columns.into_iter())
                .collect(),
        }
    }

    pub fn join_natural(
        self,
        right: Self,
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Self {
        let left = self;
        let mut left_key = vec![];
        let mut right_key = vec![];
        // TODO(jamii) check that we don't join on ambiguous column names
        for (r, (_, r_type)) in right.columns.iter().enumerate() {
            if let Some(name) = &r_type.name {
                if let Ok((l, _, _)) = left.resolve_column(name) {
                    left_key.push(l);
                    right_key.push(r);
                }
            }
        }
        // TODO(jamii) should natural join fail if left/right_key are empty?
        let project_key = (0..left.columns.len())
            .chain(
                (0..right.columns.len())
                    // drop columns on the right that were joined
                    .filter(|r| right_key.iter().find(|r2| *r2 == r).is_none())
                    .map(|r| left.columns.len() + r),
            )
            .collect::<Vec<_>>();
        left.join_on(
            right,
            Expr::columns(&left_key, &Expr::Ambient),
            Expr::columns(&right_key, &Expr::Ambient),
            include_left_outer,
            include_right_outer,
        )
        .project_raw(&project_key)
    }

    pub fn join_using(
        self,
        right: Self,
        names: &[String],
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Result<Self, failure::Error> {
        let left = self;
        let mut left_key = vec![];
        let mut right_key = vec![];
        for name in names {
            let (l, _, _) = left.resolve_column(name)?;
            let (r, _, _) = right.resolve_column(name)?;
            left_key.push(l);
            right_key.push(r);
        }
        let project_key = (0..left.columns.len())
            .chain(
                (0..right.columns.len())
                    // drop columns on the right that were joined
                    .filter(|r| right_key.iter().find(|r2| *r2 == r).is_none())
                    .map(|r| left.columns.len() + r),
            )
            .collect::<Vec<_>>();
        Ok(left
            .join_on(
                right,
                Expr::columns(&left_key, &Expr::Ambient),
                Expr::columns(&right_key, &Expr::Ambient),
                include_left_outer,
                include_right_outer,
            )
            .project_raw(&project_key))
    }

    fn project_raw(self, project_key: &[usize]) -> Self {
        let SQLPlan { plan, columns } = self;
        SQLPlan {
            plan: Plan::Project {
                outputs: project_key
                    .iter()
                    .map(|i| Expr::Column(*i, Box::new(Expr::Ambient)))
                    .collect(),
                input: Box::new(plan),
            },
            columns: project_key.iter().map(|i| columns[*i].clone()).collect(),
        }
    }

    pub fn filter(mut self, predicate: Expr) -> Self {
        self.plan = Plan::Filter {
            predicate,
            input: Box::new(self.plan),
        };
        self
    }

    pub fn aggregate(
        self,
        key_expr: Expr,
        key_columns: Vec<(Name, Type)>,
        aggregates: Vec<(FuncName, Aggregate, Type)>,
    ) -> Self {
        let SQLPlan { plan, .. } = self;
        let agg_columns = aggregates.iter().map(|(func_name, _, typ)| {
            (
                Name {
                    table_name: None,
                    column_name: None,
                    func_name: Some(*func_name),
                },
                typ.clone(),
            )
        });
        SQLPlan {
            plan: Plan::Aggregate {
                key: key_expr,
                aggs: aggregates.iter().map(|(_, agg, _)| agg.clone()).collect(),
                input: Box::new(plan),
            },
            columns: key_columns.into_iter().chain(agg_columns).collect(),
        }
    }

    pub fn project(self, outputs: Vec<(Expr, Type)>) -> Self {
        let SQLPlan { plan, .. } = self;
        SQLPlan {
            plan: Plan::Project {
                outputs: outputs.iter().map(|(e, _)| e.clone()).collect(),
                input: Box::new(plan),
            },
            columns: outputs
                .iter()
                .map(|(_, t)| {
                    (
                        Name {
                            table_name: None,
                            column_name: t.name.clone(),
                            func_name: None,
                        },
                        t.clone(),
                    )
                })
                .collect(),
        }
    }

    pub fn distinct(mut self) -> Self {
        self.plan = Plan::Distinct(Box::new(self.plan));
        self
    }

    pub fn named_columns(&self) -> Vec<(String, String)> {
        self.columns
            .iter()
            .filter_map(|(name, _)| match (&name.table_name, &name.column_name) {
                (Some(table_name), Some(column_name)) => {
                    Some((table_name.clone(), column_name.clone()))
                }
                _ => None,
            })
            .collect()
    }

    pub fn finish(self) -> (Plan, Type) {
        let SQLPlan { plan, columns } = self;
        (
            plan,
            Type {
                name: None,
                nullable: false,
                ftype: FType::Tuple(columns.into_iter().map(|(_, typ)| typ).collect()),
            },
        )
    }
}
