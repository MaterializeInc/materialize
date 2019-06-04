// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use sqlparser::sqlast::SQLFunction;

use crate::dataflow::{Aggregate, RelationExpr, ScalarExpr};
use crate::repr::{ColumnType, RelationType};
use ore::option::OptionExt;

#[derive(Debug, Clone)]
pub struct Name {
    table_name: Option<String>,
    column_name: Option<String>,
    func_hash: Option<u64>,
}

impl Name {
    pub fn none() -> Self {
        Name {
            table_name: None,
            column_name: None,
            func_hash: None,
        }
    }
}

/// Wraps a dataflow relation_expr with a sql scope
#[derive(Debug, Clone)]
pub struct SQLRelationExpr {
    relation_expr: RelationExpr,
    columns: Vec<(Name, ColumnType)>,
}

impl SQLRelationExpr {
    pub fn from_source(name: &str, types: Vec<ColumnType>) -> Self {
        SQLRelationExpr {
            relation_expr: RelationExpr::Source(name.to_owned()),
            columns: types
                .into_iter()
                .map(|typ| {
                    (
                        Name {
                            table_name: Some(name.to_owned()),
                            column_name: typ.name.clone(),
                            func_hash: None,
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
    ) -> Result<(usize, &Name, &ColumnType), failure::Error> {
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
    ) -> Result<(usize, &Name, &ColumnType), failure::Error> {
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

    pub fn resolve_func<'a, 'b>(&'a self, func: &'b SQLFunction) -> (usize, &'a ColumnType) {
        let func_hash = ore::hash::hash(func);
        let mut results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| name.func_hash == Some(func_hash));
        match (results.next(), results.next()) {
            (None, None) => panic!("no func hash {:?} in scope", func_hash),
            (Some((i, (_, typ))), None) => (i, typ),
            (Some(_), Some(_)) => panic!("func hash {:?} is ambiguous", func_hash),
            _ => unreachable!(),
        }
    }

    pub fn join_on(
        self,
        right: Self,
        left_key: Vec<ScalarExpr>,
        right_key: Vec<ScalarExpr>,
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Self {
        let SQLRelationExpr {
            relation_expr: left_relation_expr,
            columns: mut left_columns,
        } = self;
        let SQLRelationExpr {
            relation_expr: right_relation_expr,
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
        SQLRelationExpr {
            relation_expr: RelationExpr::Join {
                left: Box::new(left_relation_expr),
                right: Box::new(right_relation_expr),
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
            ScalarExpr::columns(&left_key),
            ScalarExpr::columns(&right_key),
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
                ScalarExpr::columns(&left_key),
                ScalarExpr::columns(&right_key),
                include_left_outer,
                include_right_outer,
            )
            .project_raw(&project_key))
    }

    fn project_raw(self, project_key: &[usize]) -> Self {
        let SQLRelationExpr {
            relation_expr,
            columns,
        } = self;
        SQLRelationExpr {
            relation_expr: RelationExpr::Project {
                outputs: project_key.iter().map(|&i| ScalarExpr::Column(i)).collect(),
                input: Box::new(relation_expr),
            },
            columns: project_key.iter().map(|i| columns[*i].clone()).collect(),
        }
    }

    pub fn filter(mut self, predicate: ScalarExpr) -> Self {
        self.relation_expr = RelationExpr::Filter {
            predicate,
            input: Box::new(self.relation_expr),
        };
        self
    }

    pub fn aggregate(
        self,
        key_expr: Vec<ScalarExpr>,
        key_columns: Vec<(Name, ColumnType)>,
        aggregates: Vec<(&SQLFunction, Aggregate, ColumnType)>,
    ) -> Self {
        let SQLRelationExpr { relation_expr, .. } = self;
        // Deduplicate by function hash.
        let mut aggregates: Vec<_> = aggregates
            .into_iter()
            .map(|(func, agg, typ)| (ore::hash::hash(func), agg, typ))
            .collect();
        aggregates.sort_by_key(|(func_hash, _agg, _typ)| *func_hash);
        aggregates.dedup_by_key(|(func_hash, _agg, _typ)| *func_hash);
        let mut agg_columns = Vec::new();
        let mut aggs = Vec::new();
        for (func_hash, agg, typ) in aggregates {
            agg_columns.push((
                Name {
                    table_name: None,
                    column_name: None,
                    func_hash: Some(func_hash),
                },
                typ,
            ));
            aggs.push(agg);
        }
        SQLRelationExpr {
            relation_expr: RelationExpr::Aggregate {
                key: key_expr,
                aggs,
                input: Box::new(relation_expr),
            },
            columns: key_columns.into_iter().chain(agg_columns).collect(),
        }
    }

    pub fn project(self, outputs: Vec<(ScalarExpr, ColumnType)>) -> Self {
        let SQLRelationExpr { relation_expr, .. } = self;
        SQLRelationExpr {
            relation_expr: RelationExpr::Project {
                outputs: outputs.iter().map(|(e, _)| e.clone()).collect(),
                input: Box::new(relation_expr),
            },
            columns: outputs
                .iter()
                .map(|(_, t)| {
                    (
                        Name {
                            table_name: None,
                            column_name: t.name.clone(),
                            func_hash: None,
                        },
                        t.clone(),
                    )
                })
                .collect(),
        }
    }

    pub fn distinct(mut self) -> Self {
        self.relation_expr = RelationExpr::Distinct(Box::new(self.relation_expr));
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

    pub fn finish(self) -> (RelationExpr, RelationType) {
        let SQLRelationExpr {
            relation_expr,
            columns,
        } = self;
        (
            relation_expr,
            RelationType {
                column_types: columns.into_iter().map(|(_, typ)| typ).collect(),
            },
        )
    }
}
