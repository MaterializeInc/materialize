// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use lazy_static::lazy_static;
use sqlparser::sqlast::SQLIdent;
use std::collections::HashMap;
use std::ops::Deref;

use crate::repr::{FType, Type};

pub type TableCollection = HashMap<String, Type>;

pub enum Side {
    Left,
    Right,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Side::Left => write!(f, "left"),
            Side::Right => write!(f, "right"),
        }
    }
}

/// Manages resolution of table and column references.
pub struct NameResolver<'a> {
    all_tables: &'a TableCollection,
    columns: Vec<(Option<String>, Type)>, // (table_name, type)
    funcs: HashMap<*const SQLIdent, (usize, Type)>,
    breakpoint: usize,
}

impl<'a> NameResolver<'a> {
    pub fn new(all_tables: &'a TableCollection) -> NameResolver<'a> {
        NameResolver {
            all_tables,
            columns: Vec::new(),
            funcs: HashMap::new(),
            breakpoint: 0,
        }
    }

    fn get_table(&self, name: &str) -> &Type {
        if name == "$dual" {
            &*DUAL_TYPE
        } else {
            &self.all_tables[name]
        }
    }

    pub fn import_table(&mut self, name: &str, alias: Option<&str>) {
        self.breakpoint = self.columns.len();
        let typ = self.get_table(name).clone();
        match &typ.ftype {
            FType::Null
            | FType::Bool
            | FType::Int32
            | FType::Int64
            | FType::Float32
            | FType::Float64
            | FType::Bytes
            | FType::String => self.columns.push((
                Some(name.to_owned()),
                Type {
                    name: Some(name.to_owned()),
                    nullable: typ.nullable,
                    ftype: typ.ftype.clone(),
                },
            )),
            FType::Tuple(tuple) => self.columns.extend(
                tuple
                    .iter()
                    .map(|typ| (Some(alias.unwrap_or(name).to_owned()), typ.clone())),
            ),
            _ => unimplemented!(),
        }
    }

    pub fn resolve_column(&self, column_name: &str) -> Result<(usize, &Type), failure::Error> {
        let results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (_, t))| t.name.as_ref().map(|t| t.deref()) == Some(column_name))
            .collect::<Vec<_>>();
        match results.len() {
            0 => bail!("no column named {} in scope", column_name),
            1 => {
                let (i, (_, t)) = results.into_iter().next().unwrap();
                Ok((i, t))
            }
            _ => bail!("column name {} is ambiguous", column_name),
        }
    }

    pub fn resolve_table_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<(usize, &Type), failure::Error> {
        let results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (tn, t))| {
                tn.as_ref().map(|tn| tn.deref()) == Some(table_name)
                    && t.name.as_ref().map(|t| t.deref()) == Some(column_name)
            })
            .collect::<Vec<_>>();
        match results.len() {
            0 => bail!("no column named {}.{} in scope", table_name, column_name),
            1 => {
                let (i, (_, t)) = results.into_iter().next().unwrap();
                Ok((i, t))
            }
            _ => bail!("column name {}.{} is ambiguous", table_name, column_name),
        }
    }

    pub fn resolve_column_on_side(
        &self,
        column_name: &str,
        side: Side,
    ) -> Result<(usize, &Type), failure::Error> {
        let results = self
            .columns
            .iter()
            .enumerate()
            .filter(|(i, (_, t))| {
                t.name.as_ref().map(|t| t.deref()) == Some(column_name)
                    && match side {
                        Side::Left => *i < self.breakpoint,
                        Side::Right => *i >= self.breakpoint,
                    }
            })
            .collect::<Vec<_>>();
        match results.len() {
            0 => bail!(
                "no column named {} in scope on {} of join",
                column_name,
                side
            ),
            1 => {
                let (i, (_, t)) = results.into_iter().next().unwrap();
                Ok((i, t))
            }
            _ => bail!(
                "column name {} is ambiguous on {} of join",
                column_name,
                side
            ),
        }
    }

    pub fn get_all_column_types(&self) -> Vec<&Type> {
        self.columns.iter().map(|(_, t)| t).collect()
    }

    pub fn get_table_column_types(&self, table_name: &str) -> Vec<&Type> {
        self.columns
            .iter()
            .filter(|(tn, _)| tn.as_ref().map(|tn| tn.deref()) == Some(table_name))
            .map(|(_, t)| t)
            .collect()
    }

    pub fn resolve_natural_join(&self) -> (Vec<usize>, Vec<&Type>, Vec<usize>, Vec<&Type>) {
        let mut left_key = vec![];
        let mut left_types = vec![];
        let mut right_key = vec![];
        let mut right_types = vec![];
        for r in self.breakpoint..self.columns.len() {
            let (_, r_type) = &self.columns[r];
            if let Some(name) = &r_type.name {
                if let Ok((l, l_type)) = self.resolve_column_on_side(name, Side::Left) {
                    left_key.push(l);
                    left_types.push(l_type);
                    right_key.push(r - self.breakpoint);
                    right_types.push(r_type);
                }
            }
        }
        (left_key, left_types, right_key, right_types)
    }

    pub fn resolve_using_join(
        &self,
        names: &[String],
    ) -> Result<(Vec<usize>, Vec<Type>, Vec<usize>, Vec<Type>), failure::Error> {
        let mut left_key = vec![];
        let mut left_types = vec![];
        let mut right_key = vec![];
        let mut right_types = vec![];
        for name in names {
            let (l, l_type) = self.resolve_column_on_side(name, Side::Left)?;
            let (r, r_type) = self.resolve_column_on_side(name, Side::Right)?;
            left_key.push(l);
            left_types.push(l_type.clone());
            right_key.push(r - self.breakpoint);
            right_types.push(r_type.clone());
        }
        Ok((left_key, left_types, right_key, right_types))
    }

    pub fn project(&mut self, key: &[usize]) {
        self.columns = key
            .iter()
            .map(|i| self.columns[*i].clone())
            .collect::<Vec<_>>();
    }

    pub fn side(&self, pos: usize) -> Side {
        if pos < self.breakpoint {
            Side::Left
        } else {
            Side::Right
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_columns_on_side(&self, side: Side) -> usize {
        match side {
            Side::Left => self.breakpoint,
            Side::Right => self.columns.len() - self.breakpoint,
        }
    }

    pub fn make_nullable(&mut self, side: Side) {
        let range = match side {
            Side::Left => 0..self.breakpoint,
            Side::Right => self.breakpoint..self.columns.len(),
        };
        for i in range {
            self.columns[i].1.nullable = true;
        }
    }

    pub fn adjust_rhs(&self, pos: usize) -> usize {
        pos - self.breakpoint
    }

    #[allow(clippy::ptr_arg)]
    pub fn resolve_func(&self, name: &SQLIdent) -> Result<(usize, Type), failure::Error> {
        match self.funcs.get(&(name as *const _)) {
            Some((i, typ)) => Ok((i + self.columns.len(), typ.clone())),
            None => bail!("unknown function {}", name),
        }
    }

    pub fn add_func(&mut self, id: *const SQLIdent, typ: Type) {
        self.funcs.insert(id, (self.funcs.len(), typ));
    }

    pub fn reset(&mut self, types: Vec<Type>) {
        std::mem::replace(
            &mut self.columns,
            // TODO(jamii) need to refactor this so we can preserve table_names
            types.into_iter().map(|typ| (None, typ)).collect(),
        );
    }
}

lazy_static! {
    static ref DUAL_TYPE: Type = Type {
        name: Some("dual".into()),
        nullable: false,
        ftype: FType::Tuple(vec![Type {
            name: Some("x".into()),
            nullable: false,
            ftype: FType::String,
        }]),
    };
}
