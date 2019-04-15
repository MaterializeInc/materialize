// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use lazy_static::lazy_static;
use sqlparser::sqlast::SQLIdent;
use std::collections::HashMap;

use crate::repr::{FType, Type};

pub type TableCollection = HashMap<String, Type>;

pub enum Side {
    Left,
    Right,
}

/// Manages resolution of table and column references.
pub struct NameResolver<'a> {
    all_tables: &'a TableCollection,
    columns: Vec<Type>,
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

    pub fn import_table(&mut self, name: &str) {
        self.breakpoint = self.columns.len();
        let typ = self.get_table(name);
        match &typ.ftype {
            FType::Null
            | FType::Bool
            | FType::Int32
            | FType::Int64
            | FType::Float32
            | FType::Float64
            | FType::Bytes
            | FType::String => self.columns.push(Type {
                name: Some(name.to_owned()),
                nullable: typ.nullable,
                ftype: typ.ftype.clone(),
            }),
            FType::Tuple(tuple) => self.columns.append(&mut tuple.clone()),
            _ => unimplemented!(),
        }
    }

    pub fn resolve_column(&self, name: &str) -> Result<(usize, Type), failure::Error> {
        let i = self
            .columns
            .iter()
            .position(|t| t.name.as_ref().map_or(false, |n| n == name));
        let i = match i {
            Some(i) => i,
            None => bail!("no column named {} in scope", name),
        };
        Ok((i, self.columns[i].clone()))
    }

    pub fn side(&self, pos: usize) -> Side {
        if pos < self.breakpoint {
            Side::Left
        } else {
            Side::Right
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
        std::mem::replace(&mut self.columns, types);
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
