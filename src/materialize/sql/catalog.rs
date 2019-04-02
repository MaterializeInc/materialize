// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::bail;
use lazy_static::lazy_static;
use std::collections::HashMap;

use crate::repr::{FType, Type};

pub type TableCollection = HashMap<String, Type>;

/// Manages resolution of table and column references.
pub struct NameResolver<'a> {
    all_tables: &'a TableCollection,
    columns: Vec<Type>,
}

impl<'a> NameResolver<'a> {
    pub fn new(all_tables: &'a TableCollection) -> NameResolver<'a> {
        NameResolver {
            all_tables: all_tables,
            columns: Vec::new(),
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
        let i = self.columns.iter().position(|t| {
            t.name.as_ref().map_or(false, |n| n == name)
        });
        let i = match i {
            Some(i) => i,
            None => bail!("no column named {} in scope", name),
        };
        Ok((i, self.columns[i].clone()))
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
