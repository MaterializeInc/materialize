// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::iter;

use mz_ore::now::NowFn;
use mz_repr::{Datum, RelationDesc, Row, ScalarType};

use crate::types::sources::encoding::DataEncodingInner;
use crate::types::sources::{Generator, GeneratorMessageType};

pub struct Datums {}

impl Generator for Datums {
    fn data_encoding_inner(&self) -> DataEncodingInner {
        let mut desc =
            RelationDesc::empty().with_column("rowid", ScalarType::Int64.nullable(false));
        let typs = ScalarType::enumerate();
        let mut names = HashSet::new();
        for typ in typs {
            // Cut out variant information from the debug print.
            let mut name = format!("_{:?}", typ)
                .split(' ')
                .next()
                .unwrap()
                .to_lowercase();
            // Incase we ever have multiple variants of the same type, create
            // unique names for them.
            while names.contains(&name) {
                name.push('_');
            }
            names.insert(name.clone());
            desc = desc.with_column(name, typ.clone().nullable(true));
        }
        DataEncodingInner::RowCodec(desc)
    }

    fn views(&self) -> Vec<(&str, RelationDesc)> {
        Vec::new()
    }

    fn by_seed(
        &self,
        _: NowFn,
        _seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row)>> {
        let typs = ScalarType::enumerate();
        let mut datums: Vec<Vec<Datum>> = typs
            .iter()
            .map(|typ| typ.interesting_datums().collect())
            .collect();
        let len = datums.iter().map(|v| v.len()).max().unwrap();
        // Fill in NULLs for shorter types, and append at least 1 NULL onto
        // every type.
        for dats in datums.iter_mut() {
            while dats.len() < (len + 1) {
                dats.push(Datum::Null);
            }
        }
        // Put the rowid column at the start.
        datums.insert(
            0,
            (1..=len + 1)
                .map(|i| Datum::Int64(i64::try_from(i).expect("must fit")))
                .collect(),
        );
        let mut idx = 0;
        Box::new(iter::from_fn(move || {
            if idx == len {
                return None;
            }
            let mut row = Row::with_capacity(datums.len());
            let mut packer = row.packer();
            for d in &datums {
                packer.push(d[idx]);
            }
            idx += 1;
            let message = if idx == len {
                GeneratorMessageType::Finalized
            } else {
                GeneratorMessageType::InProgress
            };
            Some((0, message, row))
        }))
    }
}
