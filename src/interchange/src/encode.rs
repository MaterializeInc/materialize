// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_repr::{ColumnName, Datum, RelationDesc, Row, SqlColumnType};

pub trait Encode {
    fn encode_unchecked(&self, row: Row) -> Vec<u8>;

    /// Given the output of a call to [`Encode::encode_unchecked`], returns
    /// a hash that is suitable for stable partitioning.
    fn hash(&self, buf: &[u8]) -> u64 {
        // We use seahash as it outperforms pretty much all other options, and
        // has great mathematically proven statistical properties. It truly is a
        // remarkable non-cryptographic hash. More details can be found here:
        // https://docs.rs/seahash/latest/seahash/
        seahash::hash(buf)
    }
}

/// Bundled information sufficient to encode Datums.
#[derive(Debug)]
pub struct TypedDatum<'a> {
    pub datum: Datum<'a>,
    pub typ: &'a SqlColumnType,
}

impl<'a> TypedDatum<'a> {
    /// Pairs a datum and its type, for encoding.
    pub fn new(datum: Datum<'a>, typ: &'a SqlColumnType) -> Self {
        Self { datum, typ }
    }
}

/// Extracts deduplicated column names and types from a relation description.
pub fn column_names_and_types(desc: RelationDesc) -> Vec<(ColumnName, SqlColumnType)> {
    // Invent names for columns that don't have a name.
    let mut columns: Vec<_> = desc.into_iter().collect();

    let mut name = String::new();
    // Deduplicate names.
    let mut seen = BTreeSet::new();
    for (column_name, _ty) in &mut columns {
        name.clear();
        name.push_str(column_name.as_str());
        let stem_len = name.len();
        let mut i = 1;
        while seen.contains(&name) {
            name.truncate(stem_len);
            if name.ends_with(|c: char| c.is_ascii_digit()) {
                name.push('_');
            }
            name.push_str(&i.to_string());
            i += 1;
        }
        seen.insert(name.clone());
        if column_name.as_str() != name {
            *column_name.as_mut_boxed_str() = name.clone().into();
        }
    }
    columns
}
