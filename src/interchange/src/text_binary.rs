// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BytesMut;
use mz_repr::{ColumnName, ColumnType, RelationDesc};

use crate::encode::{Encode, column_names_and_types};
use crate::envelopes;

#[derive(Debug)]
pub struct TextEncoder {
    columns: Vec<(ColumnName, ColumnType)>,
}

impl TextEncoder {
    pub fn new(desc: RelationDesc, debezium: bool) -> Self {
        let mut columns = column_names_and_types(desc);
        if debezium {
            columns = envelopes::dbz_envelope(columns);
        };
        Self { columns }
    }
}

impl Encode for TextEncoder {
    fn encode_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        let mut buf = BytesMut::new();
        for ((_, typ), val) in self.columns.iter().zip_eq(row.iter()) {
            if let Some(pgrepr_value) = mz_pgrepr::Value::from_datum(val, &typ.scalar_type) {
                pgrepr_value.encode_text(&mut buf);
            }
        }

        buf.to_vec()
    }
}

#[derive(Debug)]
pub struct BinaryEncoder {
    columns: Vec<(ColumnName, ColumnType)>,
}

impl BinaryEncoder {
    pub fn new(desc: RelationDesc, debezium: bool) -> Self {
        let mut columns = column_names_and_types(desc);
        if debezium {
            columns = envelopes::dbz_envelope(columns);
        };
        Self { columns }
    }
}

impl Encode for BinaryEncoder {
    fn encode_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        let mut buf = BytesMut::new();
        for ((_, typ), val) in self.columns.iter().zip_eq(row.iter()) {
            if let Some(pgrepr_value) = mz_pgrepr::Value::from_datum(val, &typ.scalar_type) {
                pgrepr_value
                    .encode_binary(&mz_pgrepr::Type::from(&typ.scalar_type), &mut buf)
                    .expect("encoding failed");
            }
        }

        buf.to_vec()
    }
}
