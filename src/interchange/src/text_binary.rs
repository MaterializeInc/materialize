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

use crate::encode::{column_names_and_types, Encode};
use crate::envelopes;

#[derive(Debug)]
pub enum TextBinaryEncoding {
    Text,
    Binary,
}

#[derive(Debug)]
pub struct TextBinaryEncoder {
    key_columns: Option<Vec<(ColumnName, ColumnType)>>,
    value_columns: Option<Vec<(ColumnName, ColumnType)>>,
    encoding: TextBinaryEncoding,
}

impl TextBinaryEncoder {
    pub fn new(
        key_desc: Option<RelationDesc>,
        value_desc: Option<RelationDesc>,
        encoding: TextBinaryEncoding,
        debezium: bool,
    ) -> Self {
        let value_columns = match value_desc {
            Some(desc) => {
                let mut value_columns = column_names_and_types(desc);
                if debezium {
                    value_columns = envelopes::dbz_envelope(value_columns);
                }
                Some(value_columns)
            }
            None => None,
        };
        let key_columns = match key_desc {
            Some(desc) => {
                let key_columns = column_names_and_types(desc);
                Some(key_columns)
            }
            None => None,
        };
        Self {
            key_columns,
            value_columns,
            encoding,
        }
    }

    fn encode_row(&self, row: mz_repr::Row, columns: &Vec<(ColumnName, ColumnType)>) -> Vec<u8> {
        let mut buf = BytesMut::new();
        for ((_, typ), val) in columns.iter().zip(row.iter()) {
            match mz_pgrepr::Value::from_datum(val, &typ.scalar_type) {
                Some(pgrepr_val) => match self.encoding {
                    TextBinaryEncoding::Text => _ = pgrepr_val.encode_text(&mut buf),
                    TextBinaryEncoding::Binary => pgrepr_val
                        .encode_binary(&mz_pgrepr::Type::from(&typ.scalar_type), &mut buf)
                        .expect("encoding failed"),
                },
                None => {} // this is a NULL value
            };
        }

        buf.to_vec()
    }
}

impl Encode for TextBinaryEncoder {
    fn encode_key_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        let columns = self.key_columns.as_ref().expect("key encoding must exist");
        self.encode_row(row, columns)
    }

    fn encode_value_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        let columns = self
            .value_columns
            .as_ref()
            .expect("value encoding must exist");
        self.encode_row(row, columns)
    }
}
