// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tools for staging Peek Response in Persist.

use arrow::array::{BinaryArray, BinaryBuilder, StructArray};
use mz_persist_types::codec_impls::{
    SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
};
use mz_persist_types::columnar::Schema;
use mz_persist_types::stats::NoneStats;

use crate::{Row, RowRef};

#[derive(Debug)]
pub struct StagedRowsSchema;

#[derive(Debug, Default)]
pub struct StagedRow {
    /// Actual Row data.
    inner: Row,
}

impl Schema<StagedRow> for StagedRowsSchema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<StagedRow>;
    type Encoder = SimpleColumnarEncoder<StagedRow>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl SimpleColumnarData for StagedRow {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(self.inner.data());
    }

    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null();
    }

    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        let slice = RowRef::from_slice(column.value(idx));
        // TODO(parkmycar): Copy the slice directly into our Row.
        self.inner = slice.to_owned();
    }
}
