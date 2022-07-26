// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use mz_ore::now::NowFn;
use mz_repr::{Datum, RelationDesc, Row, ScalarType};

use crate::types::sources::encoding::DataEncodingInner;
use crate::types::sources::Generator;

pub struct Counter {}

impl Generator for Counter {
    fn data_encoding_inner(&self) -> DataEncodingInner {
        DataEncodingInner::RowCodec(
            RelationDesc::empty().with_column("counter", ScalarType::Int64.nullable(false)),
        )
    }

    fn views(&self) -> Vec<(&str, RelationDesc)> {
        Vec::new()
    }

    fn by_seed(&self, _now: NowFn, _seed: Option<u64>) -> Box<dyn Iterator<Item = Row>> {
        let mut counter = 0;
        Box::new(iter::repeat_with(move || {
            counter += 1;
            Row::pack_slice(&[Datum::Int64(counter)])
        }))
    }
}
