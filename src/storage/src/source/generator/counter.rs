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
use mz_repr::{Datum, Row};
use mz_storage_client::types::sources::{Generator, GeneratorMessageType};

pub struct Counter {}

impl Generator for Counter {
    fn by_seed(
        &self,
        _now: NowFn,
        _seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row)>> {
        let mut counter = 0;
        Box::new(iter::repeat_with(move || {
            counter += 1;
            (
                0,
                GeneratorMessageType::Finalized,
                Row::pack_slice(&[Datum::Int64(counter)]),
            )
        }))
    }
}
