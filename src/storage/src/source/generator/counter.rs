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

pub struct Counter {
    /// How many values will be emitted
    /// before old ones are retracted, or `None` for
    /// an append-only collection.
    pub max_cardinality: Option<usize>,
}

impl Generator for Counter {
    fn by_seed(
        &self,
        _now: NowFn,
        _seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row, i64)>> {
        let mut counter = 0;
        let max_cardinality = self.max_cardinality;
        Box::new(
            iter::repeat_with(move || {
                let to_retract = match max_cardinality {
                    Some(max) => {
                        if max <= counter {
                            Some(counter - max + 1)
                        } else {
                            None
                        }
                    }
                    None => None,
                };
                counter += 1;
                // NB: we could get rid of this allocation with
                // judicious use of itertools::Either, if it were
                // important to highly optimize this code path.
                let counter: i64 = counter.try_into().expect("counter too big");
                if let Some(to_retract) = to_retract {
                    let to_retract: i64 = to_retract.try_into().expect("to_retract too big");
                    vec![
                        (
                            0,
                            GeneratorMessageType::InProgress,
                            Row::pack_slice(&[Datum::Int64(counter)]),
                            1,
                        ),
                        (
                            0,
                            GeneratorMessageType::Finalized,
                            Row::pack_slice(&[Datum::Int64(to_retract)]),
                            -1,
                        ),
                    ]
                } else {
                    vec![(
                        0,
                        GeneratorMessageType::Finalized,
                        Row::pack_slice(&[Datum::Int64(counter)]),
                        1,
                    )]
                }
            })
            .flatten(),
        )
    }
}
