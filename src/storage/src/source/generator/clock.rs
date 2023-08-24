// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, Row};
use mz_storage_client::types::sources::{Generator, GeneratorMessageType};

/// A generator that emits the current system time.
pub struct Clock;

impl Generator for Clock {
    fn by_seed(
        &self,
        now: mz_ore::now::NowFn,
        _seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, mz_repr::Row, i64)>> {
        let mut last = None;
        Box::new(
            std::iter::repeat_with(move || {
                let now = (now)().try_into().expect("sane system clock");
                // NB: we could get rid of this allocation with
                // judicious use of itertools::Either, if it were
                // important to highly optimize this code path.
                let updates = if let Some(last) = last {
                    vec![
                        (
                            0,
                            GeneratorMessageType::InProgress,
                            Row::pack_slice(&[Datum::Int64(now)]),
                            1,
                        ),
                        (
                            0,
                            GeneratorMessageType::Finalized,
                            Row::pack_slice(&[Datum::Int64(last)]),
                            -1,
                        ),
                    ]
                } else {
                    vec![(
                        0,
                        GeneratorMessageType::Finalized,
                        Row::pack_slice(&[Datum::Int64(now)]),
                        1,
                    )]
                };
                last = Some(now);
                updates
            })
            .flatten(),
        )
    }
}
