// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_repr::{Datum, Row};
use mz_storage_types::sources::load_generator::{Event, Generator, LoadGeneratorOutput};
use mz_storage_types::sources::MzOffset;
use std::{iter, mem};

pub struct Clock {
    pub tick_ms: u64,
    pub as_of_ms: u64,
}

impl Generator for Clock {
    fn by_seed(
        &self,
        now: NowFn,
        _seed: Option<u64>,
        mut resume_offset: MzOffset,
    ) -> Box<dyn Iterator<Item = (LoadGeneratorOutput, Event<Option<MzOffset>, (Row, i64)>)>> {
        let interval_ms = self.tick_ms;
        let floor = move |t| t / interval_ms * interval_ms;
        let first_tick = floor(self.as_of_ms);
        resume_offset = resume_offset.max(MzOffset::from(first_tick));

        Box::new(
            iter::from_fn(move || {
                let next_offset = MzOffset::from(now() + 1).max(resume_offset);
                let prev_offset = mem::replace(&mut resume_offset, next_offset);
                Some((prev_offset, next_offset))
            })
            .flat_map(move |(lower, upper)| {
                let row = move |tick: u64| {
                    let now_dt = mz_ore::now::to_datetime(tick)
                        .try_into()
                        .expect("system time out of bounds");
                    Row::pack_slice(&[Datum::TimestampTz(now_dt)])
                };

                let messages = (floor(lower.offset)..=floor(upper.offset))
                    .step_by(usize::cast_from(interval_ms))
                    .filter(move |&tick| lower.offset <= tick && tick < upper.offset)
                    .flat_map(move |at_offset| {
                        let last_offset = at_offset
                            .checked_sub(interval_ms)
                            .filter(move |&t| t >= first_tick);
                        [last_offset.map(|t| (t, -1)), Some((at_offset, 1))]
                            .into_iter()
                            .flatten()
                            .map(move |(time, diff)| {
                                Event::Message(MzOffset::from(at_offset), (row(time), diff))
                            })
                    });
                let progress = iter::once(Event::Progress(Some(upper)));

                messages
                    .chain(progress)
                    .map(|e| (LoadGeneratorOutput::Default, e))
            }),
        )
    }
}
