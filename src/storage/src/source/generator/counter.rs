// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::now::NowFn;
use mz_repr::{Datum, Diff, Row};
use mz_storage_types::sources::MzOffset;
use mz_storage_types::sources::load_generator::{Event, Generator, LoadGeneratorOutput};

pub struct Counter {
    /// How many values will be emitted before old ones are retracted,
    /// or `None` for an append-only collection.  (If this retraction
    /// behavior is changed,
    /// `mz_storage_types::sources::LoadGenerator::is_monotonic`
    /// must be updated.
    pub max_cardinality: Option<u64>,
}

impl Generator for Counter {
    fn by_seed(
        &self,
        _now: NowFn,
        _seed: Option<u64>,
        resume_offset: MzOffset,
    ) -> Box<dyn Iterator<Item = (LoadGeneratorOutput, Event<Option<MzOffset>, (Row, Diff)>)>> {
        let max_cardinality = self.max_cardinality;

        Box::new(
            (resume_offset.offset..)
                .map(move |offset| {
                    let retraction = match max_cardinality {
                        // At offset `max` we must start retracting the value of `offset - max`. For
                        // example if max_cardinality is 2 then the collection should contain:
                        // (1, 0, +1)
                        // (2, 1, +1)
                        // (1, 2, -1) <- Here offset becomes >= max and we retract the value that was
                        // (3, 2, +1)    emitted at (offset - max), which equals (offset - max + 1).
                        // (2, 3, -1)
                        // (4, 3, +1)
                        Some(max) if offset >= max => {
                            let retracted_value = i64::try_from(offset - max + 1).unwrap();
                            let row = Row::pack_slice(&[Datum::Int64(retracted_value)]);
                            Some((
                                LoadGeneratorOutput::Default,
                                Event::Message(MzOffset::from(offset), (row, Diff::MINUS_ONE)),
                            ))
                        }
                        _ => None,
                    };

                    let inserted_value = i64::try_from(offset + 1).unwrap();
                    let row = Row::pack_slice(&[Datum::Int64(inserted_value)]);
                    let insertion = [
                        (
                            LoadGeneratorOutput::Default,
                            Event::Message(MzOffset::from(offset), (row, Diff::ONE)),
                        ),
                        (
                            LoadGeneratorOutput::Default,
                            Event::Progress(Some(MzOffset::from(offset + 1))),
                        ),
                    ];
                    retraction.into_iter().chain(insertion)
                })
                .flatten(),
        )
    }
}
