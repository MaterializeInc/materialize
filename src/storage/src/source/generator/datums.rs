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
use mz_repr::{Datum, Row, ScalarType};
use mz_storage_types::sources::load_generator::{Event, Generator, LoadGeneratorOutput};
use mz_storage_types::sources::MzOffset;

pub struct Datums {}

// Note that this generator never issues retractions; if you change this,
// `mz_storage_types::sources::LoadGenerator::is_monotonic`
// must be updated.
impl Generator for Datums {
    fn by_seed(
        &self,
        _: NowFn,
        _seed: Option<u64>,
        _resume_offset: MzOffset,
    ) -> Box<(dyn Iterator<Item = (LoadGeneratorOutput, Event<Option<MzOffset>, (Row, i64)>)>)>
    {
        let typs = ScalarType::enumerate();
        let mut datums: Vec<Vec<Datum>> = typs
            .iter()
            .map(|typ| typ.interesting_datums().collect())
            .collect();
        let len = datums.iter().map(|v| v.len()).max().unwrap();
        // Fill in NULLs for shorter types, and append at least 1 NULL onto
        // every type.
        for dats in datums.iter_mut() {
            while dats.len() < (len + 1) {
                dats.push(Datum::Null);
            }
        }
        // Put the rowid column at the start.
        datums.insert(
            0,
            (1..=len + 1)
                .map(|i| Datum::Int64(i64::try_from(i).expect("must fit")))
                .collect(),
        );
        let mut idx = 0;
        let mut offset = 0;
        Box::new(
            iter::from_fn(move || {
                if idx == len {
                    return None;
                }
                let mut row = Row::with_capacity(datums.len());
                let mut packer = row.packer();
                for d in &datums {
                    packer.push(d[idx]);
                }
                let msg = (
                    LoadGeneratorOutput::Default,
                    Event::Message(MzOffset::from(offset), (row, 1)),
                );

                idx += 1;
                let progress = if idx == len {
                    offset += 1;
                    Some((
                        LoadGeneratorOutput::Default,
                        Event::Progress(Some(MzOffset::from(offset))),
                    ))
                } else {
                    None
                };
                Some(std::iter::once(msg).chain(progress))
            })
            .flatten(),
        )
    }
}
