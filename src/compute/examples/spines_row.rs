// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `Row`-keyed port of `differential-dataflow/examples/spines.rs`.
//!
//! Two collections — `data` and `keys` — arrange identical `Row` streams, then
//! join the two arrangements via `join_core`. Data loads in batches of `size`
//! updates until `10 * keys` total inserts, each batch at a fresh timestamp.
//! Queries repeat the same pattern on the other input.
//!
//! Usage:
//!     cargo run --release --example spines_row -- <keys> <size> <mode> [timely-args..]
//!
//! Modes:
//!     row_row       — baseline `RowRowSpine` (`ColumnationStack`-backed)
//!     fact_row_row  — factorized `FactRowRowSpine` (trie-backed)
//!
//! Example:
//!     cargo run --release --example spines_row -- 1000 10000 fact_row_row -w 4

use std::time::Instant;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use mz_compute::typedefs::{
    FactRowRowBatcher, FactRowRowBuilder, FactRowRowSpine, KeyValBatcher, RowRowBuilder,
    RowRowSpine,
};
use mz_repr::{Datum, Diff, Row, Timestamp};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle;

fn main() {
    let keys: usize = std::env::args()
        .nth(1)
        .expect("missing <keys>")
        .parse()
        .expect("keys must be a usize");
    let size: usize = std::env::args()
        .nth(2)
        .expect("missing <size>")
        .parse()
        .expect("size must be a usize");
    let mode: String = std::env::args().nth(3).expect("missing <mode>");

    println!("running [{mode}] arrangement, keys={keys}, size={size}");

    // Skip the example-specific positional args; leave the rest for timely.
    let timely_args = std::env::args().skip(4);

    let timer1 = Instant::now();
    let timer2 = timer1;

    timely::execute_from_args(timely_args, move |worker| {
        let mut probe = Handle::new();
        let (mut data_input, mut keys_input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (data_input, data) = scope.new_collection::<(Row, Row), Diff>();
            let (keys_input, keys) = scope.new_collection::<(Row, Row), Diff>();

            let data_stream = data.inner;
            let keys_stream = keys.inner;

            match mode.as_str() {
                "row_row" => {
                    let data_arranged = arrange_core::<
                        _,
                        KeyValBatcher<Row, Row, Timestamp, Diff>,
                        RowRowBuilder<Timestamp, Diff>,
                        RowRowSpine<Timestamp, Diff>,
                    >(
                        data_stream,
                        Exchange::new(|update: &((Row, Row), Timestamp, Diff)| {
                            update.0.0.hashed().into()
                        }),
                        "ArrangeData",
                    );
                    let keys_arranged = arrange_core::<
                        _,
                        KeyValBatcher<Row, Row, Timestamp, Diff>,
                        RowRowBuilder<Timestamp, Diff>,
                        RowRowSpine<Timestamp, Diff>,
                    >(
                        keys_stream,
                        Exchange::new(|update: &((Row, Row), Timestamp, Diff)| {
                            update.0.0.hashed().into()
                        }),
                        "ArrangeKeys",
                    );
                    keys_arranged
                        .join_core(data_arranged, |_k, _v1, _v2| Option::<()>::None)
                        .inner
                        .probe_with(&mut probe);
                }
                "fact_row_row" => {
                    let data_arranged = arrange_core::<
                        _,
                        FactRowRowBatcher<Timestamp, Diff>,
                        FactRowRowBuilder<Timestamp, Diff>,
                        FactRowRowSpine<Timestamp, Diff>,
                    >(
                        data_stream,
                        Exchange::new(|update: &((Row, Row), Timestamp, Diff)| {
                            update.0.0.hashed().into()
                        }),
                        "ArrangeData",
                    );
                    let keys_arranged = arrange_core::<
                        _,
                        FactRowRowBatcher<Timestamp, Diff>,
                        FactRowRowBuilder<Timestamp, Diff>,
                        FactRowRowSpine<Timestamp, Diff>,
                    >(
                        keys_stream,
                        Exchange::new(|update: &((Row, Row), Timestamp, Diff)| {
                            update.0.0.hashed().into()
                        }),
                        "ArrangeKeys",
                    );
                    keys_arranged
                        .join_core(data_arranged, |_k, _v1, _v2| Option::<()>::None)
                        .inner
                        .probe_with(&mut probe);
                }
                other => panic!("unrecognized mode: {other:?}"),
            }

            (data_input, keys_input)
        });

        // Reusable Row builders to avoid reallocating per insert.
        let mut key_buf = Row::default();
        let mut val_buf = Row::default();
        let mut pack = |n: usize| -> (Row, Row) {
            key_buf.packer().push(Datum::Int64(n as i64));
            val_buf.packer().push(Datum::Int64(n as i64));
            (key_buf.clone(), val_buf.clone())
        };

        // Loading phase: insert into `data_input` in batches of `size`.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (counter + i) % keys;
                data_input.update(pack(val), Diff::ONE);
                i += worker.peers();
            }
            counter += size;
            let next = Timestamp::from(u64::try_from(counter).expect("counter fits u64") + 1);
            data_input.advance_to(next);
            data_input.flush();
            keys_input.advance_to(next);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        // Queries phase: insert into `keys_input` in the same pattern, joining
        // against the already-arranged `data`.
        let mut queries = 0;
        while queries < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (queries + i) % keys;
                keys_input.update(pack(val), Diff::ONE);
                i += worker.peers();
            }
            queries += size;
            let next = Timestamp::from(
                u64::try_from(10 * keys + queries).expect("query count fits u64") + 1,
            );
            data_input.advance_to(next);
            data_input.flush();
            keys_input.advance_to(next);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tqueries complete", timer1.elapsed());
    })
    .unwrap();

    println!("{:?}\tshut down", timer2.elapsed());
}
