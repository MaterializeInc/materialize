// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;
use std::rc::Rc;

use crate::avro::DiffPair;
use crate::encode::column_names_and_types;
use differential_dataflow::{
    lattice::Lattice,
    trace::BatchReader,
    trace::{implementations::ord::OrdValBatch, Cursor},
};
use differential_dataflow::{AsCollection, Collection};
use itertools::Itertools;
use mz_ore::collections::CollectionExt;
use mz_repr::{ColumnType, Datum, Diff, RelationDesc, RelationType, Row, RowPacker, ScalarType};
use timely::dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream};

/// Given a stream of batches, produce a stream of (vectors of) DiffPairs, in timestamp order.
// This is useful for some sink envelopes (e.g., Debezium and Upsert), which need
// to do specific logic based on the _entire_ set of before/after diffs at a given timestamp.
pub fn combine_at_timestamp<G: Scope>(
    batches: Stream<G, Rc<OrdValBatch<Option<Row>, Row, G::Timestamp, Diff>>>,
) -> Collection<G, (Option<Row>, Vec<DiffPair<Row>>), Diff>
where
    G::Timestamp: Lattice + Copy,
{
    let mut rows_buf = vec![];
    let x: Stream<G, ((Option<Row>, Vec<DiffPair<Row>>), G::Timestamp, Diff)> =
        batches.unary(Pipeline, "combine_at_timestamp", move |_, _| {
            move |input, output| {
                while let Some((cap, batches)) = input.next() {
                    let mut session = output.session(&cap);
                    batches.swap(&mut rows_buf);
                    for batch in rows_buf.drain(..) {
                        let mut cursor = batch.cursor();
                        let mut buf: Vec<(G::Timestamp, Option<&Row>, Diff, &Row)> = vec![];

                        // This batch is valid for a range of timestamps, but it might not present things in timestamp order.
                        // Slurp it into a vector, so we can sort that by timestamps.
                        while cursor.key_valid(&batch) {
                            let k = cursor.key(&batch);
                            while cursor.val_valid(&batch) {
                                let val = cursor.val(&batch);
                                cursor.map_times(&batch, |&t, &diff| {
                                    buf.push((t, k.as_ref(), diff, val));
                                });
                                cursor.step_val(&batch);
                            }
                            cursor.step_key(&batch);
                        }
                        // For each (timestamp, key) we've seen,
                        // we need to generate a series of (before, after) pairs.
                        //
                        // Steps to do this:
                        // (1) sort by ts, diff (so that for each ts, diffs appear in ascending order).
                        // (2) in each (ts, key), find the point where negative and positive diffs meet, using binary search.
                        // (3) The (ts, entry, diff) elements before that point will go into "before" fields in DiffPairs;
                        //     the ones after that point will go in "after".

                        // Step (1) above)

                        // Because `sort_by_key` is stable, it will not reorder equal elements. Therefore, elements with the smae
                        // key will stay grouped together.
                        buf.sort_by_key(|(t, _k, diff, _row)| (*t, *diff));
                        for ((t, k), group) in &buf
                            .into_iter()
                            .group_by(|(t, k, _diff, _row)| (*t, k.clone()))
                        {
                            let mut out = vec![];
                            let elts: Vec<(G::Timestamp, Option<&Row>, Diff, &Row)> =
                                group.collect();
                            // Step (2) above
                            let pos_idx = elts
                                .binary_search_by(|(_t, _k, diff, _row)| diff.cmp(&0))
                                .expect_err("there should be no zero-multiplicity entries");

                            // Step (3) above
                            let befores = elts[0..pos_idx]
                                .iter()
                                .flat_map(|elt| iter::repeat(elt).take(elt.2.abs() as usize));
                            let afters = elts[pos_idx..]
                                .iter()
                                .flat_map(|elt| iter::repeat(elt).take(elt.2 as usize));
                            debug_assert!(befores.clone().all(|(_, _, diff, _)| *diff < 0));
                            debug_assert!(afters.clone().all(|(_, _, diff, _)| *diff > 0));
                            for pair in befores.zip_longest(afters) {
                                let (before, after) = match pair {
                                    itertools::EitherOrBoth::Both(before, after) => {
                                        (Some(before.3.clone()), Some(after.3.clone()))
                                    }
                                    itertools::EitherOrBoth::Left(before) => {
                                        (Some(before.3.clone()), None)
                                    }
                                    itertools::EitherOrBoth::Right(after) => {
                                        (None, Some(after.3.clone()))
                                    }
                                };
                                out.push(DiffPair { before, after });
                            }
                            session.give(((k.cloned(), out), t, 1));
                        }
                    }
                }
            }
        });
    x.as_collection()
}

pub fn dbz_desc(desc: RelationDesc) -> RelationDesc {
    let cols = column_names_and_types(desc);
    let row = ColumnType {
        nullable: true,
        scalar_type: ScalarType::Record {
            fields: cols.into_iter().collect(),
            custom_oid: None,
            custom_name: Some("row".to_owned()),
        },
    };
    let typ = RelationType::new(vec![row.clone(), row]);
    RelationDesc::new(typ, ["before", "after"])
}

pub fn dbz_format(rp: &mut RowPacker, dp: DiffPair<Row>) {
    if let Some(before) = dp.before {
        rp.push_list_with(|rp| rp.extend_by_row(&before));
    } else {
        rp.push(Datum::Null);
    }
    if let Some(after) = dp.after {
        rp.push_list_with(|rp| rp.extend_by_row(&after));
    } else {
        rp.push(Datum::Null);
    }
}

pub fn upsert_format(dps: Vec<DiffPair<Row>>) -> Option<Row> {
    let dp = dps.expect_element(
        "primary key error: expected at most one update \
          per key and timestamp. This can happen when the configured sink key is \
          not a primary key of the sinked relation.",
    );
    dp.after
}
