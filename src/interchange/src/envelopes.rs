// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::iter;
use std::rc::Rc;

use differential_dataflow::{
    lattice::Lattice,
    trace::BatchReader,
    trace::{implementations::ord::OrdValBatch, Cursor},
};
use differential_dataflow::{AsCollection, Collection};
use itertools::{EitherOrBoth, Itertools};
use maplit::hashmap;
use once_cell::sync::Lazy;
use timely::dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream};

use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::{ColumnName, ColumnType, Datum, Diff, GlobalId, Row, RowPacker, ScalarType};

use crate::avro::DiffPair;

/// Given a stream of batches, produce a stream of groups of DiffPairs, grouped
/// by key, at each timestamp.
///
// This is useful for some sink envelopes (e.g., Debezium and Upsert), which
// need to do specific logic based on the _entire_ set of before/after diffs for
// a given key at each timestamp.
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
                        let mut befores = vec![];
                        let mut afters = vec![];

                        let mut cursor = batch.cursor();
                        while cursor.key_valid(&batch) {
                            let k = cursor.key(&batch);

                            // Partition updates into retractions (befores)
                            // and insertions (afters).
                            while cursor.val_valid(&batch) {
                                let v = cursor.val(&batch);
                                cursor.map_times(&batch, |&t, &diff| {
                                    let update = (t, v, usize::cast_from(diff.unsigned_abs()));
                                    if diff < 0 {
                                        befores.push(update);
                                    } else {
                                        afters.push(update);
                                    }
                                });
                                cursor.step_val(&batch);
                            }

                            // Sort by timestamp.
                            befores.sort_by_key(|(t, _v, _diff)| *t);
                            afters.sort_by_key(|(t, _v, _diff)| *t);

                            // Convert diff into unary representation.
                            let befores = befores
                                .drain(..)
                                .flat_map(|(t, v, cnt)| iter::repeat((t, v)).take(cnt));
                            let afters = afters
                                .drain(..)
                                .flat_map(|(t, v, cnt)| iter::repeat((t, v)).take(cnt));

                            // At each timestamp, zip together the insertions
                            // and retractions into diff pairs.
                            let groups = itertools::merge_join_by(
                                befores,
                                afters,
                                |(t1, _v1), (t2, _v2)| t1.cmp(t2),
                            )
                            .map(|pair| match pair {
                                EitherOrBoth::Both((t, before), (_t, after)) => {
                                    (t, Some(before.clone()), Some(after.clone()))
                                }
                                EitherOrBoth::Left((t, before)) => (t, Some(before.clone()), None),
                                EitherOrBoth::Right((t, after)) => (t, None, Some(after.clone())),
                            })
                            .group_by(|(t, _before, _after)| *t);

                            // For each timestamp, emit the group of
                            // `DiffPair`s.
                            for (t, group) in &groups {
                                let group = group
                                    .map(|(_t, before, after)| DiffPair { before, after })
                                    .collect();
                                session.give(((k.clone(), group), t, 1));
                            }

                            cursor.step_key(&batch);
                        }
                    }
                }
            }
        });
    x.as_collection()
}

// NOTE(benesch): statically allocating transient IDs for the
// transaction and row types is a bit of a hack to allow us to attach
// custom names to these types in the generated Avro schema. In the
// future, these types should be real types that get created in the
// catalog with userspace IDs when the user creates the sink, and their
// names and IDs should be plumbed in from the catalog at the moment
// the sink is created.
pub(crate) const TRANSACTION_TYPE_ID: GlobalId = GlobalId::Transient(1);
pub(crate) const DBZ_ROW_TYPE_ID: GlobalId = GlobalId::Transient(2);

pub static ENVELOPE_CUSTOM_NAMES: Lazy<HashMap<GlobalId, String>> = Lazy::new(|| {
    hashmap! {
        TRANSACTION_TYPE_ID => "transaction".into(),
        DBZ_ROW_TYPE_ID => "row".into(),
    }
});

pub(crate) fn dbz_envelope(
    names_and_types: Vec<(ColumnName, ColumnType)>,
) -> Vec<(ColumnName, ColumnType)> {
    let row = ColumnType {
        nullable: true,
        scalar_type: ScalarType::Record {
            fields: names_and_types,
            custom_id: Some(DBZ_ROW_TYPE_ID),
        },
    };
    vec![("before".into(), row.clone()), ("after".into(), row)]
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

pub fn upsert_format(dps: Vec<DiffPair<Row>>, sink_id: GlobalId, from: GlobalId) -> Option<Row> {
    let dp = dps.expect_element(|| {
        format!(
            "primary key error: expected at most one update per key and timestamp \
          This can happen when the configured sink key is not a primary key of \
          the sinked relation: sink {sink_id} created from {from}."
        )
    });
    dp.after
}
