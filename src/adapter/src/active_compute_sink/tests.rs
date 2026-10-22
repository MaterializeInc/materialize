// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_expr::ColumnOrder;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, Diff, Row, Timestamp, UpdateCollection};
use mz_sql::plan::SubscribeOutput;
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::active_compute_sink::{
    ActiveSubscribe, ActiveSubscribeOwner, SubscribeBacklogAccounting, SubscribeRowFormat,
};
use crate::coord::peek::PeekResponseUnary;

/// The backlog excludes the message being drained, and zero-payload
/// messages (footprint = overhead only) still accumulate against it.
#[mz_ore::test]
fn test_subscribe_backlog_accounting() {
    let mut acc = SubscribeBacklogAccounting::default();
    assert_eq!(acc.backlog_size(), 0);

    // A single large message is fully tolerated: nothing is queued behind it.
    acc.push(10_000);
    assert_eq!(acc.backlog_size(), 0);

    // Near-empty messages (only per-message overhead) still build backlog, so
    // a flood of frontier-only advances cannot grow without bound.
    acc.push(1_024);
    acc.push(1_024);
    assert_eq!(acc.backlog_size(), 2_048);

    // Draining the oldest message advances the tolerated front.
    acc.pop();
    assert_eq!(acc.backlog_size(), 1_024);

    acc.pop();
    acc.pop();
    assert_eq!(acc.backlog_size(), 0);
}

/// A client that drains each message before the next is sent never
/// accumulates backlog, however many messages flow and however large they
/// are. This is the property that keeps a well-behaved subscribe from ever
/// being retired, so it is asserted after every step rather than at the end.
#[mz_ore::test]
fn test_subscribe_backlog_keeping_up_client() {
    let mut acc = SubscribeBacklogAccounting::default();
    for i in 0..1_000 {
        acc.push(1_024 + i * 4_096);
        assert_eq!(acc.backlog_size(), 0, "message {i} built backlog");
        acc.pop();
        assert_eq!(acc.backlog_size(), 0, "message {i} left backlog behind");
    }
}

const AS_OF: u64 = 3;

fn subscribe(
    emit_progress: bool,
    arity: usize,
    output: SubscribeOutput,
) -> (ActiveSubscribe, mpsc::UnboundedReceiver<PeekResponseUnary>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let active = ActiveSubscribe {
        owner: ActiveSubscribeOwner::Background,
        cluster_id: ClusterId::user(1).expect("valid cluster ID"),
        depends_on: BTreeSet::new(),
        channel: tx,
        backlog_accounting: Arc::new(Mutex::new(SubscribeBacklogAccounting::default())),
        max_buffered_bytes: usize::MAX,
        format: SubscribeRowFormat {
            emit_progress,
            arity,
            output,
        },
        as_of: Timestamp::from(AS_OF),
        start_time: 0,
        internal: true,
    };
    (active, rx)
}

/// Drains the queued responses, flattening every `Rows` message into rows.
fn drain(rx: &mut mpsc::UnboundedReceiver<PeekResponseUnary>) -> Vec<Row> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            PeekResponseUnary::Rows(mut rows) => {
                while let Some(row) = rows.next() {
                    out.push(row.to_owned());
                }
            }
            other => panic!("unexpected response {other:?}"),
        }
    }
    out
}

fn ts(t: u64) -> Datum<'static> {
    Datum::from(Numeric::from(Timestamp::from(t)))
}

fn batch(upper: Antichain<Timestamp>, updates: &[(Row, u64, i64)]) -> SubscribeBatch {
    let times: Vec<_> = updates
        .iter()
        .map(|(_, t, _)| Timestamp::from(*t))
        .collect();
    let updates: UpdateCollection = updates
        .iter()
        .zip_eq(&times)
        .map(|((row, _, diff), time)| (&**row, time, Diff::from(*diff)))
        .collect();
    SubscribeBatch {
        lower: Antichain::from_elem(Timestamp::from(AS_OF)),
        upper,
        updates: Ok(vec![updates]),
    }
}

#[mz_ore::test]
fn diff_rows_with_progress_are_packed_as_timestamp_progressed_diff_columns() {
    let r1 = Row::pack_slice(&[Datum::Int32(1), Datum::String("a")]);
    let r2 = Row::pack_slice(&[Datum::Int32(2), Datum::String("b")]);

    let (active, mut rx) = subscribe(true, 2, SubscribeOutput::Diffs);
    active.initialize();
    let upper = Antichain::from_elem(Timestamp::from(6u64));
    let finished =
        active.process_response(batch(upper, &[(r1.clone(), 4, 1), (r2.clone(), 5, -2)]));
    assert!(!finished);

    let progress =
        |t| Row::pack_slice(&[ts(t), Datum::True, Datum::Null, Datum::Null, Datum::Null]);
    let expected = vec![
        // The progress row announcing the as-of from `initialize`.
        progress(AS_OF),
        Row::pack_slice(&[
            ts(4),
            Datum::False,
            Datum::Int64(1),
            Datum::Int32(1),
            Datum::String("a"),
        ]),
        Row::pack_slice(&[
            ts(5),
            Datum::False,
            Datum::Int64(-2),
            Datum::Int32(2),
            Datum::String("b"),
        ]),
        progress(6),
    ];
    assert_eq!(drain(&mut rx), expected);

    // Without PROGRESS, the progressed column and the progress rows vanish.
    let (active, mut rx) = subscribe(false, 2, SubscribeOutput::Diffs);
    active.initialize();
    let upper = Antichain::from_elem(Timestamp::from(6u64));
    active.process_response(batch(upper, &[(r1, 4, 1)]));
    let expected = vec![Row::pack_slice(&[
        ts(4),
        Datum::Int64(1),
        Datum::Int32(1),
        Datum::String("a"),
    ])];
    assert_eq!(drain(&mut rx), expected);
}

#[mz_ore::test]
fn the_progress_row_carries_nulls_for_every_column() {
    // Debezium appends a null for every non-key column on top of the nulls
    // for the state column and the `arity` table columns.
    let output = SubscribeOutput::EnvelopeDebezium {
        order_by_keys: vec![ColumnOrder {
            column: 0,
            desc: false,
            nulls_last: false,
        }],
    };
    let (active, mut rx) = subscribe(true, 3, output);
    active.initialize();
    let mut datums = vec![ts(AS_OF), Datum::True];
    // State column, 3 table columns, 2 non-key columns.
    datums.extend(std::iter::repeat_n(Datum::Null, 1 + 3 + 2));
    assert_eq!(drain(&mut rx), vec![Row::pack_slice(&datums)]);

    // An empty upper emits no progress row.
    active.process_response(batch(Antichain::new(), &[]));
    assert_eq!(drain(&mut rx), Vec::<Row>::new());

    // Neither does a subscribe without PROGRESS.
    let (active, mut rx) = subscribe(false, 3, SubscribeOutput::Diffs);
    active.initialize();
    assert!(drain(&mut rx).is_empty());
}
